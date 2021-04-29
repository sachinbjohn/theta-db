package queries

import datagen.Bids
import ds._
import exec._
import utils._

abstract class Stock1 extends StockExecutable {
  override def query: String = "Stock1"

  def evaluate(stocks: Table): Unit

  override def execute(stocks: Table): Long = {
    result.clear()
    val start = System.nanoTime()
    evaluate(stocks)
    val end = System.nanoTime()
    (end - start) / onemill
  }

  val result = collection.mutable.HashMap[Double, Double]()
}

object Stock1Naive extends Stock1 {
  override def algo: Algorithm = Naive

  import Stock1._

  override def evaluate(stocks: Table) = {
    val avg = collection.mutable.HashMap[(Double, Double), (Double, Double)]()
    stocks.rows.foreach { s1 =>
      val id = s1(idCol)
      val price = s1(priceCol)
      val year = toYear(s1(timeCol))
      val key = (id, year)
      val curval: (Double, Double) = avg.getOrElse(key, (0, 0))
      val newval: (Double, Double) = ((curval._1 + price), (curval._2 + 1.0))

      avg += key -> newval
    }
    val StockAvg = new Table("StockAvg", avg.toList.map(kv => Row(Array(kv._1._1, kv._1._2, kv._2._1 / kv._2._2))))

    StockAvg.foreach { s2 =>
      val id = s2(idCol)
      StockAvg.foreach { s3 =>
        if (s3(idCol) == id && s3(timeCol) > s2(timeCol) && s3(priceCol) < s2(priceCol)) {
          result += id -> (result.getOrElse(id, 0.0) + 1.0)
        }
      }
    }
  }
}

object Stock1Range extends Stock1 {

  import Stock1._

  override def evaluate(stocks: Table) {
    val avg = collection.mutable.HashMap[(Double, Double), (Double, Double)]()
    stocks.rows.foreach { s1 =>
      val id = s1(idCol)
      val price = s1(priceCol)
      val year = toYear(s1(timeCol))
      val key = (id, year)
      val curval = avg.getOrElse(key, (0.0, 0.0))
      val newval = (curval._1 + price, curval._2 + 1.0)
      avg += key -> newval
    }
    val StockAvg = new Table("StockAvg", avg.toList.map(kv => Row(Array(kv._1._1, kv._1._2, kv._2._1 / kv._2._2))))
    val rtS3 = RangeTree.buildFrom(StockAvg, keyFn, 3, valueFn, AggPlus, "S3")
    val join = rtS3.join(StockAvg, keyFn, ops.toArray)
    join.foreach { r =>
      val id = r(idCol)
      val count = r(countCol)
      result += id -> (result.getOrElse(id, 0.0) + count)
    }

  }

  override def algo: Algorithm = Inner
}

object Stock1Merge extends Stock1 {

  import Stock1._
  import utils.Helper._

  val ord = sorting(keyFn, ops)

  override def evaluate(stocks: Table) {
    val avg = collection.mutable.HashMap[(Double, Double), (Double, Double)]()
    stocks.rows.foreach { s1 =>
      val id = s1(idCol)
      val price = s1(priceCol)
      val year = toYear(s1(timeCol))
      val key = (id, year)
      val curval = avg.getOrElse(key, (0.0, 0.0))
      val newval = (curval._1 + price, curval._2 + 1.0)
      avg += key -> newval
    }
    val StockAvg = new Table("StockAvg", avg.toList.map(kv => Row(Array(kv._1._1, kv._1._2, kv._2._1 / kv._2._2))).sorted(ord))
    val ids = Domain(StockAvg.rows.map(_ (idCol)).distinct.toArray.sorted)
    val times = Domain(StockAvg.rows.map(_ (timeCol)).distinct.toArray.sorted(Ordering[Double].reverse))
    val prices = Domain(StockAvg.rows.map(_ (priceCol)).distinct.toArray.sorted)
    val cubeS3 = Cube.fromData(Array(ids, times, prices), StockAvg, keyFn, valueFn, AggPlus)
    cubeS3.accumulate(ops)

    val join = cubeS3.join(StockAvg, keyFn, ops.toArray)
    join.foreach { r =>
      val id = r(idCol)
      val count = r(countCol)
      result += id -> (result.getOrElse(id, 0.0) + count)
    }
  }

  override def algo: Algorithm = Merge
}

object Stock1 {
  val idCol = 0
  val timeCol = 1
  val priceCol = 2
  val countCol = 3
  val ops = List(EqualTo[Double], GreaterThan[Double], LessThan[Double])
  val keyFn = (r: Row) => Array(r(idCol), r(timeCol), r(priceCol))
  val valueFn = (r: Row) => 1.0

  def main(args: Array[String]) = {
    val stocks = new Table("Stocks", Bids.generate(10, 10, 5, 5))
    val allTests = List(Stock1Naive, Stock1Range, Stock1Merge)
    allTests.foreach { a =>
      val rt = a.execute(stocks)
      println(s"${a.query},${a.algo}, $rt")
      println(a.result.toList.sortBy(_._1).mkString(","))
    }
  }
}