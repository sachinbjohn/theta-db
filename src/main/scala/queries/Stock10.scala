package queries

import datagen.Bids
import ds._
import exec._
import utils.Helper.sortingOther
import utils._

abstract class Stock10 extends StockExecutable {
  val result = collection.mutable.HashSet[(Double, Double, Double)]()

  override def execute(stocks: Table): Long = {
    result.clear()
    val start = System.nanoTime()
    evaluate(stocks)
    val end = System.nanoTime()
    (end - start) / onemill
  }

  def evaluate(stocks: Table): Unit

  override def query: String = "Stock10"
}

object Stock10Naive extends Stock10 {

  import Stock10._

  override def evaluate(stocks: Table): Unit = {
    stocks.foreach { s1 =>
      var maxTime = Double.NegativeInfinity
      var minTime = Double.PositiveInfinity
      stocks.foreach { s2 =>
        if (s2(idCol) == s1(idCol) && s2(priceCol) < s1(priceCol) && s2(timeCol) > s1(timeCol) && s2(timeCol) > maxTime)
          maxTime = s2(timeCol)
      }
      stocks.foreach { s3 =>
        if (s3(idCol) == s1(idCol) && s3(priceCol) < s1(priceCol) && s3(timeCol) < s1(timeCol) && s3(timeCol) < minTime)
          minTime = s3(timeCol)
      }
      if (maxTime >= minTime + tconst)
        result += ((s1(idCol), s1(timeCol), s1(priceCol)))
    }
  }

  override def algo: Algorithm = Naive
}

object Stock10Range extends Stock10 {

  import Stock10._

  override def evaluate(stocks: Table): Unit = {
    val rt2 = RangeTree.buildFrom(stocks, keyFn, 3, valueFn, AggMax, "S2")
    val rt3 = RangeTree.buildFrom(stocks, keyFn, 3, valueFn, AggMin, "S3")
    val s1s2 = rt2.join(stocks, keyFn, ops2.toArray)
    val s1s2s3 = rt3.join(s1s2, keyFn, ops3.toArray)
    s1s2s3.foreach { r =>
      if (r(maxTimeCol) >= r(minTimeCol) + tconst)
        result += ((r(idCol), r(timeCol), r(priceCol)))
    }
  }

  override def algo: Algorithm = Inner
}

object Stock10Merge extends Stock10 {

  import Stock10._
  import Helper.sorting


  override def evaluate(stocks: Table): Unit = {
    val ord2 = sorting(keyFn, ops2)
    val ord3 = sorting(keyFn, ops3)
    val sortedStocks2 = new Table("S2", stocks.rows.sorted(ord2))
    val sortedStocks3 = new Table("S3", stocks.rows.sorted(ord3))
    val ids = Domain(stocks.rows.map(_ (idCol)).distinct.toArray.sorted)
    val prices = Domain(stocks.rows.map(_ (priceCol)).distinct.toArray.sorted)
    val tvs = stocks.rows.map(_ (timeCol)).distinct.toArray
    val times2 = Domain(tvs.sorted(Ordering[Double].reverse), false)
    val times3 = Domain(tvs.sorted)
    val domains2 = Array(ids, prices, times2)
    val domains3 = Array(ids, prices, times3)

    val cube2 = Cube.fromData(domains2, sortedStocks2, keyFn, valueFn, AggMax)
    cube2.accumulate(ops2)

    val cube3 = Cube.fromData(domains3, sortedStocks3, keyFn, valueFn, AggMin)
    cube3.accumulate(ops3)

    val s1s2 = cube2.join(sortedStocks2, keyFn, ops2.toArray)
    val s1s2Sorted = new Table("S1s2", s1s2.rows.sorted(ord3))
    val s1s2s3 = cube3.join(s1s2Sorted, keyFn, ops3.toArray)
    s1s2s3.foreach { r =>
      if (r(maxTimeCol) >= r(minTimeCol) + tconst)
        result += ((r(idCol), r(timeCol), r(priceCol)))
    }
  }

  override def algo: Algorithm = Merge
}

object Stock10 {
  val idCol = 0
  val timeCol = 1
  val priceCol = 2
  val maxTimeCol = 3
  val minTimeCol = 4

  val tconst = 5
  val keyFn = (r: Row) => Array(r(idCol), r(priceCol), r(timeCol)) //SBJ: Order matters for performance?
  val valueFn = (r: Row) => r(timeCol)
  val ops2 = List(EqualTo[Double], LessThan[Double], GreaterThan[Double])
  val ops3 = List(EqualTo[Double], LessThan[Double], LessThan[Double])

  def main(args: Array[String]) {
    val stocks = new Table("Stocks", Bids.generate(10, 10, 8, 8))
    val allTests = List(Stock10Naive, Stock10Range, Stock10Merge)
    allTests.foreach { a =>
      val rt = a.execute(stocks)
      println(s"${a.query},${a.algo}, $rt")
      println(a.result.toList.sortBy(_._1).mkString(","))
    }
    val results = allTests.map(_.result)
    assert(results.forall(_ equals results.head))
  }
}