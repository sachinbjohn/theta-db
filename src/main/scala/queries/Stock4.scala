package queries

import datagen.Bids
import ds._
import exec._
import utils._

abstract class Stock4 extends StockExecutable {
  val result = collection.mutable.HashSet[(Double, Double, Double)]()

  override def execute(stocks: Table): Long = {
    result.clear()
    val start = System.nanoTime()
    evaluate(stocks)
    val end = System.nanoTime()
    (end - start) / onemill
  }

  def evaluate(stocks: Table): Unit

  override def query: String = "Stock4"
}

object Stock4Naive extends Stock4 {

  import Stock4._

  override def evaluate(stocks: Table): Unit = {
    stocks.foreach { s1 =>
      val id = s1(idCol)
      val time = s1(timeCol)
      val price = s1(priceCol)
      var max = Double.NegativeInfinity
      stocks.foreach { s2 =>
        if (s2(idCol) == id && s2(timeCol) <= time && max < s2(priceCol)) {
          max = s2(priceCol)
        }
      }
      if (max == price) {
        result += ((id, time, price))
      }
    }
  }

  override def algo: Algorithm = Naive
}

object Stock4Range extends Stock4 {

  import Stock4._

  override def evaluate(stocks: Table): Unit = {
    val rt = RangeTree.buildFrom(stocks, keyFn, 2, valueFn, AggMax, "S2")
    val join = rt.join(stocks,keyFn, ops.toArray)
    join.foreach { r =>
      if (r(priceCol) == r(maxpriceCol))
        result += ((r(idCol), r(timeCol), r(priceCol)))
    }
  }

  override def algo: Algorithm = Inner
}

object Stock4Merge extends Stock4 {
  import Stock4._
  import Helper.sorting
  override def evaluate(stocks: Table): Unit = {
    implicit val ord = sorting(keyFn, ops)
    val sortedStocks = new Table("S", stocks.rows.sorted)
    val ids = Domain(sortedStocks.rows.map(_(idCol)).distinct.toArray.sorted)
    val times = Domain(sortedStocks.rows.map(_(timeCol)).distinct.toArray.sorted)
    val cube = Cube.fromData(Array(ids, times), sortedStocks, keyFn, valueFn, AggMax)
    cube.accumulate(ops)
    val join = cube.join(sortedStocks, keyFn, ops.toArray)
    join.foreach{r =>
      if(r(priceCol) == r(maxpriceCol))
        result += ((r(idCol), r(timeCol), r(priceCol)))
    }
  }

  override def algo: Algorithm = Merge
}
object Stock4 {
  val idCol = 0
  val timeCol = 1
  val priceCol = 2
  val maxpriceCol = 3
  val ops = List(EqualTo, LessThanEqual)
  val keyFn = (r: Row) => Array(r(idCol), r(timeCol))
  val valueFn = (r: Row) => r(priceCol)

  def main(args: Array[String]) = {
    val stocks = new Table("Stocks", Bids.generate(10, 10, 5, 5))
    val allTests = List(Stock4Naive, Stock4Range, Stock4Merge)
    allTests.foreach { a =>
      val rt = a.execute(stocks)
      println(s"${a.query},${a.algo}, $rt")
      println(a.result.toList.sortBy(_._1).mkString(","))
    }
    val results = allTests.map(_.result)
      assert(results.forall(_ equals results.head))
  }
}