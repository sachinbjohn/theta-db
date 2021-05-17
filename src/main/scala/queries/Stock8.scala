package queries

import datagen.Bids
import ds._
import exec._
import utils.Helper.sortingOther
import utils._

abstract class Stock8 extends StockExecutable {
  val result = collection.mutable.HashSet[(Double, Double, Double)]()

  override def execute(stocks: Table): Long = {
    result.clear()
    val start = System.nanoTime()
    evaluate(stocks)
    val end = System.nanoTime()
    (end - start) / onemill
  }

  def evaluate(stocks: Table): Unit

  override def query: String = "Stock8"
}

object Stock8Naive extends Stock8 {

  import Stock8._

  override def evaluate(stocks: Table): Unit = {
    stocks.foreach { s1 =>
      var maxPrice = Double.NegativeInfinity
      stocks.foreach { s2 =>
        if (s2(idCol) == s1(idCol) && s2(timeCol) < s1(timeCol) && s2(priceCol) > maxPrice)
          maxPrice = s2(priceCol)
      }
      if (s1(priceCol) > 1.1 * maxPrice)
        result += ((s1(idCol), s1(timeCol), s1(priceCol)))
    }
  }

  override def algo: Algorithm = Naive
}

object Stock8Range extends Stock8 {

  import Stock8._

  override def evaluate(stocks: Table): Unit = {
    val rt = RangeTree.buildFrom(stocks, keyFn, 2, valueFn, AggMax, "S2")
    val join = rt.join(stocks, keyFn, ops.toArray)
    join.foreach { r =>
      if (r(priceCol) > 1.1 * r(maxPriceCol))
        result += ((r(idCol), r(timeCol), r(priceCol)))
    }
  }

  override def algo: Algorithm = Inner
}

object Stock8Merge extends Stock8 {

  import Stock8._
  import Helper.sorting

  implicit val ord = sorting(keyFn, ops)

  override def evaluate(stocks: Table): Unit = {
    val sortedStocks = new Table("S", stocks.rows.sorted)
    val ids = Domain(sortedStocks.rows.map(_ (idCol)).distinct.toArray.sorted)
    val times = Domain(sortedStocks.rows.map(_ (timeCol)).distinct.toArray.sorted)
    val domains = Array(ids, times)
    val cube = Cube.fromData(domains, sortedStocks, keyFn, valueFn, AggMax)
    cube.accumulate(ops)
    val join = cube.join(sortedStocks, keyFn, ops.toArray)
    join.foreach { r =>
      if (r(priceCol) > 1.1 * r(maxPriceCol))
        result += ((r(idCol), r(timeCol), r(priceCol)))
    }
  }

  override def algo: Algorithm = Merge
}

object Stock8 {
  val idCol = 0
  val timeCol = 1
  val priceCol = 2
  val maxPriceCol = 3

  val keyFn = (r: Row) => Array(r(idCol), r(timeCol))
  val valueFn = (r: Row) => r(priceCol)
  val ops = List(EqualTo, LessThan)

  def main(args: Array[String]) {
    val stocks = new Table("Stocks", Bids.generate(10, 10, 8, 8))
    val allTests = List(Stock8Naive, Stock8Range, Stock8Merge)
    allTests.foreach { a =>
      val rt = a.execute(stocks)
      println(s"${a.query},${a.algo}, $rt")
      println(a.result.toList.sortBy(_._1).mkString(","))
    }
    val results = allTests.map(_.result)
    assert(results.forall(_ equals results.head))
  }
}