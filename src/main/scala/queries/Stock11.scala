package queries

import datagen.Bids
import ds._
import exec._
import utils.Helper.sortingOther
import utils._

abstract class Stock11 extends StockExecutable {
  val result = collection.mutable.HashSet[(Double, Double, Double)]()

  override def execute(stocks: Table): Long = {
    result.clear()
    val start = System.nanoTime()
    evaluate(stocks)
    val end = System.nanoTime()
    (end - start) / onemill
  }

  def evaluate(stocks: Table): Unit

  override def query: String = "Stock11"
}

object Stock11Naive extends Stock11 {

  import Stock11._

  override def evaluate(stocks: Table): Unit = {
    stocks.foreach { s1 =>
      var sum2 = 0.0
      var sum3 = 0.0
      stocks.foreach { s2 =>
        if (s2(idCol) == s1(idCol) && s2(timeCol) < s1(timeCol) && toDay(s2(timeCol)) >= toDay(s1(timeCol)) - tconst) {
          sum2 += 1
          sum3 += s2(priceCol)
        }
      }

      if (s1(priceCol) * sum2 > sum3)
        result += ((s1(idCol), s1(timeCol), s1(priceCol)))
    }
  }

  override def algo: Algorithm = Naive
}

object Stock11Range extends Stock11 {

  import Stock11._

  override def evaluate(stocks: Table): Unit = {
    val rt2 = RangeTree.buildFrom(stocks, keyFnInner, 3, valueFn2, AggPlus, "S2")
    val rt3 = RangeTree.buildFrom(stocks, keyFnInner, 3, valueFn3, AggPlus, "S3")
    val s1s2 = rt2.join(stocks, keyFnOuter, ops.toArray)
    val s1s2s3 = rt3.join(s1s2, keyFnOuter, ops.toArray)
    s1s2s3.foreach { r =>
      if (r(priceCol) * r(sum2Col) > r(sum3Col))
        result += ((r(idCol), r(timeCol), r(priceCol)))
    }
  }

  override def algo: Algorithm = Inner
}

object Stock11Merge extends Stock11 {

  import Stock11._
  import Helper.sorting


  override def evaluate(stocks: Table): Unit = {
    val ids = Domain(stocks.rows.map(_ (idCol)).distinct.toArray.sorted)
    val tvs = stocks.rows.map(_ (timeCol)).distinct.toArray
    val times2 = Domain(tvs.sorted(Ordering[Double].reverse))
    val times1 = Domain(tvs.sorted)
    val domains = Array(ids, times1, times2)

    val ordInner = sorting(keyFnInner, ops)
    val ordOuter = sortingOther(domains.zip(List(false, false, true)), keyFnOuter, ops)
    val sortedStocksInner = new Table("S2", stocks.rows.sorted(ordInner))
    val sortedStocksOuter = new Table("S3", stocks.rows.sorted(ordOuter))

    val cube2 = Cube.fromData(domains, sortedStocksInner, keyFnInner, valueFn2, AggPlus)
    cube2.accumulate(ops)

    val cube3 = Cube.fromData(domains, sortedStocksInner, keyFnInner, valueFn3, AggPlus)
    cube3.accumulate(ops)


    val s1s2 = cube2.join(sortedStocksOuter, keyFnOuter, ops.toArray)
    val s1s2s3 = cube3.join(s1s2, keyFnOuter, ops.toArray)
    s1s2s3.foreach { r =>
      if (r(priceCol) * r(sum2Col) > r(sum3Col))
        result += ((r(idCol), r(timeCol), r(priceCol)))
    }
  }

  override def algo: Algorithm = Merge
}

object Stock11 {
  val idCol = 0
  val timeCol = 1
  val priceCol = 2
  val sum2Col = 3
  val sum3Col = 4

  import Stock11Naive._

  val tconst = 20
  val keyFnInner = (r: Row) => Array(r(idCol), r(timeCol), toDay(r(timeCol)))
  val keyFnOuter = (r: Row) => Array(r(idCol), r(timeCol), toDay(r(timeCol)) - tconst)
  val valueFn2 = (r: Row) => 1.0
  val valueFn3 = (r: Row) => r(priceCol)
  val ops = List(EqualTo[Double], LessThan[Double], GreaterThanEqual[Double])

  def main(args: Array[String]) {
    val stocks = new Table("Stocks", Bids.generate(10, 10, 8, 8))
    val allTests = List(Stock11Naive, Stock11Range, Stock11Merge)
    allTests.foreach { a =>
      val rt = a.execute(stocks)
      println(s"${a.query},${a.algo}, $rt")
      println(a.result.toList.sortBy(_._1).mkString(","))
    }
    val results = allTests.map(_.result)
    assert(results.forall(_ equals results.head))
  }
}