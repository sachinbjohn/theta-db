package queries

import datagen.Bids
import ds._
import exec._
import utils._

abstract class Stock5 extends StockExecutable {
  val result = collection.mutable.HashSet[(Double, Double, Double)]()

  override def execute(stocks: Table): Long = {
    result.clear()
    val start = System.nanoTime()
    evaluate(stocks)
    val end = System.nanoTime()
    (end - start) / onemill
  }

  def evaluate(stocks: Table)

  override def query: String = "Stock5"

}

object Stock5Naive extends Stock5 {

  import Stock5._

  override def evaluate(stocks: Table): Unit =
    stocks.foreach { s1 =>
      val id = s1(idCol)
      val time = s1(timeCol)
      val price = s1(priceCol)

      var aggs2 = 0.0
      var aggs3 = 0.0
      var aggs4 = 0.0
      var aggs5 = 0.0
      stocks.foreach { s =>
        if (s(idCol) == id && s(timeCol) >= s1(timeCol) - fromDay(5) && s(timeCol) < s1(timeCol)) {
          aggs2 += 1
          aggs3 += (s(timeCol) * s(priceCol))
          aggs4 += s(priceCol)
          aggs5 += s(timeCol)
        }
      }
    if(aggs2 * aggs3 > aggs4 * aggs5)
      result += ((id, time, price))
    }

  override def algo: Algorithm = Naive
}
object Stock5Range extends Stock5{
  import Stock5._
  override def evaluate(stocks: Table): Unit = {
    val rtS2 = RangeTree.buildFrom(stocks, keyFns2345, 3, valueFn2, AggPlus, "S2")
    val rtS3 = RangeTree.buildFrom(stocks, keyFns2345, 3, valueFn3, AggPlus, "S3")
    val rtS4 = RangeTree.buildFrom(stocks, keyFns2345, 3, valueFn4, AggPlus, "S4")
    val rtS5 = RangeTree.buildFrom(stocks, keyFns2345, 3, valueFn5, AggPlus, "S5")

    val s1s2 = rtS2.join(stocks, keyFns1, ops.toArray)
    val s1s2s3 = rtS3.join(s1s2, keyFns1, ops.toArray)
    val s1s2s3s4 = rtS4.join(s1s2s3, keyFns1, ops.toArray)
    val s1s2s3s4s5 = rtS5.join(s1s2s3s4, keyFns1, ops.toArray)

    s1s2s3s4s5.foreach{r =>
      if(r(aggs2Col) * r(aggs3Col) > r(aggs4Col) * r(aggs5Col))
        result += ((r(idCol), r(timeCol), r(priceCol)))
    }
  }

  override def algo: Algorithm = Inner
}

object Stock5Merge extends Stock5 {
  import Stock5._
  import Helper._
  override def evaluate(stocks: Table): Unit = {
    val ids = Domain(stocks.rows.map(_(idCol)).distinct.toArray.sorted)
    val tvs = stocks.rows.map(_(timeCol)).distinct.toArray
    val times2 = Domain(tvs.sorted)
    val times1 = Domain(tvs.sorted(Ordering[Double].reverse), false)
    val domains = Array(ids, times1, times2)
    val ordinner = sorting(keyFns2345, ops)
    times2.sameAsOuter = false
    val ordouter = sortingOther(domains, keyFns1, ops)
    val sortedStocksInner = new Table("S", stocks.rows.sorted(ordinner))
    val sortedStocksOuter = new Table("S", stocks.rows.sorted(ordouter))

    val cubeS2 = Cube.fromData(domains, sortedStocksInner, keyFns2345, valueFn2, AggPlus)
    cubeS2.accumulate(ops)
    val cubeS3 = Cube.fromData(domains, sortedStocksInner, keyFns2345, valueFn3, AggPlus)
    cubeS3.accumulate(ops)
    val cubeS4 = Cube.fromData(domains, sortedStocksInner, keyFns2345, valueFn4, AggPlus)
    cubeS4.accumulate(ops)
    val cubeS5 = Cube.fromData(domains, sortedStocksInner, keyFns2345, valueFn5, AggPlus)
    cubeS5.accumulate(ops)

    val s1s2 = cubeS2.join(sortedStocksOuter, keyFns1, ops.toArray)
    val s1s2s3 = cubeS3.join(s1s2, keyFns1, ops.toArray)
    val s1s2s3s4 = cubeS4.join(s1s2s3, keyFns1, ops.toArray)
    val s1s2s3s4s5 = cubeS5.join(s1s2s3s4, keyFns1, ops.toArray)

    s1s2s3s4s5.foreach{r =>
      if(r(aggs2Col) * r(aggs3Col) > r(aggs4Col) * r(aggs5Col))
        result += ((r(idCol), r(timeCol), r(priceCol)))
    }

  }

  override def algo: Algorithm = Merge
}
object Stock5 {
  val idCol = 0
  val timeCol = 1
  val priceCol = 2
  val aggs2Col = 3
  val aggs3Col= 4
  val aggs4Col = 5
  val aggs5Col = 6

  //Fix toDay link from StockExecutable somehow
  def fromDay(d: Double) = d

  val keyFns1 = (r: Row) => Array(r(idCol), r(timeCol) - fromDay(tconst), r(timeCol))
  val keyFns2345 = (r: Row) => Array(r(idCol), r(timeCol), r(timeCol))
  val valueFn2 = (r: Row) => 1.0
  val valueFn3 = (r: Row) => r(priceCol) * r(timeCol)
  val valueFn4 = (r: Row) => r(priceCol)
  val valueFn5 = (r: Row) => r(timeCol)

  val ops = List(EqualTo, GreaterThanEqual, LessThan)
  val tconst = 5

  def main(args: Array[String]) = {
    val stocks = new Table("Stocks", Bids.generate(7, 7, 5, 5))
    val allTests = List(Stock5Naive, Stock5Range, Stock5Merge)
    allTests.foreach { a =>
      val rt = a.execute(stocks)
      println(s"${a.query},${a.algo}, $rt")
      println(a.result.toList.sortBy(_._1).mkString(","))
    }
    val results = allTests.map(_.result)
    assert(results.forall(_ equals results.head))
  }
}