package queries

import datagen.Bids
import ds._
import exec._
import utils.Helper.sortingOther
import utils._

abstract class Stock9 extends StockExecutable {
  val result = collection.mutable.HashSet[(Double, Double, Double)]()

  override def execute(stocks: Table): Long = {
    result.clear()
    val start = System.nanoTime()
    evaluate(stocks)
    val end = System.nanoTime()
    (end - start) / onemill
  }

  def evaluate(stocks: Table): Unit

  override def query: String = "Stock9"
}

object Stock9Naive extends Stock9 {

  import Stock9._

  override def evaluate(stocks: Table): Unit = {
    stocks.foreach { s1 =>
      var maxPrice = Double.NegativeInfinity
      stocks.foreach { s2 =>
        if (s2(idCol) == s1(idCol) && s2(timeCol) < s1(timeCol)) {
          var sum = 0
          stocks.foreach{ s3 =>
            if(s3(idCol) == s2(idCol) && s3(timeCol) > s2(timeCol) && s3(timeCol) < s1(timeCol))
              sum += 1
          }
          if(sum == 0 && s2(priceCol) > maxPrice)
            maxPrice = s2(priceCol)
        }
      }
      if (s1(priceCol) > 1.1 * maxPrice)
        result += ((s1(idCol), s1(timeCol), s1(priceCol)))
    }
  }

  override def algo: Algorithm = Naive
}

object Stock9Range extends Stock9 {

  import Stock9._

  override def evaluate(stocks: Table): Unit = {

  }

  override def algo: Algorithm = Inner
}

object Stock9Merge extends Stock9 {

  import Stock9._
  import Helper.sorting

  override def evaluate(stocks: Table): Unit = {

  }

  override def algo: Algorithm = Merge
}

object Stock9 {
  val idCol = 0
  val timeCol = 1
  val priceCol = 2
  val maxPriceCol = 3


  def main(args: Array[String]) {
    val stocks = new Table("Stocks", Bids.generate(10, 10, 8, 8))
    val allTests = List(Stock9Naive, Stock9Range, Stock9Merge)
    allTests.foreach { a =>
      val rt = a.execute(stocks)
      println(s"${a.query},${a.algo}, $rt")
      println(a.result.toList.sortBy(_._1).mkString(","))
    }
    val results = allTests.map(_.result)
    assert(results.forall(_ equals results.head))
  }
}