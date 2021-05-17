package queries


import datagen.Bids
import ds._
import exec._
import utils._

abstract class Stock2 extends StockExecutable {
  override def query: String = "Stock2"

  val result = collection.mutable.HashMap[Double, Double]()

  def evaluate(stocks: Table): Unit

  override def execute(stocks: Table): Long = {
    result.clear()
    val start = System.nanoTime()
    evaluate(stocks)
    val end = System.nanoTime()
    (end - start) / onemill
  }

}

object Stock2Naive extends Stock2 {

  import Stock2._

  override def evaluate(stocks: Table) {
    stocks.rows.foreach { s2 =>
      val id = s2(idCol)
      if (toMin(s2(timeCol)) >= toMin(tval) - tconst) {
        stocks.rows.foreach { s3 =>
          if (id == s3(idCol) && s3(timeCol) < tval && s3(timeCol) > s2(timeCol) && s3(priceCol) < s2(priceCol)) {
            result += id -> (result.getOrElse(id, 0.0) + 1.0)
          }
        }
      }
    }
  }

  override def algo: Algorithm = Naive
}

object Stock2Range extends Stock2 {

  import Stock2._

  override def evaluate(stocks: Table){
    val s2f = new Table("S2", stocks.rows.filter(r => toMin(r(timeCol)) >= toMin(tval) - tconst))
    val s3f = new Table("S3", stocks.rows.filter(r => r(timeCol) < tval))

    val rtS3 = RangeTree.buildFrom(s3f, keyFn, 3, valueFn, AggPlus, "S3")
    val join = rtS3.join(s2f, keyFn, ops.toArray)

    join.foreach { r =>
      val id = r(idCol)
      val count = r(countCol)
      if(count != 0)
      result += id -> (result.getOrElse(id, 0.0) + count)
    }
  }

  override def algo: Algorithm = Inner
}

object Stock2Merge extends Stock2 {

  import Stock2._
  import utils.Helper._
  val ord = sorting(keyFn, ops)
  override def evaluate(stocks: Table): Unit = {
    val s2f = new Table("S2", stocks.rows.filter(r => toMin(r(timeCol)) >= toMin(tval) - tconst).sorted(ord))
    val s3f = new Table("S3", stocks.rows.filter(r => r(timeCol) < tval).sorted(ord))

    val ids = Domain(s3f.rows.map(_(idCol)).distinct.toArray.sorted)
    val times = Domain(s3f.rows.map(_(timeCol)).distinct.toArray.sorted(Ordering[Double].reverse), false)
    val prices = Domain(s3f.rows.map(_(priceCol)).distinct.toArray.sorted)

    val cubeS3 = Cube.fromData(Array(ids, times, prices), s3f, keyFn, valueFn, AggPlus)
    cubeS3.accumulate(ops)
    val join = cubeS3.join(s2f, keyFn, ops.toArray)
    join.foreach { r =>
      val id = r(idCol)
      val count = r(countCol)
      if(count != 0)
      result += id -> (result.getOrElse(id, 0.0) + count)
    }
  }

  override def algo: Algorithm = Merge
}

object Stock2 {
  val idCol = 0
  val timeCol = 1
  val priceCol = 2
  val countCol = 3

  var tval: Double = 20
  val tconst: Double = 5

  val ops = List(EqualTo, GreaterThan, LessThan)
  val keyFn = (r: Row) => Array(r(idCol), r(timeCol), r(priceCol))
  val valueFn = (r: Row) => 1.0
 def main(args: Array[String]): Unit = {
   val stocks = new Table("Stocks", Bids.generate(10,10, 5, 5))
   val allTests = List(Stock2Naive, Stock2Range, Stock2Merge)
   allTests.foreach{ a=>
     val rt = a.execute(stocks)
     println(s"${a.query},${a.algo}, $rt")
     println(a.result.toList.sortBy(_._1).mkString(","))
   }
 }
}