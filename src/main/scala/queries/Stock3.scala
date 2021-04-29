package queries
import datagen.Bids
import ds._
import exec._
import utils._

abstract class Stock3 extends StockExecutable{
  val result = collection.mutable.HashMap[(Double, Double), Double]()
  def evaluate(stocks: Table):Unit
  override def execute(stocks: Table): Long = {
    result.clear()
    val start = System.nanoTime()
    evaluate(stocks)
    val end = System.nanoTime()
    (end - start) / onemill
  }
  override def query: String = "Stock3"
}
object Stock3Naive extends Stock3{
  import Stock3._
  override def evaluate(stocks: Table): Unit = {
    stocks.rows.foreach {s1 =>
      val id = s1(idCol)
      val time = s1(timeCol)
      stocks.rows.foreach{ s2 =>
      if(s2(idCol) == id && toMin(s2(timeCol)) >= toMin(s1(timeCol)) - tconst)
        stocks.rows.foreach{ s3 =>
          if(s3(idCol) == id && s3(timeCol) < s1(timeCol) && s3(timeCol) > s2(timeCol) && s3(priceCol) < s2(priceCol)) {
            result += (id,time) -> (result.getOrElse((id, time), 0.0) + 1.0)
          }
        }
      }
    }
  }
  override def algo: Algorithm = Naive
}

object Stock3Range extends Stock3 {
  import Stock3._
  override def evaluate(stocks: Table): Unit = {
    val rts3 = RangeTree.buildFrom(stocks, keyFn23, 3, valueFn, AggPlus, "S3")
    val s2s3 = rts3.join(stocks, keyFn23, ops23.toArray)

    /*
      S1 * ( S2 * (id2=id1) * (t2 >= t1-5) * S3 * (t3 < t1) * (id3=id2) (t3 > t2) * (p3 < p2))

     S2S3 := S2 * S3 * (id3 = id2) * (t3 > t2) * (p3 < p2)

     S1 * S2S3 * (id2 = id1) * (t2 >= t1-5) * (t3 < t1)
     */
  }
  override def algo: Algorithm = Inner
}
object Stock3 {
  val idCol = 0
  val timeCol = 1
  val priceCol = 2
  val countCol = 3
  val ops23 = List(EqualTo[Double], GreaterThan[Double], LessThan[Double])
  val keyFn23 = (r: Row) => Array(r(idCol), r(timeCol), r(priceCol))
  val valueFn = (r: Row) => 1.0
  val tconst: Double = 5

}