package query

import datagen.Bids
import ds.{Cube, Domain, RangeTree, Row, Table}
import utils.{AggPlus, LessThan}
import utils.Helper._
import scala.collection.mutable.HashMap

trait VWAP1 {
  def evaluate(bids: Table): (Double, Long)
}

object VWAP1Naive extends VWAP1 {

  import VWAP1._

  override def evaluate(bids: Table): (Double, Long) = {
    var nC2 = 0.0
    var result = 0.0

    val start = System.nanoTime()
    bids.rows.foreach { b => nC2 += (0.25 * b(volCol)) }
    bids.foreach { b1 => {
      var sum = 0.0
      bids.rows.foreach { b2 =>
        if (b2(priceCol) < b1(priceCol))
          sum += b2(volCol)
      }
      if (nC2 < sum)
        result += b1(priceCol) * b1(volCol)
    }
    }
    val end = System.nanoTime()
    (result, end - start)
  }
}

object VWAP1DBT extends VWAP1 {

  import VWAP1._




  override def evaluate(bids: Table): (Double, Long) = {
    val nA = new HashMap[Double, Double]()
    val nC1 = new HashMap[Double, Double]()
    var nC2 = 0.0
    var result = 0.0

    val start = System.nanoTime()
    bids.rows.foreach {
      b =>
        val price = b(priceCol)
        val volume = b(volCol)
        nA += (price -> (nA.getOrElse(price, 0.0) + price * volume))
        nC2 += (0.25 * volume)
        nC1 += (price -> (nC1.getOrElse(price, 0.0) + volume))

    }

    result = 0
    nA.foreach { case (p1, v1) => var sum = 0.0;
      nC1.foreach { case (p2, v2) => if (p2 < p1) sum += v2 };
      if (nC2 < sum) {
        result += v1
      }
    }
    val end = System.nanoTime()
    (result, end - start)
  }


}

object VWAP1Algo1 extends VWAP1 {

  import VWAP1._

  override def evaluate(bids: Table): (Double, Long) = {
    val start = System.nanoTime()
    var result = 0.0

    var nC2 = 0.0
    bids.rows.foreach { b => nC2 += (0.25 * b(volCol)) }

    val rtB2 = RangeTree.buildFrom(bids, keyVector, 1, valueFn, AggPlus, "B2")
    val join = rtB2.join(bids, keyVector , op.toArray)
    join.foreach { r => if (nC2 < r(aggCol)) result += r(priceCol) * r(volCol) }
    val end = System.nanoTime()
    (result, end - start)
  }
}

object VWAP1Algo2 extends VWAP1 {
  override def evaluate(bids: Table): (Double, Long) = {
    import VWAP1._
    val start = System.nanoTime()
    var result = 0.0

    var nC2 = 0.0
    bids.rows.foreach { b => nC2 += (0.25 * b(volCol)) }

    val prices = Domain(bids.rows.map(_(priceCol)).distinct.toArray.sorted)
    val cubeB2 = Cube.fromData(Array(prices), bids, keyVector, valueFn)

    cubeB2.accumulate(op)
    val join = cubeB2.join(bids, keyVector, op.toArray)

    join.foreach { r => if (nC2 < r(aggCol)) result += r(priceCol) * r(volCol) }
    val end = System.nanoTime()
    (result, end - start)
  }
}

object VWAP1 {
  var result = collection.mutable.ListBuffer[Double]()
  var exectime = collection.mutable.ListBuffer[Long]()
  var test = 31
  lazy val enableNaive = (test & 1) == 1
  lazy val enableDBT = (test & 2) == 2
  lazy val enableAlgo1 = (test & 4) == 4
  lazy val enableAlgo2 = (test & 8) == 8


  val op = List(LessThan[Double])
  val priceCol = 0
  val timeCol = 1
  val volCol = 2
  val aggCol = 3
  val keyVector = (r: Row) => Array(r(priceCol))
  val valueFn = (r: Row) => r(volCol)
  implicit val ord = sorting(keyVector, op)

  def main(args: Array[String]) = {

    var total = 1 << 10
    var price = 1 << 5
    var time = 1 << 5
    var pricetime = 1 << 9
    var numRuns = 3

    if (args.length > 0) {
      total = args(0).toInt
      price = args(1).toInt
      time = args(2).toInt
      pricetime = args(3).toInt
      numRuns = args(4).toInt
      test = args(5).toInt
    }


    val bids = new Table("Bids", Bids.generate(total, price, time, pricetime).sorted)
    (1 to numRuns).foreach { i =>
      if (enableNaive) {
        val rt = VWAP1Naive.evaluate(bids)
        result += rt._1
        exectime += rt._2
      }
      if (enableDBT) {
        val rt = VWAP1DBT.evaluate(bids)
        result += rt._1
        exectime += rt._2
      }
      if (enableAlgo1) {
        val rt = VWAP1Algo1.evaluate(bids)
        result += rt._1
        exectime += rt._2
      }
      if (enableAlgo2) {
        val rt = VWAP1Algo2.evaluate(bids)
        result += rt._1
        exectime += rt._2
      }
    }
    //println("Res = " + result.mkString(", "))
    val res = result.head
    assert(result.map(_ == res).reduce(_ && _))
    println(s"Q1,$test,$total,$price,$time,$pricetime," + exectime.map(_ / 1000000).mkString(","))
  }
}