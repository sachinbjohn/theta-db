package queries

import Math._
import datagen.Bids
import ds.{Cube, Domain, RangeTree, Row, Table}
import ddbt.lib.M3Map
import exec.{DBT, DBT_LMS, Inner, Merge, Naive, ParamsVWAP, VWAPExecutable}
import queries.dbt.VWAP1.TDLLDD
import queries.dbt.VWAP1Base
import utils.{AggPlus, LessThan}
import utils.Helper._

import scala.collection.mutable
import scala.collection.mutable.HashMap

abstract class VWAP1 extends VWAPExecutable {
  def evaluate(bids: Table): (Double, Long)

  override def execute(bids: Table): Long =
    evaluate(bids)._2

  override def query = "Q1"
}

object VWAP1Naive extends VWAP1 {

  import VWAP1._

  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n * n

  override def algo = Naive

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

object VWAP1_DBT_LMS extends VWAP1 {

  import VWAP1._

  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n + p * p

  override def algo = DBT_LMS

  override def evaluate(bids: Table): (Double, Long) = {
    val obj = new VWAP1Base
    //incoming = (p,t,v)
    //outgoing = (t, x, x, v, p)
    val DELTA_BIDS = M3Map.make[TDLLDD, Long]()
    bids.rows.foreach { r => DELTA_BIDS.add(new TDLLDD(r(timeCol), 0, 0, r(volCol), r(priceCol)), 1) }

    val start = System.nanoTime()
    obj.onSystemReady();
    obj.onBatchUpdateBIDS(DELTA_BIDS)
    val end = System.nanoTime()
    (obj.VWAP, end - start)
  }
}

object VWAP1DBT extends VWAP1 {

  import VWAP1._


  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n + p * p

  override def algo = DBT

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

  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n + p * log(p)

  override def algo = Inner

  override def evaluate(bids: Table): (Double, Long) = {
    val start = System.nanoTime()
    var result = 0.0

    var nC2 = 0.0
    val nC1 = new mutable.HashMap[Double, Double]()
    bids.rows.foreach { b =>
      val price = b(priceCol)
      val volume = b(volCol)
      nC2 += (0.25 * volume)
      nC1 += (price -> (nC1.getOrElse(price, 0.0) + volume))
    }

    val preAgg = new Table("BidsAgg", nC1.toList.map(kv => Row(Array(kv._1, kv._2))))

    val rtB2 = RangeTree.buildFrom(preAgg, keyVector, 1, valueFn2, AggPlus, "B2")
    val join = rtB2.join(preAgg, keyVector, op.toArray)
    join.foreach { r => if (nC2 < r(aggCol2)) result += r(priceCol) * r(volCol2) }
    val end = System.nanoTime()
    (result, end - start)
  }
}

object VWAP1Algo2 extends VWAP1 {
  override def algo = Merge

  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n + p * log(p)

  override def evaluate(bids: Table): (Double, Long) = {
    import VWAP1._

    val start = System.nanoTime()
    var result = 0.0

    var nC2 = 0.0
    val nC1 = new mutable.HashMap[Double, Double]()
    bids.rows.foreach { b =>
      val price = b(priceCol)
      val volume = b(volCol)
      nC2 += (0.25 * volume)
      nC1 += (price -> (nC1.getOrElse(price, 0.0) + volume))
    }

    val nC1Sorted = nC1.toList.sortBy(_._1)
    val preAgg = new Table("BidsAgg", nC1Sorted.map(kv => Row(Array(kv._1, kv._2))))

    val prices = Domain(nC1Sorted.map(_._1).toArray)
    val cubeB2 = Cube.fromData(Array(prices), preAgg, keyVector, valueFn2, AggPlus)

    cubeB2.accumulate(op)
    val join = cubeB2.join(preAgg, keyVector, op.toArray)

    join.foreach { r => if (nC2 < r(aggCol2)) result += r(priceCol) * r(volCol2) }
    val end = System.nanoTime()
    (result, end - start)
  }
}

object VWAP1 {
  var result = collection.mutable.ListBuffer[Long]()
  var exectime = collection.mutable.ListBuffer[Long]()


  val op = List(LessThan[Double])
  val priceCol = 0
  val timeCol = 1
  val volCol = 2
  val volCol2 = 1
  val aggCol = 3
  val aggCol2 = 2
  val keyVector = (r: Row) => Array(r(priceCol))
  val valueFn = (r: Row) => r(volCol)
  val valueFn2 = (r: Row) => r(volCol2)
  implicit val ord = sorting(keyVector, op)

  val allTests: List[VWAP1] = List(VWAP1Naive, VWAP1_DBT_LMS, VWAP1Algo1, VWAP1Algo2)


  def main(args: Array[String]) = {

    val maxTimeinMS = 1000 * 60 * 60
    (0 to 5).map(13 + 5 * _).foreach { all =>
      var numRuns = 1
      var logn = all
      var logr = all
      var logp = all
      var logt = 10

      if (args.length > 0) {
        logn = args(0).toInt
        logr = args(1).toInt
        logp = args(2).toInt
        logt = args(3).toInt
        numRuns = args(4).toInt
      }

      val bids = new Table("Bids", Bids.loadFromFile(logn, logr, logp, logt))

      allTests.foreach { case a =>
        if (a.enable) {
          exectime.clear();
          (1 to numRuns).foreach { i =>
            val rt = a.evaluate(bids)
            exectime += rt._2
            if (rt._2 / 1000000 > maxTimeinMS)
              a.enable = false
            if (i == numRuns) {
              result += rt._1.toLong
              println(s"${a.query},${a.algo},$logn,$logr,$logp,$logt," + exectime.map(_ / 1000000).mkString(","))
            }
          }

        }
        //println("Res = " + result.mkString(", "))
        //val res = result.head
        //assert(result.map(_ == res).reduce(_ && _))
      }
    }
  }
}


