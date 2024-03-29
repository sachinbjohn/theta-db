package queries

import datagen.Bids
import ddbt.lib._
import ds.{Cube, Domain, RangeTree, Row, Table}
import exec.{Algorithm, DBT, DBT_LMS, Inner, Merge, Naive, ParamsVWAP, BidsExecutable}
import queries.dbt.VWAP2.TDLLDD
import utils.{AggPlus, LessThan, LessThanEqual}
import utils.Helper._

import Math._
import scala.collection.mutable
import scala.collection.mutable.HashMap

abstract class VWAP2 extends BidsExecutable {

  override def execute(bids: Table): Long = evaluate(bids)._2

  override def query = "Q2"

  def evaluate(bids: Table): (Map[Double, Double], Long)
}

object VWAP2Naive extends VWAP2 {

  import VWAP2._

  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n * n

  override def algo = Naive

  override def evaluate(bids: Table): (Map[Double, Double], Long) = {
    var result = mutable.HashMap[Double, Double]()

    val start = System.nanoTime()
    bids.foreach { b1 => {
      var sum = 0.0
      var nC2 = 0.0
      bids.rows.foreach { b3 =>
        if (b3(timeCol) <= b1(timeCol))
          nC2 += 0.25 * b3(volCol)
      }
      bids.rows.foreach { b2 =>
        if (b2(priceCol) < b1(priceCol) && b2(timeCol) <= b1(timeCol))
          sum += b2(volCol)
      }
      if (nC2 < sum) {
        val v = b1(priceCol) * b1(volCol)
        val t = b1(timeCol)
        result += (t -> (result.getOrElse(t, 0.0) + v))
      }
    }
    }
    val end = System.nanoTime()
    (result.toMap, end - start)
  }
}

object VWAP2_DBT_LMS extends VWAP2 {

  import queries.dbt.VWAP2Base
  import VWAP2._

  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n + r * r

  override def algo: Algorithm = DBT_LMS

  override def evaluate(bids: Table): (Map[Double, Double], Long) = {
    val obj = new VWAP2Base
    val DELTA_BIDS = M3Map.make[TDLLDD, Long]()
    bids.rows.foreach { r => DELTA_BIDS.add(new TDLLDD(r(timeCol), 0, 0, r(volCol), r(priceCol)), 1) }

    val start = System.nanoTime()
    obj.onSystemReady();
    obj.onBatchUpdateBIDS(DELTA_BIDS)
    val end = System.nanoTime()
    (obj.VWAP.toMap, end - start)
  }
}

object VWAP2DBT extends VWAP2 {

  import VWAP2._


  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n + r * r

  override def algo: Algorithm = DBT

  override def evaluate(bids: Table): (Map[Double, Double], Long) = {
    val nA = new HashMap[(Double, Double), Double]()
    val nC1 = new HashMap[(Double, Double), Double]()
    var nC2 = new HashMap[Double, Double]()
    var result = new HashMap[Double, Double]()

    val start = System.nanoTime()
    bids.rows.foreach {
      b =>
        val price = b(priceCol)
        val volume = b(volCol)
        val time = b(timeCol)
        val pt = (price, time)
        nA += (pt -> (nA.getOrElse(pt, 0.0) + price * volume))
        nC2 += (time -> (nC2.getOrElse(time, 0.0) + 0.25 * volume))
        nC1 += (pt -> (nC1.getOrElse(pt, 0.0) + volume))

    }

    nA.foreach { case ((p1, t1), v1) =>
      var sum = 0.0;
      var c2 = 0.0
      nC2.foreach { case (t3, v3) => if (t3 <= t1) c2 += v3 }
      nC1.foreach { case ((p2, t2), v2) => if (p2 < p1 && t2 <= t1) sum += v2 };
      if (c2 < sum) {
        result += (t1 -> (result.getOrElse(t1, 0.0) + v1))
      }
    }
    val end = System.nanoTime()
    (result.toMap, end - start)
  }


}

object VWAP2Algo1 extends VWAP2 {

  import VWAP2._


  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n + r * pow(log(r), 2)

  override def algo: Algorithm = Inner

  override def evaluate(bidsx: Table): (Map[Double, Double], Long) = {
    val start = System.nanoTime()
    val nC1 = new HashMap[(Double, Double), Double]()
    var result = new HashMap[Double, Double]()

    bidsx.rows.foreach {
      b =>
        val price = b(priceCol)
        val volume = b(volCol)
        val time = b(timeCol)
        val pt = (price, time)
        nC1 += (pt -> (nC1.getOrElse(pt, 0.0) + volume))
    }

    val preAgg = new Table("Bids", nC1.toList.map { case ((p, t), v) => Row(Array(p, t, v)) })


    var rtB3 = RangeTree.buildFrom(preAgg, keyVector3, 1, 0.25 * valueFn(_), AggPlus, "B3")
    val rtB2 = RangeTree.buildFrom(preAgg, keyVector2, 2, valueFn, AggPlus, "B2")
    val B1B3 = rtB3.join(preAgg, keyVector3, op3.toArray)
    val join = rtB2.join(B1B3, keyVector2, op2.toArray)
    join.foreach { r =>
      val t = r(timeCol)
      val v = r(priceCol) * r(volCol)
      if (r(aggB3Col) < r(aggB2Col))
        result += (t -> (result.getOrElse(t, 0.0) + v))
    }
    val end = System.nanoTime()
    (result.toMap, end - start)
  }
}

object VWAP2Algo2 extends VWAP2 {


  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n + r * log(r) + p * t

  override def algo: Algorithm = Merge

  override def evaluate(bidsx: Table): (Map[Double, Double], Long) = {
    import VWAP2._
    val start = System.nanoTime()

    val nC1 = new HashMap[(Double, Double), Double]()
    var result = new HashMap[Double, Double]()

    bidsx.rows.foreach {
      b =>
        val price = b(priceCol)
        val volume = b(volCol)
        val time = b(timeCol)
        val pt = (price, time)
        nC1 += (pt -> (nC1.getOrElse(pt, 0.0) + volume))
    }

    val preAgg = new Table("Bids", nC1.toList.map { case ((p, t), v) => Row(Array(p, t, v)) }.sorted(ord))


    val prices = Domain(preAgg.rows.map(_ (priceCol)).distinct.toArray.sorted)
    val times = Domain(preAgg.rows.map(_ (timeCol)).distinct.toArray.sorted)

    var cubeB3 = Cube.fromData(Array(times), preAgg, keyVector3, valueFn(_) * 0.25, AggPlus)
    cubeB3.accumulate(op3)

    val cubeB2 = Cube.fromData(Array(times, prices), preAgg, keyVector2, valueFn, AggPlus)
    cubeB2.accumulate(op2)

    val B1B3 = cubeB3.join(preAgg, keyVector3, op3.toArray)
    val join = cubeB2.join(B1B3, keyVector2, op2.toArray)

    join.foreach { r =>
      val t = r(timeCol)
      val v = r(priceCol) * r(volCol)
      if (r(aggB3Col) < r(aggB2Col))
        result += (t -> (result.getOrElse(t, 0.0) + v))
    }
    val end = System.nanoTime()
    (result.toMap, end - start)
  }
}

object VWAP2 {
  var result = collection.mutable.ListBuffer[Map[Double, Double]]()
  var exectime = collection.mutable.ListBuffer[Long]()


  //time has to come first , for sorting to be correct
  val op2 = List(LessThanEqual, LessThan)
  val op3 = List(LessThanEqual)

  val priceCol = 0
  val timeCol = 1
  val volCol = 2
  val aggB3Col = 3
  val aggB2Col = 4

  val keyVector2 = (r: Row) => Array(r(timeCol), r(priceCol))
  val keyVector3 = (r: Row) => Array(r(timeCol))

  val valueFn = (r: Row) => r(volCol)
  implicit val ord = sorting(keyVector2, op2)

  val allTests: List[VWAP2] = List(VWAP2Naive, VWAP2_DBT_LMS, VWAP2Algo1, VWAP2Algo2)

  def main(args: Array[String]) = {

    var maxTimeInMS = 1000 * 60 * 5
    var testFlags = 0xFF
    var enable = true
    (10 to 28).foreach { all =>
      var logn = all
      var logr = all
      var logp = all
      var logt = 10
      var numRuns = 1

      if (args.length > 0) {
        //logn = args(0).toInt
        //logr = args(1).toInt
        //logp = args(2).toInt
        //logt = args(3).toInt
        //numRuns = args(4).toInt
        testFlags = args(0).toInt
        maxTimeInMS = args(1).toInt * 60 * 1000
      }

      if (enable) {
        val bids = new Table("Bids", Bids.loadFromFile(logn, logr, logp, logt))
        allTests.zipWithIndex.foreach { case (a, ai) =>
          if ((testFlags & (1 << ai)) != 0) {
            exectime.clear();
            (1 to numRuns).foreach { i =>
              val rt = a.evaluate(bids)
              exectime += rt._2
              if (i == numRuns) {
                result += rt._1.filter(_._2 != 0)
                println(s"${a.query},${a.algo},$logn,$logr,$logp,$logt," + exectime.map(_ / 1000000).mkString(","))
                if (rt._2 / 1000000 > maxTimeInMS)
                  enable = false
              }
            }
          }
        }
        //println("Res = \n" + result.map(_.toList.sortBy(_._1)).mkString("\n"))
        //val res = result.head
        //assert(result.map(_ equals res).reduce(_ && _))
      }
    }
  }

}
