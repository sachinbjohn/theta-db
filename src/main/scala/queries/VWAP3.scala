package queries

import datagen.Bids
import ddbt.lib.M3Map
import ds.{Cube, Domain, RangeTree, Row, Table}
import exec.{Algorithm, DBT, DBT_LMS, Inner, Merge, Naive, VWAPExecutable}
import queries.dbt.VWAP3.TDLLDD
import utils.{AggPlus, GreaterThanEqual, LessThan, LessThanEqual}
import utils.Helper._

import scala.collection.mutable
import scala.collection.mutable.HashMap
import Math._
trait VWAP3 extends  VWAPExecutable{
  def evaluate(bids: Table): (Map[Double, Double], Long)

  override def execute(bids: Table): Long = evaluate(bids)._2

  override def query: String = "Q3"
}

object VWAP3Naive extends VWAP3 {

  import VWAP3Obj._

  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n * n

  override def algo: Algorithm = Naive

  override def evaluate(bids: Table): (Map[Double, Double], Long) = {
    var result = mutable.HashMap[Double, Double]()

    val start = System.nanoTime()
    bids.foreach { b1 => {
      var sum = 0.0
      var nC2 = 0.0
      bids.rows.foreach { b3 =>
        if (b3(timeCol) <= b1(timeCol) && b3(timeCol) >= b1(timeCol) - 10)
          nC2 += 0.25 * b3(volCol)
      }
      bids.rows.foreach { b2 =>
        if (b2(priceCol) < b1(priceCol) && b2(timeCol) <= b1(timeCol) && b2(timeCol) >= b1(timeCol) - 10)
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

object VWAP3_DBT_LMS extends  VWAP3{
  import queries.dbt.VWAP3Base
  import VWAP3Obj._

  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n + r * r

  override def algo: Algorithm = DBT_LMS

  override def evaluate(bids: Table): (Map[Double, Double], Long) = {
    val obj = new VWAP3Base
    val DELTA_BIDS = M3Map.make[TDLLDD, Long]()
    bids.rows.foreach{r => DELTA_BIDS.add(new TDLLDD(r(timeCol), 0, 0, r(volCol), r(priceCol)), 1)}

    val start = System.nanoTime()
    obj.onBatchUpdateBIDS(DELTA_BIDS)
    val end = System.nanoTime()
    (obj.VWAP.toMap, end-start)
  }
}

object VWAP3DBT extends VWAP3 {

  import VWAP3Obj._

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
      nC2.foreach { case (t3, v3) => if (t3 <= t1 && t3 >= t1 - 10) c2 += v3 }
      nC1.foreach { case ((p2, t2), v2) => if (p2 < p1 && t2 <= t1 && t2 >= t1 - 10) sum += v2 };

      //println((p1, t1, c2, sum))
      if (c2 < sum) {
        result += (t1 -> (result.getOrElse(t1, 0.0) + v1))
      }
    }
    val end = System.nanoTime()
    (result.toMap, end - start)
  }


}

object VWAP3Algo1 extends VWAP3 {

  import VWAP3Obj._


  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n + r * pow(log(r), 3)

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

    val preAgg = new Table("Bids", nC1.toList.map{case ((p,t),v) => Row(Array(p, t, v))})


    var rtB3 = RangeTree.buildFrom(preAgg, keyVector3S, 2, 0.25 * valueFn(_), AggPlus, "B3")
    val rtB2 = RangeTree.buildFrom(preAgg, keyVector2S, 3, valueFn, AggPlus, "B2")
    val B1B3 = rtB3.join(preAgg, keyVector3R, op3.toArray)
    val join = rtB2.join(B1B3, keyVector2R, op2.toArray)

    //println("Algo1")
    join.foreach { r =>
      //println(r)
      val t = r(timeCol)
      val v = r(priceCol) * r(volCol)
      if (r(aggB3Col) < r(aggB2Col))
        result += (t -> (result.getOrElse(t, 0.0) + v))
    }
    val end = System.nanoTime()
    (result.toMap, end - start)
  }
}

object VWAP3Algo2 extends VWAP3 {


  override def cost(n: Int, r: Int, p: Int, t: Int): Double = n + p * t * t + r * log(r)

  override def algo: Algorithm = Merge

  override def evaluate(bidsx: Table): (Map[Double, Double], Long) = {
    import VWAP3Obj._
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

    val preAgg = new Table("Bids", nC1.toList.map{case ((p,t),v) => Row(Array(p, t, v))}.sorted(ord))

    val prices = Domain(preAgg.rows.map(_ (priceCol)).distinct.toArray.sorted)
    val tvs = preAgg.rows.map(_ (timeCol)).distinct.toArray
    val times = Domain(tvs.sorted)
    val times2 = Domain(tvs.sorted(Ordering[Double].reverse))

    var cubeB3 = Cube.fromData(Array(times, times2), preAgg, keyVector3S, valueFn(_) * 0.25)
    cubeB3.accumulate(op3)

    val cubeB2 = Cube.fromData(Array(times, times2, prices), preAgg, keyVector2S, valueFn)
    cubeB2.accumulate(op2)

    val B1B3 = cubeB3.join(preAgg, keyVector3R, op3.toArray)
    val join = cubeB2.join(B1B3, keyVector2R, op2.toArray)

    //println("Algo2")
    join.foreach { r =>
      //println(r)
      val t = r(timeCol)
      val v = r(priceCol) * r(volCol)
      if (r(aggB3Col) < r(aggB2Col))
        result += (t -> (result.getOrElse(t, 0.0) + v))
    }
    val end = System.nanoTime()
    (result.toMap, end - start)
  }
}

object VWAP3Obj {
  var result = collection.mutable.ListBuffer[Map[Double, Double]]()
  var exectime = collection.mutable.ListBuffer[Long]()
  var test = 0xFFFF

  val allTests: List[VWAP3] = List(VWAP3Naive, VWAP3DBT, VWAP3Algo1, VWAP3Algo2, VWAP3_DBT_LMS)


  //time has to come first , for sorting to be correct
  val op2 = List(LessThanEqual[Double], GreaterThanEqual[Double], LessThan[Double])
  val op3 = List(LessThanEqual[Double], GreaterThanEqual[Double])

  val priceCol = 0
  val timeCol = 1
  val volCol = 2
  val aggB3Col = 3
  val aggB2Col = 4

  val keyVector2S = (r: Row) => Array(r(timeCol), r(timeCol), r(priceCol))
  val keyVector2R = (r: Row) => Array(r(timeCol), r(timeCol) - 10, r(priceCol))
  val keyVector3S = (r: Row) => Array(r(timeCol), r(timeCol))
  val keyVector3R = (r: Row) => Array(r(timeCol), r(timeCol) - 10)

  val valueFn = (r: Row) => r(volCol)
  implicit val ord = sorting(keyVector2S, op2)

  def main(args: Array[String]) = {
    var total = 1 << 10
    var price = 1 << 5
    var time = 1 << 5
    var pricetime = 1 << 9
    var numRuns = 1

    if (args.length > 0) {
      total = args(0).toInt
      price = args(1).toInt
      time = args(2).toInt
      pricetime = args(3).toInt
      numRuns = args(4).toInt
      test = args(5).toInt
    }


    val bids = new Table("Bids", Bids.generate(total, price, time, pricetime))
    (1 to numRuns).foreach { i =>
      allTests.zipWithIndex.foreach { case (a, ai) =>
        if((1 << ai & test) != 0 ) {
          val rt = a.evaluate(bids)
          result += rt._1
          exectime += rt._2
        }
      }
    }
    // println("Res = \n " + result.map(_.mkString(",")).mkString("\n "))
    val res = result.head
    assert(result.map(_.equals(res)).reduce(_ && _))
    println(s"Q3,$test,$total,$price,$time,$pricetime,   " + exectime.map(_ / 1000000).mkString(","))
  }
}