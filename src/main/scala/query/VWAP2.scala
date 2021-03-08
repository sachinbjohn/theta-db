package query

import datagen.Bids
import ds.{Cube, Domain, RangeTree, Row, Table}
import utils.{AggPlus, LessThan, LessThanEqual}
import utils.Helper._

import scala.collection.mutable
import scala.collection.mutable.HashMap

trait VWAP2 {
  def evaluate(bids: Table): (Map[Double, Double], Long)
}

object VWAP2Naive extends VWAP2 {

  import VWAP2._

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

object VWAP2DBT extends VWAP2 {

  import VWAP2._



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

  override def evaluate(bids: Table): (Map[Double, Double], Long) = {
    val start = System.nanoTime()
    var result = mutable.HashMap[Double, Double]()

    var rtB3 = RangeTree.buildFrom(bids, keyVector3, 1, 0.25 * valueFn(_), AggPlus, "B3")
    val rtB2 = RangeTree.buildFrom(bids, keyVector2, 2, valueFn, AggPlus, "B2")
    val B1B3 = rtB3.join(bids, keyVector3, op3.toArray)
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
  override def evaluate(bids: Table): (Map[Double, Double], Long) = {
    import VWAP2._
    val start = System.nanoTime()
    var result = mutable.HashMap[Double, Double]()

    val prices = Domain(bids.rows.map(_ (priceCol)).distinct.toArray.sorted)
    val times = Domain(bids.rows.map(_ (timeCol)).distinct.toArray.sorted)

    var cubeB3 = Cube.fromData(Array(times), bids, keyVector3, valueFn(_) * 0.25)
    cubeB3.accumulate(op3)

    val cubeB2 = Cube.fromData(Array(times, prices), bids, keyVector2, valueFn)
    cubeB2.accumulate(op2)

    val B1B3 = cubeB3.join(bids, keyVector3, op3.toArray)
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
  var test = 31
  lazy val enableNaive = (test & 1) == 1
  lazy val enableDBT = (test & 2) == 2
  lazy val enableAlgo1 = (test & 4) == 4
  lazy val enableAlgo2 = (test & 8) == 8


  //time has to come first , for sorting to be correct
  val op2 = List( LessThanEqual[Double], LessThan[Double])
  val op3 = List(LessThanEqual[Double])

  val priceCol = 0
  val timeCol = 1
  val volCol = 2
  val aggB3Col = 3
  val aggB2Col = 4

  val keyVector2 = (r: Row) => Array(r(timeCol), r(priceCol))
  val keyVector3 = (r: Row) => Array(r(timeCol))

  val valueFn = (r: Row) => r(volCol)
  implicit val ord = sorting(keyVector2, op2)

  def main(args: Array[String]) = {

    var total = 1 << 10
    var price = 1 << 5
    var time = 1 << 5
    var density = 0.5
    var numRuns = 3
    if(args.length > 0) {
      total = args(0).toInt
      price = args(1).toInt
      time = args(2).toInt
      density = args(3).toDouble
      numRuns = args(4).toInt
    }

    if(density * price * time >= total) {
      density = (total-1) / (price * time)
    }

    val bids = new Table("Bids", Bids.generate(total, price, time, density).sorted)
    (1 to numRuns).foreach { i =>
      if (enableNaive) {
        val rt = VWAP2Naive.evaluate(bids)
        result += rt._1
        exectime += rt._2
      }
      if (enableDBT) {
        val rt = VWAP2DBT.evaluate(bids)
        result += rt._1
        exectime += rt._2
      }
      if (enableAlgo1) {
        val rt = VWAP2Algo1.evaluate(bids)
        result += rt._1
        exectime += rt._2
      }
      if (enableAlgo2) {
        val rt = VWAP2Algo2.evaluate(bids)
        result += rt._1
        exectime += rt._2
      }
    }
    //println("Res = \n " + result.map(_.mkString(",")).mkString("\n "))
    val res = result.head
    assert(result.map(_.equals(res)).reduce(_ && _))
    println(s"Q2,$total,$price,$time,$density," + exectime.map(_ / 1000000).mkString(","))
  }
}