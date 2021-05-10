package queries

import datagen.Bids
import ds._
import exec._
import queries.Bids5Range.result
import utils._
import ddbt.lib.M3Map
import queries.dbt.Bids5._
abstract class Bids5 extends BidsExecutable {
  val verify = collection.mutable.ListBuffer[Row]()
  val result = collection.mutable.ListBuffer[Row]()

  override def execute(bids: Table): Long = {
    verify.clear()
    result.clear()
    val start = System.nanoTime()
    evaluate(bids)
    val end = System.nanoTime()
    (end - start) / onemill
  }

  def evaluate(bids: Table): Unit

  override def cost(n: Int, r: Int, p: Int, t: Int): Double = ???

  override def query: String = "Bids5"
}

object Bids5Naive extends Bids5 {

  import Bids5._

  override def evaluate(bids: Table): Unit = {
    bids.foreach { b1 =>
      var maxTime = Double.NegativeInfinity
      bids.foreach { b3 =>
        if (b3(timeCol) < b1(timeCol) && b3(timeCol) > maxTime)
          maxTime = b3(timeCol)
      }
      var maxPrice = Double.NegativeInfinity
      bids.foreach { b2 =>
        if (b2(timeCol) == maxTime && b2(priceCol) > maxPrice) {
          maxPrice = b2(priceCol)
        }
      }
      verify += Row(b1.a ++ Array(maxTime, maxPrice))
      if (b1(priceCol) >= 1.1 * maxPrice)
        result += b1
    }
  }

  override def algo: Algorithm = Naive
}
object Bids5DBT extends Bids5 {
  import queries.dbt.Bids5Base
  import Bids5._
  override def evaluate(bids: Table): Unit = {
    val obj = new Bids5Base
    val DELTA_BIDS = M3Map.make[TDLLDD, Long]()
    bids.rows.foreach{ r=> DELTA_BIDS.add(new TDLLDD(r(timeCol), 0, 0, r(volCol), r(priceCol)), 1) }
    obj.onSystemReady()
    obj.onBatchUpdateBIDS(DELTA_BIDS)
  }
  override def algo: Algorithm = DBT_LMS
}

object Bids5Range extends Bids5 {

  import Bids5._

  override def evaluate(bids: Table): Unit = {

    val rt3 = RangeTree.buildFrom(bids, keyFn, 1, valueFn3, AggMax, "RT")
    val rt2 = RangeTree.buildFrom(bids, keyFn, 1, valueFn2, AggMax, "RT")

    val b13 = rt3.join(bids, keyFn, ops3.toArray)
    val b132 = rt2.join(b13, keyFn2Outer, ops2.toArray)

    verify ++= b132.rows
    b132.foreach { r =>
      if (r(priceCol) >= 1.1 * r(agg2Col))
        result += Row(Array(r(priceCol), r(timeCol), r(volCol)))
    }
  }

  override def algo: Algorithm = Inner
}

object Bids5Merge extends Bids5 {

  import Bids5._

  override def evaluate(bids: Table): Unit = {
    val times = Domain(bids.rows.map(_ (timeCol)).distinct.toArray.sorted)
    val sortedBidsInner = new Table("sortedBids", bids.rows.sorted(ord))
    val cube2 = Cube.fromData(Array(times), sortedBidsInner, keyFn, valueFn2, AggMax)
    cube2.accumulate(ops2)
    val cube3 = Cube.fromData(Array(times), sortedBidsInner, keyFn, valueFn3, AggMax)
    cube3.accumulate(ops3)

    val ord2 = Helper.sortingOther(Array((times, true)), keyFn2Outer, ops2)
    val b13 = cube3.join(sortedBidsInner, keyFn, ops3.toArray)
    val b13s = new Table("sda", b13.rows.sorted(ord2))
    val b132 = cube2.join(b13s, keyFn2Outer, ops2.toArray)

    verify ++= b132.rows
    b132.foreach { r =>
      if (r(priceCol) >= 1.1 * r(agg2Col))
        result += Row(Array(r(priceCol), r(timeCol), r(volCol)))
    }
  }

  override def algo: Algorithm = Merge
}

object Bids5 {
  val priceCol = 0
  val timeCol = 1
  val volCol = 2
  val agg3Col = 3
  val agg2Col = 4


  val ops3 = List(LessThan[Double]())
  val ops2 = List(EqualTo[Double]())

  val keyFn = (r: Row) => Array(r(timeCol))
  val keyFn2Outer = (r: Row) => Array(r(agg3Col))

  val valueFn2 = (r: Row) => r(priceCol)
  val valueFn3 = (r: Row) => r(timeCol)

  val ord = Helper.sorting(keyFn, ops3)

  def main(args: Array[String]) = {
    var exectime = collection.mutable.ListBuffer[Long]()
    var maxTimeInMS = 1000 * 60 * 5
    val allTests = List[Bids5](Bids5Naive, Bids5DBT, Bids5Range, Bids5Merge)
    var testFlags = 0xFF
    var enable = true
    (10 to 28).foreach { all =>
      var logn = all
      var logr = all
      var logp = all
      var logt = all
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
              val rt = a.execute(bids)
              exectime += rt
              if (i == numRuns) {
                println(s"${a.query},${a.algo},$logn,$logr,$logp,$logt," + exectime.mkString(","))
                if (rt > maxTimeInMS)
                  enable = false
              }
            }
          }
        }
        //val ver = allTests.map(_.verify.toList.sorted(ord))
        //assert(ver.forall(_ equals ver.head))
        //val res = allTests.map(_.result.toList.sorted(ord))
        //assert(res.forall(_ equals res.head))
      }
    }
  }
}
