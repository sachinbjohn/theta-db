package queries

import datagen.Bids
import ds._
import exec._
import utils._
import ddbt.lib.M3Map
import queries.dbt.Bids6._

abstract class Bids6 extends BidsExecutable {
  val verify = collection.mutable.ListBuffer[Row]()
  var result = 0.0

  override def execute(bids: Table): Long = {
    verify.clear()
    result = 0.0
    val start = System.nanoTime()
    evaluate(bids)
    val end = System.nanoTime()
    (end - start) / onemill
  }

  def evaluate(bids: Table): Unit

  override def cost(n: Int, r: Int, p: Int, t: Int): Double = ???

  override def query: String = "Bids6"
}

object Bids6Naive extends Bids6 {

  import Bids6._

  override def evaluate(bids: Table): Unit = {

    bids.foreach { b1 =>
      var sum1 = 0.0
      if (b1(timeCol) > t1 && b1(timeCol) < t2) {
        bids.foreach { b2 =>
          if (b2(timeCol) > t1 && b2(timeCol) < t2) {
            if (b2(timeCol) > b1(timeCol) && b2(priceCol) < b1(priceCol))
              sum1 += 1.0

          }
        }
        verify += Row(b1.a.:+(sum1))
      }

      result += sum1
    }
  }

  override def algo: Algorithm = Naive
}
object Bids6DBT extends Bids6 {
  import queries.dbt.Bids6Base
  import Bids6._
  override def evaluate(bids: Table): Unit = {
    val obj = new Bids6Base
    val DELTA_BIDS = M3Map.make[TDLLDD, Long]()
    bids.rows.foreach{ r=> DELTA_BIDS.add(new TDLLDD(r(timeCol), 0, 0, r(volCol), r(priceCol)), 1) }
    obj.onSystemReady()
    obj.onBatchUpdateBIDS(DELTA_BIDS)
  }
  override def algo: Algorithm = DBT_LMS
}
object Bids6Range extends Bids6 {

  import Bids6._

  override def evaluate(bidsX: Table): Unit = {
    val bids = new Table("Sad", bidsX.rows.filter(r => r(timeCol) > t1 && r(timeCol) < t2))
    val rt = RangeTree.buildFrom(bids, keyFn, 2, valueFn, AggPlus, "RT")
    val join = rt.join(bids, keyFn, ops.toArray)
    verify ++= join.rows
    join.foreach { r =>
        result += r(aggCol)
    }
  }

  override def algo: Algorithm = Inner
}

object Bids6Merge extends Bids6 {

  import Bids6._

  override def evaluate(bidsX: Table): Unit = {
    val bids = new Table("Sad", bidsX.rows.filter(r => r(timeCol) > t1 && r(timeCol) < t2))
    val times = Domain(bids.rows.map(_ (timeCol)).toArray.sorted(Ordering[Double].reverse), false)
    val prices = Domain(bids.rows.map(_ (priceCol)).toArray.sorted)
    val sortedBids = new Table("sortedBids", bids.rows.sorted(ord))
    val cube = Cube.fromData(Array(times, prices), sortedBids, keyFn, valueFn, AggPlus)
    cube.accumulate(ops)
    val join = cube.join(sortedBids, keyFn, ops.toArray)
    verify ++= join.rows
    join.foreach { r =>
        result += r(aggCol)
    }
  }

  override def algo: Algorithm = Merge
}

object Bids6 {
  val priceCol = 0
  val timeCol = 1
  val volCol = 2
  val aggCol = 3
  var t1 = Double.NegativeInfinity
  var t2 = Double.PositiveInfinity
  val ops = List(GreaterThan[Double], LessThan[Double])
  val keyFn = (r: Row) => Array(r(timeCol), r(priceCol))
  val valueFn = (r: Row) => 1.0

  val ord = Helper.sorting(keyFn, ops)

  def main(args: Array[String]) = {
    var exectime = collection.mutable.ListBuffer[Long]()
    var maxTimeInMS = 1000 * 60 * 5
    val allTests = List[Bids6](Bids6Naive, Bids6DBT, Bids6Range, Bids6Merge)
    var testFlags = 0xFF
    var enable = true
    (10 to 28).foreach { all =>
      var logn = all
      var logr = all
      var logp = all-3
      var logt = all-7
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
