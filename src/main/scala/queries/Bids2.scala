package queries

import datagen.Bids
import ddbt.lib.M3Map
import queries.dbt.Bids2._
import ds._
import exec._
import utils._

abstract class Bids2 extends BidsExecutable {
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

  override def query: String = "Bids2"
}

object Bids2Naive extends Bids2 {

  import Bids2._

  override def evaluate(bids: Table): Unit = {
    bids.foreach { b1 =>
      var max = Double.NegativeInfinity
      bids.foreach { b2 =>
        if (b2(timeCol) <= b1(timeCol) && b2(priceCol) > max)
          max = b2(priceCol)
      }
      verify += Row(b1.a.:+(max))
      if (b1(priceCol) == max)
        result += b1
    }
  }

  override def algo: Algorithm = Naive
}

object Bids2DBT extends Bids2 {

  import queries.dbt.Bids2Base
  import Bids2._

  override def evaluate(bids: Table): Unit = {
    val obj = new Bids2Base
    val DELTA_BIDS = M3Map.make[TDLLDD, Long]()
    bids.rows.foreach { r => DELTA_BIDS.add(new TDLLDD(r(timeCol), 0, 0, r(volCol), r(priceCol)), 1) }
    obj.onSystemReady()
    obj.onBatchUpdateBIDS(DELTA_BIDS)
  }

  override def algo: Algorithm = DBT_LMS
}

object Bids2Range extends Bids2 {

  import Bids2._

  override def evaluate(bids: Table): Unit = {

    val rt = RangeTree.buildFrom(bids, keyFn, 1, valueFn, AggMax, "RT")
    val join = rt.join(bids, keyFn, ops.toArray)
    verify ++= join.rows
    join.foreach { r =>
      if (r(priceCol) == r(aggCol))
        result += Row(Array(r(priceCol), r(timeCol), r(volCol)))
    }
  }
  override def algo: Algorithm = Inner
}

object Bids2Merge extends Bids2 {
  import Bids2._

  override def evaluate(bids: Table): Unit = {
    val times = Domain(bids.rows.map(_(timeCol)).toArray.sorted)
    val sortedBids = new Table("sortedBids", bids.rows.sorted(ord))
    val cube = Cube.fromData(Array(times), sortedBids, keyFn, valueFn, AggMax)
    cube.accumulate(ops)
    val join = cube.join(sortedBids, keyFn, ops.toArray)
    verify ++= join.rows
    join.foreach { r =>
      if (r(priceCol) == r(aggCol))
        result += Row(Array(r(priceCol), r(timeCol), r(volCol)))
    }
  }

  override def algo: Algorithm = Merge
}
object Bids2 {
  val priceCol = 0
  val timeCol = 1
  val volCol = 2
  val aggCol = 3

  val ops = List(LessThanEqual[Double]())
  val keyFn = (r: Row) => Array(r(timeCol))
  val valueFn = (r: Row) => r(priceCol)

  val ord = Helper.sorting(keyFn, ops)

  def main(args: Array[String]) = {
    var exectime = collection.mutable.ListBuffer[Long]()
    var maxTimeInMS = 1000 * 60 * 5
    val allTests = List[Bids2](Bids2Naive, Bids2DBT, Bids2Range, Bids2Merge)
    var testFlags = 0xFF
    var enable = true
    (10 to 28).foreach { all =>
      var logn = all
      var logr = all
      var logp = all - 3
      var logt = all - 7
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
