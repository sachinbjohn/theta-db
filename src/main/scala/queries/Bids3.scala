package queries

import datagen.Bids
import ds._
import exec._
import utils._

abstract class Bids3 extends BidsExecutable {
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

  override def query: String = "Bids3"
}

object Bids3Naive extends Bids3 {

  import Bids3._

  override def evaluate(bids: Table): Unit = {
    bids.foreach { b1 =>
      var sum2 = 0.0
      var sum3 = 0.0
      var sum4 = 0.0
      var sum5 = 0.0

      bids.foreach { b2 =>
        if (b2(timeCol) < b1(timeCol)) {
          sum2 += 1.0
          sum3 += b2(priceCol) * b2(timeCol)
          sum4 += b2(priceCol)
          sum5 += b2(timeCol)
        }
      }
      verify += Row(b1.a ++ Array(sum2, sum3, sum4, sum5))
      if (sum2 * sum3 > sum4 * sum5)
        result += b1
    }
  }

  override def algo: Algorithm = Naive
}

object Bids3Range extends Bids3 {

  import Bids3._

  override def evaluate(bids: Table): Unit = {

    val rt2 = RangeTree.buildFrom(bids, keyFn, 1, valueFn2, AggPlus, "RT")
    val rt3 = RangeTree.buildFrom(bids, keyFn, 1, valueFn3, AggPlus, "RT")
    val rt4 = RangeTree.buildFrom(bids, keyFn, 1, valueFn4, AggPlus, "RT")
    val rt5 = RangeTree.buildFrom(bids, keyFn, 1, valueFn5, AggPlus, "RT")

    val b12 = rt2.join(bids, keyFn, ops.toArray)
    val b123 = rt3.join(b12, keyFn, ops.toArray)
    val b1234 = rt4.join(b123, keyFn, ops.toArray)
    val b12345 = rt5.join(b1234, keyFn, ops.toArray)

    verify ++= b12345.rows
    b12345.foreach { r =>
      if (r(agg2Col) * r(agg3Col) > r(agg4Col) * r(agg5Col))
        result += Row(Array(r(priceCol), r(timeCol), r(volCol)))
    }
  }

  override def algo: Algorithm = Inner
}

object Bids3Merge extends Bids3 {

  import Bids3._

  override def evaluate(bids: Table): Unit = {
    val times = Domain(bids.rows.map(_ (timeCol)).toArray.sorted)
    val sortedBids = new Table("sortedBids", bids.rows.sorted(ord))
    val cube2 = Cube.fromData(Array(times), sortedBids, keyFn, valueFn2, AggPlus)
    cube2.accumulate(ops)
    val cube3 = Cube.fromData(Array(times), sortedBids, keyFn, valueFn3, AggPlus)
    cube3.accumulate(ops)
    val cube4 = Cube.fromData(Array(times), sortedBids, keyFn, valueFn4, AggPlus)
    cube4.accumulate(ops)
    val cube5 = Cube.fromData(Array(times), sortedBids, keyFn, valueFn5, AggPlus)
cube5.accumulate(ops)

    val b12 = cube2.join(sortedBids, keyFn, ops.toArray)
    val b123 = cube3.join(b12, keyFn, ops.toArray)
    val b1234 = cube4.join(b123, keyFn, ops.toArray)
    val b12345 = cube5.join(b1234, keyFn, ops.toArray)

    verify ++= b12345.rows
    b12345.foreach { r =>
      if (r(agg2Col) * r(agg3Col) > r(agg4Col) * r(agg5Col))
        result += Row(Array(r(priceCol), r(timeCol), r(volCol)))
    }
  }

  override def algo: Algorithm = Merge
}

object Bids3 {
  val priceCol = 0
  val timeCol = 1
  val volCol = 2
  val agg2Col = 3
  val agg3Col = 4
  val agg4Col = 5
  val agg5Col = 6

  val ops = List(LessThan[Double]())
  val keyFn = (r: Row) => Array(r(timeCol))
  val valueFn2 = (r: Row) => 1.0
  val valueFn3 = (r: Row) => r(priceCol) * r(timeCol)
  val valueFn4 = (r: Row) => r(priceCol)
  val valueFn5 = (r: Row) => r(timeCol)

  val ord = Helper.sorting(keyFn, ops)

  def main(args: Array[String]) = {
    var exectime = collection.mutable.ListBuffer[Long]()
    var maxTimeInMS = 1000 * 60 * 5
    val allTests = List[Bids3](Bids3Naive, Bids3Range, Bids3Merge)
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
