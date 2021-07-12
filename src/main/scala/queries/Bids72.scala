package queries

import datagen.Bids
import ds._
import exec._
import utils._
import ddbt.lib.M3Map

abstract class Bids7 extends BidsExecutable {
  val verify = collection.mutable.ListBuffer[Row]()
  var result = collection.mutable.ListBuffer[Row]()

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

}

abstract class Bids72 extends Bids7 {
  override def query: String = "Bids72"
}

object Bids72Naive extends Bids72 {

  import Bids72._

  override def evaluate(bids: Table): Unit = {
    bids.foreach { b1 =>
      var sum1 = 0.0
      bids.foreach { b3 =>
        bids.foreach { b2 =>
          if (b2(timeCol) < b1(timeCol)) {
            if (b3(timeCol) < b2(timeCol) && b3(priceCol) > b2(priceCol))
              sum1 += 1.0
          }
        }
      }
      verify += Row(b1.a.:+(sum1))
      result += Row(b1.a.:+(sum1))
    }
  }

  override def algo: Algorithm = Naive
}

/*
object Bids72DBT extends Bids72 {

  import queries.dbt.Bids72Base
  import Bids72._

  override def evaluate(bids: Table): Unit = {
    val obj = new Bids72Base
    obj.tstart = tstart
    obj.tend = tend
    val DELTA_BIDS = M3Map.make[TDLLDD, Long]()
    bids.rows.foreach { r => DELTA_BIDS.add(new TDLLDD(r(timeCol), 0, 0, r(volCol), r(priceCol)), 1) }
    obj.onSystemReady()
    obj.onBatchUpdateBIDS(DELTA_BIDS)
  }

  override def algo: Algorithm = DBT_LMS
}
*/

object Bids72Range extends Bids72 {

  import Bids72._

  override def evaluate(bids: Table): Unit = {

    val rt2 = MultiRangeTree.buildFrom(bids, gbyFn32, emptyFn, keyFn32, valueFn32, AggPlus, ops32.toArray)
    val b32 = rt2.join(bids, emptyFn, keyFn32)

    val rt32 = MultiRangeTree.buildFrom(b32, emptyFn, emptyFn, keyFn132Inner, valueFn132, AggPlus, ops132.toArray)
    val b132 = rt32.join(bids, emptyFn, keyFn132Outer)
    verify ++= b132.rows
    result ++= b132.rows
  }

  override def algo: Algorithm = Inner
}

object Bids72Merge extends Bids72 {

  import Bids72._

  override def evaluate(bids: Table): Unit = {
    //val t1 = System.nanoTime()
    val sortedBids = new Table("sortedBids", bids.rows.sorted(ord32))
    //val t2 = System.nanoTime()
    val cube2 = MultiCube.fromData(sortedBids, gbyFn32, emptyFn, keyFn32, valueFn32, AggPlus, ops32.toArray)
    val t3 = System.nanoTime()
    val b32 = cube2.join(sortedBids, emptyFn, keyFn32)
    val t4 = System.nanoTime()
    val sorted32 = new Table("sorted32", b32.rows.sorted(ord132))
    //val t5 = System.nanoTime()
    val cube23 = MultiCube.fromData(sorted32, emptyFn, emptyFn, keyFn132Inner, valueFn132, AggPlus, ops132.toArray)
    //val t6 = System.nanoTime()
    val b132 = cube23.join(sortedBids, emptyFn, keyFn132Outer)
    //val t7 = System.nanoTime()
    val allts = List(t4 - t3).map(_ / 1000000).zipWithIndex.foreach(println)
    verify ++= b132.rows
    result ++= b132.rows

  }

  override def algo: Algorithm = Merge
}

object Bids72 {
  val priceCol = 0
  val timeCol = 1
  val volCol = 2
  val B32time2Col = 3
  val B32aggCol = 4
  val B132aggCol = 3
  val ops32 = List(GreaterThan, LessThan)
  val ops132 = List(LessThan)
  val emptyFn = (r: Row) => Row.empty
  val gbyFn32 = (r: Row) => Row(Array(r(timeCol)))
  val keyFn32 = (r: Row) => Array(r(timeCol), r(priceCol))
  val keyFn132Outer = (r: Row) => Array(r(timeCol))
  val keyFn132Inner = (r: Row) => Array(r(B32time2Col))
  val valueFn32 = (r: Row) => 1.0
  val valueFn132 = (r: Row) => r(B32aggCol)

  val ord32 = Helper.sorting(keyFn32, ops32)
  val ord132 = Helper.sorting(keyFn132Inner, ops132)

  def main(args: Array[String]) = {
    var exectime = collection.mutable.ListBuffer[Long]()
    var maxTimeInMS = 1000 * 60 * 5
    val allTests = List[Bids7](Bids72Naive, Bids72Range, Bids72Merge, Bids71Naive, Bids71Range, Bids71Merge)
    var testFlags = 0xFF
    var enable = true
    (10 to 13).foreach { all =>
      var logn = all
      var logr = all
      var logp = all - 5
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
        /*
        val vn = Bids72Naive.verify.toSet
        val vn1 = Bids72Naive.verify.toSet
        val vm = Bids72Merge.verify.toSet
        val vr = Bids72Range.verify.toSet

        val filterfn = (r: Row) => r(0) < 10 && r(1) < 10
        println("BASE2")
        vn.filter(filterfn).foreach(println)
        println("Naive1++")
        vn1.diff(vn).filter(filterfn).foreach(println)
        println("Naive1--")
        vn.diff(vn1).filter(filterfn).foreach(println)
        println("Merge++")
        vm.diff(vn).filter(filterfn).foreach(println)
        println("Merge--")
        vn.diff(vm).filter(filterfn).foreach(println)

        println("Range++")
        vr.diff(vn).filter(filterfn).foreach(println)
        println("Range--")
        vn.diff(vr).filter(filterfn).foreach(println)

        val ver = allTests.map(_.verify.toSet)
        assert(ver.forall(_ equals ver.head))
*/
        //val res = allTests.map(_.result.toList.sorted(ord))
        //assert(res.forall(_ equals res.head))
      }
    }
  }
}
