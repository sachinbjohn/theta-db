package queries

import datagen.Bids
import ds._
import exec._
import utils._
import ddbt.lib.M3Map

abstract class Bids71 extends Bids7 {
  override def query: String = "Bids71"
}

object Bids71Naive extends Bids71 {

  import Bids71._

  override def evaluate(bids: Table): Unit = {
    bids.foreach { b1 =>
      var sum1 = 0.0
      bids.foreach { b2 =>
        if (b2(timeCol) < b1(timeCol)) {
          bids.foreach { b3 =>
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
object Bids71DBT extends Bids71 {

  import queries.dbt.Bids71Base
  import Bids71._

  override def evaluate(bids: Table): Unit = {
    val obj = new Bids71Base
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

object Bids71Range extends Bids71 {

  import Bids71._

  override def evaluate(bids: Table): Unit = {

    val rt3 = MultiRangeTree.buildFrom(bids, emptyFn, emptyFn, keyFn23, valueFn23, AggPlus, ops23.toArray)
    val b23 = rt3.join(bids, emptyFn, keyFn23)

    val rt23 = MultiRangeTree.buildFrom(b23, emptyFn, emptyFn, keyFn123, valueFn123, AggPlus, ops123.toArray)
    val b123 = rt23.join(bids, emptyFn, keyFn123)
    verify ++= b123.rows
    result ++= b123.rows
  }

  override def algo: Algorithm = Inner
}

object Bids71Merge extends Bids71 {

  import Bids71._

  override def evaluate(bids: Table): Unit = {
    val sortedBids = new Table("sortedBids", bids.rows.sorted(ord23))
    val cube3 = MultiCube.fromData(sortedBids, emptyFn, emptyFn, keyFn23, valueFn23, AggPlus, ops23.toArray)
    val b23 = cube3.join(sortedBids, emptyFn, keyFn23)

    val sorted23 = new Table("sorted23", b23.rows.sorted(ord123)) //Not required?
    val cube23 = MultiCube.fromData(sorted23, emptyFn, emptyFn, keyFn123, valueFn123, AggPlus, ops123.toArray)
    val b123 = cube23.join(sortedBids, emptyFn, keyFn123)
    verify ++= b123.rows
    result ++= b123.rows

  }

  override def algo: Algorithm = Merge
}

object Bids71 {
  val priceCol = 0
  val timeCol = 1
  val volCol = 2
  val B23aggCol = 3
  val B123aggCol = 3
  val ops23 = List(LessThan, GreaterThan)
  val ops123 = List(LessThan)
  val emptyFn = (r: Row) => Row.empty
  val keyFn23 = (r: Row) => Array(r(timeCol), r(priceCol))
  val keyFn123 = (r: Row) => Array(r(timeCol))
  val valueFn23 = (r: Row) => 1.0
  val valueFn123 = (r: Row) => r(B23aggCol)

  val ord23 = Helper.sorting(keyFn23, ops23)
  val ord123 = Helper.sorting(keyFn123, ops123)

  def main(args: Array[String]) = {
    var exectime = collection.mutable.ListBuffer[Long]()
    var maxTimeInMS = 1000 * 60 * 5
    val allTests = List[Bids71](Bids71Naive, Bids71Range, Bids71Merge)
    var testFlags = 0xFF
    var enable = true
    (10 to 10).foreach { all =>
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
       /* val vn = Bids71Naive.verify.toSet
        val vm = Bids71Merge.verify.toSet
        val vr = Bids71Range.verify.toSet

        val filterfn = (r: Row) => r(0) < 10 && r(1) < 10
        println("BASE")
        vn.filter(filterfn).foreach(println)
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

        //val res = allTests.map(_.result.toList.sorted(ord))
        //assert(res.forall(_ equals res.head))*/
      }
    }
  }
}
