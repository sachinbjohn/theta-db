import ds.RangeTree.buildFrom
import ds.{Row, Table}
import org.scalatest.GivenWhenThen
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.funsuite.AnyFunSuite
import utils.{AggPlus, GreaterThan, LessThanEqual}

class RangeTreeTest extends AnyFunSuite {

  test("Range tree from 1 to 10") {

    val tuples = (1 to 10).map(k => Row(Array(k.toDouble, k.toDouble)))
    val table = new Table("test", tuples)
    val rt = buildFrom(table, (r: Row) => Array(r(0)), 1, _ (1), AggPlus, "One")
    //rt.printTree()
    assert(rt.root.value == 55)
  }

  val relT = List(
    Array(3, 10, 10),
    Array(2, 20, 15),
    Array(4, 20, 12),
    Array(7, 30, 34),
    Array(2, 40, 9),
    Array(4, 50, 7),
    Array(7, 60, 5),
    Array(2, 60, 34),
    Array(3, 70, 8),
    Array(4, 70, 55),
    Array(7, 70, 1)).map(a => Row(a.map(_.toDouble)))
    println("TEST 2")
  val tableT = new Table("T", relT)
  val rt2 = buildFrom(tableT, (r: Row) => Array(r(0), r(1)), 2, _ (2), AggPlus, "Two")

  test("Rel T Range Query") {
    val range = List((2.0, 3.0, true, true), (15.0, 50.0, false, true))
    assert(24 == rt2.rangeQuery(range))

  }

  test("RangeTree Join") {
    val relS = List(
      Array(3, 30),
      Array(5, 20),
      Array(7, 35),
      Array(6, 45)
    ) map (a => Row(a.map(_.toDouble)))
    val tableS = new Table("S", relS)

    val ops = List(LessThanEqual[Double], GreaterThan[Double])
    val res = rt2.join(tableS, (r: Row) => Array(r(0), r(1)), ops.toArray).rows.toList
    rt2.printTree()
    val ans = relS.map(s => {
      var  sum = 0.0
      relT.foreach{t =>
        val c1 = ops(0)(t(0),s(0))
        val c2 = ops(1)(t(1),s(1))
        if( c1 && c2) sum += t(2)}
      Row(s.a.:+(sum))
    })

    assert(res equals ans)
  }


}
