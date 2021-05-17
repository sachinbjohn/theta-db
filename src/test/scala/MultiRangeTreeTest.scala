import ds.{MultiRangeTree, Row, Table}
import org.scalatest.funsuite.AnyFunSuite
import utils._

class MultiRangeTreeTest extends AnyFunSuite {


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


  val relS = List(
    Array(3, 30),
    Array(5, 20),
    Array(7, 35),
    Array(6, 45)
  ) map (a => Row(a.map(_.toDouble)))

  test("MultiRange join ") {
    val ops = List(LessThanEqual, GreaterThan)
    val keyVector = (r: Row) => Array(r(0), r(1))
    val gbyFn = (r: Row) => Row.empty
    val eqFn = (r: Row) => Row.empty
    val ord = Helper.sorting(keyVector, ops)
    val tableT = new Table("T", relT.sorted(ord))



    val rt = MultiRangeTree.buildFrom(tableT, gbyFn, eqFn, keyVector, _ (2), AggPlus, ops.toArray)

    //println()
    //println(c3.mkString("\n"))


    val tableS = new Table("S", relS)


    val res = rt.join(tableS, eqFn, keyVector).rows.toSet
    //res.foreach(println)

    val ans = relS.map(s => {
      var sum = 0.0
      relT.foreach { t =>
        val c1 = ops(0)(t(0), s(0))
        val c2 = ops(1)(t(1), s(1))
        if (c1 && c2) sum += t(2)
      }
      Row(s.a.:+(sum))
    }).toSet
    assert(res == ans)

  }

  test("Multicube join groupBy") {
    val ops = List(GreaterThan)
    val keyVector = (r: Row) => Array(r(1))
    val gbyFn = (r: Row) => Row(Array(r(0)))
    val eqFn = (r: Row) => Row.empty
    val ord = Helper.sorting(keyVector, ops)
    val tableT = new Table("T", relT.sorted(ord))

    val rt = MultiRangeTree.buildFrom(tableT, gbyFn, eqFn, keyVector, _ (2), AggPlus, ops.toArray)

    val tableS = new Table("S", relS)

    val res = rt.join(tableS, eqFn, keyVector).rows.toSet
    //res.foreach(println)

    val ans = relS.flatMap(s => {
      relT.groupBy(gbyFn).map { case (gby, rs) =>
        var sum = 0.0
        rs.foreach { t =>
          val c2 = ops(0)(t(1), s(1))
          if (c2) sum += t(2)
        }
        Row(s.a ++ gby.a :+ sum)
      }
    }).toSet
    assert(res == ans)
  }

  test("Multicube join equality") {
    val ops = List(GreaterThan)
    val keyVector = (r: Row) => Array(r(1))
    val gbyFn = (r: Row) => Row.empty
    val eqFn = (r: Row) => Row(Array(r(0)))
    val ord = Helper.sorting(keyVector, ops)
    val tableT = new Table("T", relT.sorted(ord))

    val rt = MultiRangeTree.buildFrom(tableT, gbyFn, eqFn, keyVector, _ (2), AggPlus, ops.toArray)

    //println()
    //println(c3.mkString("\n"))
    val tableS = new Table("S", relS)

    val res = rt.join(tableS, eqFn, keyVector).rows.toSet
    //res.foreach(println)

    val ans = relS.map(s => {
      var sum = 0.0
      relT.foreach { t =>
        val c1 = t(0) == s(0)
        val c2 = ops(0)(t(1), s(1))
        if (c1 && c2) sum += t(2)
      }
      Row(s.a :+ sum)
    }).toSet
    assert(res == ans)
  }


  test("MultiRangeTree 2D join with different domains") {
    val ops = List(LessThan, LessThan)
    val keyFn = (r: Row) => Array(r(0), r(1))
    val gbyFn = (r: Row) => Row.empty
    val eqFn = (r: Row) => Row.empty

    val valueFn = (r: Row) => r(0) * 100 + r(1)

    val outer = List(
      Array(1, 1),
      Array(1, 3),
      Array(1, 5),
      Array(3, 1),
      Array(3, 3),
      Array(3, 5),
      Array(5, 1),
      Array(5, 3),
      Array(5, 5)
    ).map(r => Row(r.map(_.toDouble)))

    val inner = List(
      Array(2, 2),
      Array(4, 2),
      Array(4, 4),
      Array(6, 2),
      Array(6, 4),
      Array(6, 6)
    ).map(r => Row(r.map(_.toDouble)))

    val R = new Table("R", outer)
    val S = new Table("S", inner)

    val naiveRes = outer.map { r1 =>
      var sum = 0.0
      inner.foreach { r2 =>
        if (r2(0) < r1(0) && r2(1) < r1(1))
          sum += valueFn(r2)
      }
      Row(r1.a.:+(sum))
    }
    val rt = MultiRangeTree.buildFrom(S, gbyFn, eqFn, keyFn, valueFn, AggPlus, ops.toArray)
    val join = rt.join(R, eqFn, keyFn).rows
    //println()
    //join.foreach(println)
    assert(naiveRes == join)
  }
}
