import ds.{Domain, MultiCube, Row, Table}
import org.scalatest.funsuite.AnyFunSuite
import utils._

class MultiCubeTest extends AnyFunSuite {


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

  test("Empty Rows must match ") {
    val gbyFn = (r: Row) => Row.empty
    val r1 = Row(Array(1, 2, 3))
    val r2 = Row(Array(4, 5, 6))
    assert(gbyFn(r1) equals gbyFn(r2))
  }

  test("Group BY test") {
    val h1 = Row.empty.hashCode()
    val h2 = Row.empty.hashCode()
    assert(h1 == h2)
    val gbyFn = (r: Row) => Row.empty
    assert(relT.groupBy(gbyFn)(Row.empty).size == relT.size)
  }


  test("Multidomain test") {
    val ops = List(LessThanEqual, GreaterThan)
    val keyVector = (r: Row) => Array(r(0), r(1))
    val gbyFn = (r: Row) => Row.empty
    val eqFn = (r: Row) => Row.empty
    val ord = Helper.sorting(keyVector, ops)
    val tableT = new Table("T", relT.sorted(ord))

    val cube = MultiCube.fromData(tableT, gbyFn, eqFn, keyVector, _ => 1.0, AggPlus, ops.toArray)
    val domains = cube.multidom(Row.empty, Row.empty)

    assert(domains(0).arr.toList === List(2, 3, 4, 7))
    assert(domains(1).arr.toList === List(70, 60, 50, 40, 30, 20, 10))
  }

  val relS = List(
    Array(3, 30),
    Array(5, 20),
    Array(7, 35),
    Array(6, 45)
  ) map (a => Row(a.map(_.toDouble)))

  test("MultiCube join ") {
    val ops = List(LessThanEqual, GreaterThan)
    val keyVector = (r: Row) => Array(r(0), r(1))
    val gbyFn = (r: Row) => Row.empty
    val eqFn = (r: Row) => Row.empty
    val ord = Helper.sorting(keyVector, ops)
    val tableT = new Table("T", relT.sorted(ord))

    val dom1 = Domain(relT.map(_ (0)).distinct.sorted.toArray)
    val dom2 = Domain(relT.map(_ (1)).distinct.sorted(Ordering[Double].reverse).toArray, false)
    val dom = Array(dom1, dom2)
    dom1.sameAsOuter = false
    dom2.sameAsOuter = false
    val ord2 = Helper.sortingOther(dom, keyVector, ops)

    val c3 = MultiCube.fromData(tableT, gbyFn, eqFn, keyVector, _ (2), AggPlus, ops.toArray)

    //println()
    //println(c3.mkString("\n"))


    val tableS = new Table("S", relS.sorted(ord2))


    val res = c3.join(tableS, eqFn, keyVector).rows.toSet
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
  test("Multicube groupBy") {
    val ops = List(GreaterThan)
    val keyVector = (r: Row) => Array(r(1))
    val gbyFn = (r: Row) => Row(Array(r(0)))
    val eqFn = (r: Row) => Row.empty
    val ord = Helper.sorting(keyVector, ops)
    val tableT = new Table("T", relT.sorted(ord))


    val dom2 = Domain(relT.map(_ (1)).distinct.sorted(Ordering[Double].reverse).toArray, false)
    val dom = Array(dom2)
    dom2.sameAsOuter = false
    val ord2 = Helper.sortingOther(dom, keyVector, ops)

    val c3 = MultiCube.fromData(tableT, gbyFn, eqFn, keyVector, _ (2), AggPlus, ops.toArray)

    //println()
    //println(c3.mkString("\n"))

    val res = c3.toSet
    val groupedTable = relT.groupBy(gbyFn)
    val ans = groupedTable.flatMap { case (gby, rs) =>
      rs.map { r1 =>
        var sum = 0.0
        rs.foreach { r2 =>
          if (r2(1) >= r1(1))
            sum += r2(2)
        }
        Row(gby.a :+ r1(1)) -> sum
      }
    }.toSet
    assert(res === ans)
  }

  test("Multicube equality") {
    val ops = List(GreaterThan)
    val keyVector = (r: Row) => Array(r(1))
    val gbyFn = (r: Row) => Row.empty
    val eqFn = (r: Row) => Row(Array(r(0)))
    val ord = Helper.sorting(keyVector, ops)
    val tableT = new Table("T", relT.sorted(ord))


    val dom2 = Domain(relT.map(_ (1)).distinct.sorted(Ordering[Double].reverse).toArray, false)
    val dom = Array(dom2)
    dom2.sameAsOuter = false
    val ord2 = Helper.sortingOther(dom, keyVector, ops)

    val c3 = MultiCube.fromData(tableT, gbyFn, eqFn, keyVector, _ (2), AggPlus, ops.toArray)

    //println()
    //println(c3.mkString("\n"))

    val res = c3.toSet

    val ans =
      relT.map { r1 =>
        var sum = 0.0
        relT.foreach { r2 =>
          if (r1(0) == r2(0) && r2(1) >= r1(1))
            sum += r2(2)
        }
        Row(Array(r1(0), r1(1))) -> sum
      }.toSet
    assert(res === ans)
  }

  test("Multicube join groupBy") {
    val ops = List(GreaterThan)
    val keyVector = (r: Row) => Array(r(1))
    val gbyFn = (r: Row) => Row(Array(r(0)))
    val eqFn = (r: Row) => Row.empty
    val ord = Helper.sorting(keyVector, ops)
    val tableT = new Table("T", relT.sorted(ord))


    val dom2 = Domain(relT.map(_ (1)).distinct.sorted(Ordering[Double].reverse).toArray, false)
    val dom = Array(dom2)
    dom2.sameAsOuter = false
    val ord2 = Helper.sortingOther(dom, keyVector, ops)

    val c3 = MultiCube.fromData(tableT, gbyFn, eqFn, keyVector, _ (2), AggPlus, ops.toArray)

    //println()
    //println(c3.mkString("\n"))


    val tableS = new Table("S", relS.sorted(ord2))


    val res = c3.join(tableS, eqFn, keyVector).rows.toSet
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


    val dom2 = Domain(relT.map(_ (1)).distinct.sorted(Ordering[Double].reverse).toArray, false)
    val dom = Array(dom2)
    dom2.sameAsOuter = false
    val ord2 = Helper.sortingOther(dom, keyVector, ops)

    val c3 = MultiCube.fromData(tableT, gbyFn, eqFn, keyVector, _ (2), AggPlus, ops.toArray)

    //println()
    //println(c3.mkString("\n"))


    val tableS = new Table("S", relS.sorted(ord2))


    val res = c3.join(tableS, eqFn, keyVector).rows.toSet
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


  test("MultiCube 2D join with different domains") {
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

    val d1 = Domain(inner.map(_ (0)).distinct.toArray.sorted)
    val d2 = Domain(inner.map(_ (1)).distinct.toArray.sorted)
    val doms = Array(d1, d2)

    d1.sameAsOuter = false
    d2.sameAsOuter = false
    implicit val ord = Helper.sortingOther(doms, keyFn, ops)

    val R = new Table("R", outer.sorted)
    val S = new Table("S", inner.sorted)

    val naiveRes = outer.map { r1 =>
      var sum = 0.0
      inner.foreach { r2 =>
        if (r2(0) < r1(0) && r2(1) < r1(1))
          sum += valueFn(r2)
      }
      Row(r1.a.:+(sum))
    }

    val cube = MultiCube.fromData(S, gbyFn, eqFn, keyFn, valueFn, AggPlus, ops.toArray)


    val join = cube.join(R, eqFn, keyFn).rows
    //println()
    //join.foreach(println)
    assert(naiveRes == join)
  }
}
