import ds.{Cube, Domain, Row, Table}
import org.scalatest.funsuite.AnyFunSuite
import utils._

class CubeTest extends AnyFunSuite {

  test("3D cube") {
    val d1 = Domain((10 to 14).map(_.toDouble).toArray)
    val d2 = Domain((20 to 22).map(_.toDouble).toArray)
    val d3 = Domain((30 to 36).map(_.toDouble).toArray)
    val d = Array(d1, d2, d3)
    val c = new Cube(d, AggPlus)
    assert(105 === c.totalSize)
    (0 to c.totalSize.toInt - 1).foreach({ i =>
      val a = c.OneToD(i)
      val n = c.DtoOne(a)
      //println(s"i = $i, a =  ${a.mkString("[", ",", "]")}, n = $n")
      assert(i == n)
    })
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
  test("Cube join ") {
    val ops = List(LessThanEqual, GreaterThan)
    val keyVector = (r: Row) => Array(r(0), r(1))
    val ord = Helper.sorting(keyVector, ops)
    val tableT = new Table("T", relT.sorted(ord))

    val dom1 = Domain(relT.map(_ (0)).distinct.sorted.toArray)
    val dom2 = Domain(relT.map(_ (1)).distinct.sorted(Ordering[Double].reverse).toArray, false)
    dom1.sameAsOuter = false
    dom2.sameAsOuter = false
    val dom = Array(dom1, dom2)
    val ord2 = Helper.sortingOther(dom, keyVector, ops)

    val c3 = Cube.fromData(dom, tableT, keyVector, _ (2), AggPlus)
    c3.accumulate(ops)
    //println()
    //println(c3.mkString("\n"))


    val relS = List(
      Array(3, 30),
      Array(5, 20),
      Array(7, 35),
      Array(6, 45)
    ) map (a => Row(a.map(_.toDouble)))
    val tableS = new Table("S", relS.sorted(ord2))


    val res = c3.join(tableS, keyVector, ops.toArray).rows.toSet
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

  test("Cube 2D join with different domains") {
    val ops = List(LessThan, LessThan)
    val keyFn = (r: Row) => Array(r(0), r(1))
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

    val cube = Cube.fromData(doms, S, keyFn, valueFn, AggPlus)
    cube.accumulate(ops)

    val join = cube.join(R, keyFn, ops.toArray).rows
    println()
    join.foreach(println)
    assert(naiveRes == join)
  }
}
