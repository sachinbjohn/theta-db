import ds.{Cube, Domain, Row, Table}
import org.scalatest.funsuite.AnyFunSuite
import utils.{GreaterThan, Helper, LessThanEqual}

class CubeTest extends AnyFunSuite {

  test("3D cube") {
    val d1 = Domain((10 to 14).map(_.toDouble).toArray)
    val d2 = Domain((20 to 22).map(_.toDouble).toArray)
    val d3 = Domain((30 to 36).map(_.toDouble).toArray)
    val d = Array(d1, d2, d3)

    val c = new Cube(d)
    assert(105 == c.totalSize)
    (0 to c.totalSize.toInt-1).foreach({ i =>
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
    val ops = List(LessThanEqual[Double], GreaterThan[Double])
    val keyVector = (r: Row) => Array(r(0), r(1))
    val ord = Helper.sorting(keyVector, ops)
    val tableT = new Table("T", relT.sorted(ord))

    val dom1 = Domain(relT.map(_ (0)).distinct.sorted.toArray)
    val dom2 = Domain(relT.map(_ (1)).distinct.sorted(Ordering[Double].reverse).toArray)
    val dom = Array(dom1, dom2)
    val ord2 = Helper.sortingOther(dom, keyVector, ops)

    val c3 = Cube.fromData(dom, tableT, keyVector, _ (2))
    c3.accumulate(ops)
    println()
    println(c3.mkString("\n"))


    val relS = List(
      Array(3, 30),
      Array(5, 20),
      Array(7, 35),
      Array(6, 45)
    ) map (a => Row(a.map(_.toDouble)))
    val tableS = new Table("S", relS.sorted(ord2))


    val res = c3.join(tableS, keyVector, ops.toArray).rows.toSet
    res.foreach(println)

    val ans = relS.map(s => {
      var  sum = 0.0
      relT.foreach{t =>
        val c1 = ops(0)(t(0),s(0))
        val c2 = ops(1)(t(1),s(1))
        if( c1 && c2) sum += t(2)}
      Row(s.a.:+(sum))
    }).toSet
    assert(res equals ans)

  }
}