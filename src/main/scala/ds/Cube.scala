package ds

import utils.{ComparatorOp, EqualTo, GreaterThan, GreaterThanEqual, Helper, LessThanEqual}

class BigArray(val s: Long, val data: Array[Array[Double]]) extends Iterable[Double] {

  import BigArray._

  def apply(n: Long) = {
    val a = (n / P).toInt;
    val b = (n % P).toInt;
    // n = a * P + b
    data(a)(b)
  }

  def update(n: Long, v: Double) = {
    val a = (n / P).toInt;
    val b = (n % P).toInt;
    data(a)(b) = v

  }

  override def iterator: Iterator[Double] = new BigArrayIterator(this)
}

object BigArray {
  val P = 1 << 25

  def apply(size: Long) = {
    val N = (size / P + 1).toInt
    val mod = (size % P).toInt
    val data = new Array[Array[Double]](N)
    for (i <- 0 to N - 2) {
      data(i) = new Array[Double](P)
    }
    data(N - 1) = new Array[Double](mod)
    new BigArray(size, data)
  }
}

class BigArrayIterator(val ba: BigArray) extends Iterator[Double] {

  import BigArray._

  var a = 0
  var b = 0
  val s = ba.s

  def n = (a * P.toLong + b)

  override def hasNext: Boolean = n < ba.s

  override def next(): Double = {
    val ret = ba.data(a)(b)
    b += 1
    if (b == P) {
      b = 0
      a += 1
    }
    ret
  }

  def +=(n: Long): Unit = {
    a += (n / P).toInt
    b += (n % P).toInt
  }
}

class Cube(val domains: Array[Domain]) extends Iterable[(Row, Double)] {
  val domainSizes = domains.map(_.size.toLong)
  val totalSize = domainSizes.reduce(_ * _)
  val data = BigArray(totalSize)
  val D = domains.size

  def DtoOne(dims: Array[Int]): Long = {
    dims.zip(domainSizes).foldLeft(0L)({ case (n, (a, s)) => n * s + a })
  }

  def OneToD(n: Long): Array[Int] = {
    val array = Array.fill(D)(0)
    var index = n
    for (i <- 1 to D) {
      array(D - i) = (index % domainSizes(D - i)).toInt
      index = index / domainSizes(D - i)
    }
    array
  }

  def join(t: Table, keyVector: Row => Array[Double], ops: Array[ComparatorOp[Double]]) = {
    import Helper.DoubleComparisons
    val dim = Array.fill(D)(-1)
    val newrows = t.rows.map { r =>
      val k = keyVector(r)
      var reset = false
      var isZero = false

      for (i <- 0 until D) {

        var index = if (reset) -1 else dim(i)

        def di = if (index == -1) ops(i).first else domains(i)(index)

        def disucc = if (index == domains(i).size - 1) ops(i).last else domains(i)(index + 1)

        def condition = if (ops(i).isInstanceOf[EqualTo[Double]]) {

          disucc <= k(i)
        } else {
          ops(i)(disucc, k(i))
        }

        while (condition) {
          index += 1
        }

        isZero = isZero || (if (ops(i).isInstanceOf[EqualTo[Double]])
          k(i) != di
        else
          index == -1
          )

        if (dim(i) != index) {
          reset = true
          dim(i) = index
        }
      }
      val v = if (isZero) 0.0 else apply(dim)
      Row(r.a.:+(v))
    }
    new Table("join", newrows)
  }

  def accumulate(ops: List[ComparatorOp[Double]]) = {
    var ops2 = ops
    var skip = totalSize
    for (i <- 0 to D - 1) {
      skip /= domainSizes(i)
      //println(s"\nAccumulating dim $i")
      val (o :: tail) = ops2
      if (o != EqualTo[Double]) {
        var n = skip
        while (n < totalSize) {
          if ((n / skip) % domainSizes(i) != 0) {
            data(n) = data(n - skip) + data(n)
            //println(OneToD(n).mkString("[", ",", "]") + "+=" + OneToD(n - skip).mkString("[", ",", "]"))
          }
          n += 1
        }
      }
      ops2 = tail
    }
  }


  def apply(dims: Array[Int]) = {
    val n = DtoOne(dims)
    data(n)
  }

  def update(dims: Array[Int], v: Double) = {
    val n = DtoOne(dims)
    data(n) = v
  }

  override def iterator: Iterator[(Row, Double)] = new CubeIterator(this)
}

class CubeIterator(val cube: Cube) extends Iterator[(Row, Double)] {
  val it = cube.data.iterator.asInstanceOf[BigArrayIterator]

  override def hasNext: Boolean = it.hasNext

  override def next(): (Row, Double) = {
    val n = it.n
    val dims = cube.OneToD(n)
    val v = it.next()
    val array = new Array[Double](cube.D)
    (0 until cube.D).foreach(i => array(i) = cube.domains(i)(dims(i)))
    (Row(array), v)
  }
}

object Cube {
  def fromData(domains: Array[Domain], t: Table, keyVector: Row => Array[Double], valueFn: Row => Double): Cube = {
    val cube = new Cube(domains)
    val dim = Array.fill(cube.D)(0)
    t.rows.foreach { r =>
      val k = keyVector(r)
      assert(k.size == cube.D)
      var reset = false
      for (i <- 0 until cube.D) {
        var index = if (reset) 0 else dim(i)
        while (k(i) != domains(i)(index)) { // k(i) guaranteed to be in domain
          index += 1
        }
        if (dim(i) != index) {
          reset = true
          dim(i) = index
        }
      }

      cube(dim) += valueFn(r)
    }
    cube
  }

  def main(args: Array[String]): Unit = {
    val d1 = Domain((10 to 14).map(_.toDouble).toArray)
    val d2 = Domain((20 to 22).map(_.toDouble).toArray)
    val d3 = Domain((30 to 36).map(_.toDouble).toArray)
    val d = Array(d1, d2, d3)

    val c = new Cube(d)
    println("Total size = " + c.totalSize)
    (0 to c.totalSize.toInt).foreach({ i =>
      val a = c.OneToD(i)
      val n = c.DtoOne(a)
      println(s"i = $i, a =  ${a.mkString("[", ",", "]")}, n = $n")
    })


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

    val ops = List(LessThanEqual[Double], GreaterThan[Double])
    val keyVector = (r: Row) =>Array(r(0), r(1))
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
    )map(a => Row(a.map(_.toDouble)))
    val tableS = new Table("S",relS.sorted(ord2))

    println("JOIN")
    c3.join(tableS, keyVector, ops.toArray).foreach(println)
  }
}

