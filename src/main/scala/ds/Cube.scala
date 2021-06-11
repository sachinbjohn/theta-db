package ds

import utils.{Aggregator, ComparatorOp, EqualTo, GreaterThan, GreaterThanEqual, Helper, LessThanEqual}

class BigArray(val s: Long, val array2d: Array[Array[Double]]) extends Iterable[Double] {

  import BigArray._

  def apply(n: Long) = {
    val a = (n / P).toInt;
    val b = (n % P).toInt;
    // n = a * P + b
    array2d(a)(b)
  }

  def update(n: Long, v: Double) = {
    val a = (n / P).toInt;
    val b = (n % P).toInt;
    array2d(a)(b) = v

  }

  override def iterator: Iterator[Double] = new BigArrayIterator(this)
}

object BigArray {
  val P = 1 << 25

  def apply(size: Long, zero: Double) = {
    val N = (size / P + 1).toInt
    val mod = (size % P).toInt
    val data = new Array[Array[Double]](N)
    for (i <- 0 to N - 2) {
      data(i) = Array.fill(P)(zero)
    }
    data(N - 1) = Array.fill(mod)(zero)
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
    val ret = ba.array2d(a)(b)
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

class Cube(val domains: Array[Domain], agg: Aggregator[Double]) extends Iterable[(Row, Double)] {
  val domainSizes = domains.map(_.size.toLong)
  val totalSize = domainSizes.reduce(_ * _)
  val data = BigArray(totalSize, agg.zero)
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

        def condition = if (ops(i) == EqualTo) {
          disucc <= k(i)
        } else {
          ops(i)(disucc, k(i))
        }

        while (condition) {
          index += 1
        }

        isZero = isZero || (index == -1) || (if (ops(i) == EqualTo)
          k(i) != di
        else
          false
          )

        if (dim(i) != index) {
          reset = true
          dim(i) = index
        }
      }
      val v = if (isZero) agg.zero else apply(dim)
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
      if (o != EqualTo) {
        var n = skip
        while (n < totalSize) {
          if ((n / skip) % domainSizes(i) != 0) {
            data(n) = agg(data(n - skip), data(n))
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

  def getKey(dims: Array[Int]) = {
    val array = new Array[Double](D)
    (0 until D).foreach { i =>
      array(i) = domains(i)(dims(i))
    }
    array
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
    val key = cube.getKey(dims)
    (Row(key), v)
  }
}

object Cube {
  def fromData(domains: Array[Domain], t: Table, keyVector: Row => Array[Double], valueFn: Row => Double, agg: Aggregator[Double]): Cube = {
    val cube = new Cube(domains, agg)
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

      cube(dim) = agg(cube(dim), valueFn(r))
    }
    cube
  }

}


class MultiCube(val multidom: MultiDomain, agg: Aggregator[Double], val ops: Array[ComparatorOp[Double]]) extends Iterable[(Row, Double)] {
  val domainSizes = multidom.hmarr.map(kv => kv._1 -> kv._2.map(xy => xy._1 -> xy._2.map(_.size.toLong)))
  val totalSize = domainSizes.map(kv => kv._1 -> kv._2.map(xy => xy._1 -> xy._2.reduce(_ * _)))
  val data = totalSize.map(kv => kv._1 -> kv._2.map(xy => xy._1 -> BigArray(xy._2, agg.zero)))
  val D = ops.size

  def DtoOne(gbyKey: Row, eqKey: Row)(dims: Array[Int]): Long = {
    dims.zip(domainSizes(gbyKey)(eqKey)).foldLeft(0L)({ case (n, (a, s)) => n * s + a })
  }

  def OneToD(gbyKey: Row, eqKey: Row)(n: Long): Array[Int] = {
    val array = Array.fill(D)(0)
    var index = n
    for (i <- 1 to D) {
      array(D - i) = (index % domainSizes(gbyKey)(eqKey)(D - i)).toInt
      index = index / domainSizes(gbyKey)(eqKey)(D - i)
    }
    array
  }

  def join(t: Table, eqKeyFn: Row => Row, ineqKeyFn: Row => Array[Double]) = {
    import Helper.DoubleComparisons
    val newrows =
      multidom.hmarr.flatMap { case (gbyKey, eqMap) =>
        var reset = false
        var isZero = false
        val dim = Array.fill(D)(-1)

        t.rows.map { r =>
          val ineqK = ineqKeyFn(r)
          val eqK = eqKeyFn(r)

          if (eqMap.contains(eqK)) {
            val domains = eqMap(eqK)

            for (i <- 0 until D) {

              var index = if (reset) -1 else dim(i)

              def di = if (index == -1) ops(i).first else domains(i)(index)

              def disucc = if (index == domains(i).size - 1) ops(i).last else domains(i)(index + 1)

              def condition = ops(i)(disucc, ineqK(i))

              while (condition) {
                index += 1
              }

              isZero = isZero || (index == -1)
              if (dim(i) != index) {
                reset = true
                dim(i) = index
              }
            }
          } else {
            isZero = true
          }
          val v = if (isZero) agg.zero else apply(gbyKey, eqK)(dim)
          Row(r.a ++ gbyKey.a :+ (v))
        }
      }
    new Table("join", newrows.toList)
  }

  def accumulate() = {

    data.foreach { case (gbyKey, eqMap) =>
      eqMap.foreach { case (eqKey, ineqdata) =>
        val tS = totalSize(gbyKey)(eqKey)
        val dS = (i: Int) => domainSizes(gbyKey)(eqKey)(i)
        var skip = tS
        for (i <- 0 to D - 1) {
          skip /= dS(i)
          //println(s"\nAccumulating dim $i")
          var n = skip
          while (n < tS) {
            if ((n / skip) % dS(i) != 0) {
              ineqdata(n) = agg(ineqdata(n - skip), ineqdata(n))
              //println(OneToD(gbyKey, eqKey)(n).mkString("[", ",", "]") + " : " + ineqdata(n) + " +=" + OneToD(gbyKey, eqKey)(n - skip).mkString("[", ",", "]") + " : " + ineqdata(n - skip))
            }
            n += 1
          }
        }
      }
    }
  }


  def apply(gbyKey: Row, eqKey: Row)(dims: Array[Int]) = {
    val n = DtoOne(gbyKey, eqKey)(dims)
    data(gbyKey)(eqKey)(n)
  }

  def getKey(gbyKey: Row, eqKey: Row)(dims: Array[Int]) = {
    val domains = multidom.hmarr(gbyKey)(eqKey)
    val array = new Array[Double](D)
    (0 until D).foreach { i =>
      array(i) = domains(i)(dims(i))
    }
    array
  }

  def update(gbyKey: Row, eqKey: Row, dims: Array[Int], v: Double) = {
    val n = DtoOne(gbyKey, eqKey)(dims)
    data(gbyKey)(eqKey).update(n, v)
  }

  override def iterator: Iterator[(Row, Double)] = new MultiCubeIterator(this)
}

class MultiCubeIterator(val cube: MultiCube) extends Iterator[(Row, Double)] {
  val it1 = cube.data.iterator
  var it1val = it1.next
  var gbyKey = it1val._1
  var it2 = it1val._2.iterator
  var it2val = it2.next
  var eqKey = it2val._1
  var it3 = it2val._2.iterator.asInstanceOf[BigArrayIterator]

  override def hasNext: Boolean = it1.hasNext || it2.hasNext || it3.hasNext

  override def next(): (Row, Double) = {

    if (!it3.hasNext) {
      if (!it2.hasNext) {
        it1val = it1.next()
        gbyKey = it1val._1
        it2 = it1val._2.iterator
      }
      it2val = it2.next()
      eqKey = it2val._1
      it3 = it2val._2.iterator.asInstanceOf[BigArrayIterator]
    }

    val n = it3.n
    val dims = cube.OneToD(gbyKey, eqKey)(n)
    val v = it3.next()
    val key = cube.getKey(gbyKey, eqKey)(dims)
    (Row(gbyKey.a ++ eqKey.a ++ key), v)
  }
}

object MultiCube {
  def fromData(t: Table, gbyKeyFn: Row => Row, eqKeyFn: Row => Row, ineqkeyFn: Row => Array[Double], valueFn: Row => Double, agg: Aggregator[Double], ops: Array[ComparatorOp[Double]]): MultiCube = {

    val groupedTable = t.rows.groupBy(gbyKeyFn).map(kv => kv._1 -> kv._2.groupBy(eqKeyFn))
    val multidom = MultiDomain.fromTable(groupedTable, ineqkeyFn, ops)
    val multicube = new MultiCube(multidom, agg, ops)
    groupedTable.foreach { case (gbyKey, eqMap) =>
      eqMap.foreach { case (eqKey, rs) =>
        val domains = multidom(gbyKey, eqKey)
        val dim = Array.fill(multicube.D)(0)

        rs.foreach { r =>
          val ineqK = ineqkeyFn(r)
          assert(ineqK.size == multicube.D)
          var reset = false
          for (i <- 0 until multicube.D) {
            var index = if (reset) 0 else dim(i)
            while (ineqK(i) != domains(i)(index)) { // k(i) guaranteed to be in domain
              index += 1
            }
            if (dim(i) != index) {
              reset = true
              dim(i) = index
            }
          }
          multicube(gbyKey, eqKey, dim) = agg(multicube(gbyKey, eqKey)(dim), valueFn(r))
        }
      }
    }
    multicube.accumulate()
    multicube
  }
}


