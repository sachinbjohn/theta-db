package ds

import utils.{AggPlus, Aggregator, ComparatorOp, EqualTo, GreaterThan, GreaterThanEqual, LessThan, LessThanEqual}

class Node() {
  var keyUpper = 0.0
  var keyLower = 0.0
  var value = 0.0

  var numDuplicateKey = 1
  var numKeysInRange = 1

  var leftChild: Node = null
  var rightChild: Node = null
  var parent: Node = null
  var prevDim: RangeTree = null
  var nextDim: RangeTree = null
  var isRed: Boolean = true

  override def toString: String = {
    s"  [$keyLower:$keyUpper] = ${value}  @ $hashCode"
  }

  def fixValues = ()

  def leftRotate(): Node = {
    val x = this
    //x.applyLazyUpdate
    val xr = x.rightChild
    //xr.applyLazyUpdate
    val xrl = xr.leftChild
    x.setRight(xrl)
    xr.setLeft(x)
    xr
  }

  def rightRotate(): Node = {
    val x = this
    //x.applyLazyUpdate
    val xl = x.leftChild
    //assert(xl.minDepth >= 2)
    //xl.applyLazyUpdate
    val xlr = xl.rightChild
    x.setLeft(xlr)
    xl.setRight(x)
    xl
  }

  def setLeft(n: Node): Unit = {
    if (n != null) {
      n.parent = this
    }
    leftChild = n
    fixValues
  }

  def setRight(n: Node): Unit = {
    if (n != null) {
      n.parent = this
    }
    rightChild = n
  }

  def successor[U](root: Node) = {
    var x = this
    while (x != root && x != x.parent.leftChild)
      x = x.parent
    if (x == root)
      None
    else {
      x = x.parent.rightChild
      //Some(x.keyLower)
      while (x.leftChild != null) {
        //x.applyLazyUpdate
        x = x.leftChild
      }
      //x.applyLazyUpdate
      Some(x)

    }
  }

  def predecessor[U](root: Node) = {
    var x = this
    while (x != root && x != x.parent.rightChild)
      x = x.parent
    if (x == root)
      None
    else {
      x = x.parent.leftChild
      //Some(x.keyUpper)
      while (x.rightChild != null) {
        //x.applyLazyUpdate
        x = x.rightChild
      }
      //x.applyLazyUpdate
      Some(x)
    }
  }

  def replaceBy(n: Node) = {
    //No change to parent
    //n.applyLazyUpdate
    keyUpper = n.keyUpper
    keyLower = n.keyLower

    value = n.value

    setLeft(n.leftChild)
    setLeft(n.rightChild)


    numKeysInRange = n.numKeysInRange
    numDuplicateKey = n.numDuplicateKey
    isRed = n.isRed
  }

  def printTree(depth: Int): String = {
    var res = ""
    res = res + ("     |" * depth)
    if (depth != 0)
      res = res + ("---")
    res = res + (toString) + "\n"
    if (leftChild != null) {
      res = res + leftChild.printTree(depth + 1)
      res = res + rightChild.printTree(depth + 1)
    }
    res
  }

}


class RangeTree(val name: String, val agg: Aggregator[Double], val dim: Int) {

  var root: Node = null

  def printTree() {
    println(s"DIM = $dim")
    println(root.printTree(0))

    def rec(n: Node): Unit = {
      println(s"Subtree at node $n")
      n.nextDim.printTree()
      if (n.leftChild != null) {
        rec(n.leftChild)
        rec(n.rightChild)
      }
    }

    if (dim > 1)
      rec(root)
  }

  def transplant(u: Node, v: Node): Unit = {
    if (u.parent == null) {
      root = v
      v.parent = null
    } else {
      if (u == u.parent.leftChild) {
        u.parent.setLeft(v)
      } else {
        u.parent.setRight(v)
      }
    }
  }

  def join(t: Table, keyVector: Array[Int], ops: Array[ComparatorOp[Double]]) = {
    def genRange(op: ComparatorOp[Double]) = (x: Double) => op match {
      case l: LessThan[Double] => (Double.NegativeInfinity, x, false, false)
      case leq: LessThanEqual[Double] => (Double.NegativeInfinity, x, false, true)
      case eq: EqualTo[Double] => (x, x, true, true)
      case geq: GreaterThanEqual[Double] => (x, Double.PositiveInfinity, true, false)
      case g: GreaterThan[Double] => (x, Double.PositiveInfinity, false, false)
    }

    val rangeFn = ops.map(genRange(_))
    val newRows = t.rows.map { r =>
      val k = keyVector.map(i => r(i))
      val ranges = k.zip(rangeFn).map {case (k, f) => f(k)}.toList
      val v = rangeQuery(ranges)
      Row(r.a.:+(v))
    }
    new Table("join", newRows)
  }

  //TODO: return key that joins as well
  def rangeQuery(ranges: List[(Double, Double, Boolean, Boolean)]): Double = {
    val (l, r, lc, rc) = ranges.head
    val tail = ranges.tail

    def rec(n: Node): Double = {
      if (n == null)
        return agg.zero
      //n.applyLazyUpdate

      val c11 = if (lc) l > n.keyUpper else l >= n.keyUpper
      val c12 = if (rc) r < n.keyLower else r <= n.keyLower
      val c1 = c11 || c12

      if (c1)
        return agg.zero

      val c21 = if (lc) l <= n.keyLower else l < n.keyLower
      val c22 = if (rc) n.keyUpper <= r else n.keyUpper < r
      val c2 = c21 && c22

      if (c2)
        return if (dim == 1) n.value else n.nextDim.rangeQuery(tail)

      val c3 = if (rc) r < n.rightChild.keyLower else r <= n.rightChild.keyLower
      if (c3)
        return rec(n.leftChild)

      val c4 = if (lc) l > n.leftChild.keyUpper else l >= n.leftChild.keyUpper
      if (c4)
        return rec(n.rightChild)

      val lval = rec(n.leftChild)
      val rval = rec(n.rightChild)
      return agg(lval, rval)
    }

    rec(root)
  }

}

object RangeTree {

  /*
  def makeTree(l: Iterable[Node]) = {
    var nodes = l
    while (nodes.size > 1) {
      val nodes2 = nodes.foldLeft[(Option[Node], List[Node])]((None, Nil)) {
        case ((None, par), cur) => (Some(cur), par)
        case ((Some(n), par), cur) => {
          val n2 = new Node()
          n2.keyLower = n.keyLower
          n2.keyUpper = cur.keyUpper
          n2.value = agg(n.value, cur.value)
          n2.setLeft(n)
          n2.setRight(cur)
          (None, par ++ List(n2))
        }
      }
      val extra = nodes2._1.toList
      nodes = nodes2._2 ++ extra
      //nodes.foreach(n3 => println(n3.printTree(0)))
    }
    //println(nodes.head.printTree(0))
    nodes.head
  }*/

  //tuple = list(k1,k2,...,kd,v)
  def buildFrom(table: Table, keyVector: Array[Int], valueFn: Row => Double, agg: Aggregator[Double], name: String) = buildLayer(table.rows, 0, keyVector, valueFn, agg, name)

  def buildLayer(rows: Seq[Row], currentDim: Int, keyVector: Array[Int], valueFn: Row => Double, agg: Aggregator[Double], name: String): RangeTree = {
    val keys = rows.groupBy(kv => kv(keyVector(currentDim))).toArray.sortBy(_._1)
    val totalDim = keyVector.size
    val rt = new RangeTree(name + "dim" + currentDim, agg, totalDim - currentDim)
    rt.root = buildDim(keys)

    def buildDim(keys: Array[(Double, Seq[Row])]): Node = {
      val keySize = keys.size
      val med = keys((keySize - 1) / 2)._1

      val n = new Node
      if (keys.size == 1) {
        n.keyUpper = keys(0)._1
        n.keyLower = n.keyUpper
        if (currentDim == totalDim - 1)
          n.value = keys(0)._2.map(valueFn).reduce(agg.apply)
        else
          n.nextDim = buildLayer(keys(0)._2, currentDim + 1, keyVector, valueFn, agg, name)
      } else {
        val (left, right) = keys.partition(_._1 <= med)
        n setLeft buildDim(left)
        n setRight buildDim(right)
        if (currentDim == totalDim - 1)
          n.value = agg(n.leftChild.value, n.rightChild.value)
        else
          n.nextDim = buildLayer(keys.map(_._2).reduce(_ ++ _), currentDim + 1, keyVector, valueFn, agg, name)
        n.keyUpper = n.rightChild.keyUpper
        n.keyLower = n.leftChild.keyLower
      }
      n
    }

    rt
  }


  def main(args: Array[String]): Unit = {
    val tuples = (1 to 10).map(k => Row(Array(k.toDouble, k.toDouble)))
    val table = new Table("test", tuples)
    val rt = buildFrom(table, Array(0), _ (1), AggPlus, "One")
    rt.printTree()

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

    val tableT =  new Table("T", relT)
    val rt2 = buildFrom(tableT, Array(0, 1), _ (2), AggPlus, "Two")
    rt2.printTree()

    val range = List((2.0, 3.0, true, true), (15.0, 50.0, false, true))
    println("Result is " + rt2.rangeQuery(range))

    val relS = List(
      Array(3, 30),
      Array(5, 20),
      Array(7, 35),
      Array(6, 45)
    )map(a => Row(a.map(_.toDouble)))
    val tableS = new Table("S",relS)

    println("JOIN")
    val ops = List(LessThanEqual[Double], GreaterThan[Double])
    rt2.join(tableS, Array(0,1), ops.toArray).foreach(println)
  }
}