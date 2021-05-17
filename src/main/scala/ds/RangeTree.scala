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

class MultiRangeTree(val rts: Map[Row, Map[Row, RangeTree]], val ops: Array[ComparatorOp[Double]]) {

  def join(t: Table, outereqKeyFn: Row => Row, outerineqKey: Row => Array[Double]) = {
    def genRange(op: ComparatorOp[Double]) = (x: Double) => op match {
      case LessThan => (Double.NegativeInfinity, x, false, false)
      case LessThanEqual => (Double.NegativeInfinity, x, false, true)
      case EqualTo => (x, x, true, true)
      case GreaterThanEqual => (x, Double.PositiveInfinity, true, false)
      case GreaterThan => (x, Double.PositiveInfinity, false, false)
    }

    val rangeFn = ops.map(genRange(_))
    val newRows = t.rows.flatMap { r =>
      val ineqK = outerineqKey(r)
      val eqK = outereqKeyFn(r)
      val ranges = ineqK.zip(rangeFn).map { case (k, f) => f(k) }.toList
      rts.map { case (gbyKey, eqMap) =>
        val v = eqMap.get(eqK).map(_.rangeQuery(ranges)).getOrElse(0.0)
        Row(r.a ++ gbyKey.a :+ (v))
      }
    }
    new Table("join", newRows)
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

  def join(t: Table, keyVector: Row => Array[Double], ops: Array[ComparatorOp[Double]]) = {
    def genRange(op: ComparatorOp[Double]) = (x: Double) => op match {
      case LessThan => (Double.NegativeInfinity, x, false, false)
      case LessThanEqual => (Double.NegativeInfinity, x, false, true)
      case EqualTo => (x, x, true, true)
      case GreaterThanEqual => (x, Double.PositiveInfinity, true, false)
      case GreaterThan => (x, Double.PositiveInfinity, false, false)
    }

    val rangeFn = ops.map(genRange(_))
    val newRows = t.rows.map { r =>
      val k = keyVector(r)
      val ranges = k.zip(rangeFn).map { case (k, f) => f(k) }.toList
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
  def buildFrom(table: Table, keyVector: Row => Array[Double], totalDim: Int, valueFn: Row => Double, agg: Aggregator[Double], name: String) = {
    val kvrows = table.rows.map { r =>
      val k = keyVector(r)
      val v = valueFn(r)
      (k, v)
    }.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2).reduce(agg.apply)).toArray
    buildLayer(kvrows, 0, totalDim, agg, name)
  }

  def buildLayer(rows: Seq[(Array[Double], Double)], currentDim: Int, totalDim: Int, agg: Aggregator[Double], name: String): RangeTree = {
    val keys = rows.groupBy(kv => kv._1(currentDim)).toArray.sortBy(_._1)
    val rt = new RangeTree(name + "dim" + currentDim, agg, totalDim - currentDim)
    rt.root = buildDim(keys)

    def buildDim(keys: Array[(Double, Seq[(Array[Double], Double)])]): Node = {
      val keySize = keys.size
      val med = keys((keySize - 1) / 2)._1

      val n = new Node
      if (keys.size == 1) {
        n.keyUpper = keys(0)._1
        n.keyLower = n.keyUpper
        if (currentDim == totalDim - 1) {
          n.value = keys(0)._2.map(_._2).reduce(agg.apply)
        } else
          n.nextDim = buildLayer(keys(0)._2, currentDim + 1, totalDim, agg, name)
      } else {
        val (left, right) = keys.partition(_._1 <= med)
        n setLeft buildDim(left)
        n setRight buildDim(right)
        if (currentDim == totalDim - 1)
          n.value = agg(n.leftChild.value, n.rightChild.value)
        else
          n.nextDim = buildLayer(keys.map(_._2).reduce(_ ++ _), currentDim + 1, totalDim, agg, name)
        n.keyUpper = n.rightChild.keyUpper
        n.keyLower = n.leftChild.keyLower
      }
      n
    }

    rt
  }

}

object MultiRangeTree {
  def buildFrom(table: Table, gbyKeyFn: Row => Row, eqKeyFn: Row => Row, ineqkeyFn: Row => Array[Double], valueFn: Row => Double, agg: Aggregator[Double], ops: Array[ComparatorOp[Double]]) = {
     val groupedTable = table.rows.groupBy(gbyKeyFn).map(kv => kv._1 -> kv._2.groupBy(eqKeyFn).map{xy =>
       val kvrows = xy._2.map { r =>
         val k = ineqkeyFn(r)
         val v = valueFn(r)
         (k, v)
       }.groupBy(_._1).map(kv => kv._1 -> kv._2.map(_._2).reduce(agg.apply)).toArray
       xy._1 -> RangeTree.buildLayer(kvrows, 0, ops.size, agg, "RT")
     })
     new MultiRangeTree(groupedTable, ops)
  }
}