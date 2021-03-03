package ds

import utils.{AggPlus, Aggregator}

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

  def rangeQuery(ranges: List[(Double, Double)]): Double = {
    val (l, r) = ranges.head
    val tail = ranges.tail

    def rec(n: Node): Double = {
      if (n == null)
        return agg.zero
      //n.applyLazyUpdate
      if (r < n.keyLower || l > n.keyUpper)
        return agg.zero
      if (l <= n.keyLower && n.keyUpper <= r)
        return if (dim == 1) n.value else n.nextDim.rangeQuery(tail)
      if (r < n.leftChild.keyUpper)
        return rec(n.leftChild)
      if (l > n.rightChild.keyLower)
        return rec(n.rightChild)
      val lval = rec(n.leftChild)
      val rval = rec(n.rightChild)
      return agg(lval, rval)
    }

    rec(root)
  }

}

object RangeTree {
  type Tuple = List[Double]

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
  def buildFrom(table: List[Tuple], dim: Int, agg: Aggregator[Double], name: String): RangeTree = {
    val keys = table.groupBy(r => r.head).map(kv => kv._1 -> kv._2.map(_.tail)).toArray.sortBy(_._1)


    val rt = new RangeTree(name, agg, dim)
    rt.root = buildDim(keys)

    def buildDim(keys: Array[(Double, List[Tuple])]): Node = {
      val keySize = keys.size
      val med = keys((keySize - 1) / 2)._1

      val n = new Node
      if (keys.size == 1) {
        n.keyUpper = keys(0)._1
        n.keyLower = n.keyUpper
        if (dim == 1)
          n.value = keys(0)._2.map(_.head).reduce(agg.apply)
        else
          n.nextDim = buildFrom(keys.map(_._2).reduce(_ ++ _), dim - 1, agg, name)
      } else {
        val (left, right) = keys.partition(_._1 <= med)
        n setLeft buildDim(left)
        n setRight buildDim(right)
        if (dim == 1)
          n.value = agg(n.leftChild.value, n.rightChild.value)
        else
          n.nextDim = buildFrom(keys.map(_._2).reduce(_ ++ _), dim - 1, agg, name)
        n.keyUpper = n.rightChild.keyUpper
        n.keyLower = n.leftChild.keyLower
      }
      n
    }

    rt
  }


  def main(args: Array[String]): Unit = {
    val tuples = (1 to 10).map(k => List(k.toDouble, k.toDouble)).toList
    val rt = buildFrom(tuples, 1, AggPlus, "One")
    rt.printTree()

    val relT = List(
      List(3, 10, 10),
      List(2, 20, 15),
      List(4, 20, 12),
      List(7, 30, 34),
      List(2, 40, 9),
      List(4, 50, 7),
      List(7, 60, 5),
      List(2, 60, 34),
      List(3, 70, 8),
      List(4, 70, 55),
      List(7, 70, 1)).map(_.map(_.toDouble))

    val rt2 = buildFrom(relT, dim = 2, AggPlus, "Two")
    rt2.printTree()

    val range = List((2.0,3.0), (15.0,50.0))
    println("Result is " + rt2.rangeQuery(range))
  }
}