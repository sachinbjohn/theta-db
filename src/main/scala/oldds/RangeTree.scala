/*
package oldds
import utils._
trait PendingOperation[T] {}

case class LazySet[T](v: T) extends PendingOperation[T]

case class LazyAdd[T](v: T) extends PendingOperation[T]

case class LazyAdd2D[V](l2: Double, r2: Double, v: V) extends PendingOperation[RangeTreeLayer[V]]

case class LazyInsert2D[V](x2: Double, v: V) extends PendingOperation[RangeTreeLayer[V]]

trait Value

class ValueNode[T](private var v: T)(valueAgg: Aggregator[T], rangeAgg: Aggregator[T]) {
  var pendingOperation: Option[PendingOperation[T]] = None
  var keyNode: LowerNode[T] = null

  def emptyclone: ValueNode[T] = {
    new ValueNode[T](valueAgg.zero)(valueAgg, rangeAgg)
  }

  def deepclone: ValueNode[T] = {
    assert(pendingOperation == None)
    val c = new ValueNode[T](v)(valueAgg, rangeAgg)
    c
  }


  def addValue(v2: T) = v = valueAgg(v, v2)

  def removeValue(v2: T): T = {
    val (nv, rv) = valueAgg.minus(v, v2)
    v = nv
    rv
  }

  def rangeAggregate(vl: ValueNode[T], vr: ValueNode[T]): Unit = {
    v = rangeAgg(vl.getValue, vr.getValue)
  }

  def addPendingOperation(p: PendingOperation[T]): Unit = {
    val newp = if (pendingOperation.isDefined) {
      p match {
        case LazySet(v2) => p
        case LazyAdd(v2) => pendingOperation.get match {
          case LazySet(v3) => LazySet(valueAgg(v3, v2))
          case LazyAdd(v3) => LazyAdd(valueAgg(v3, v2))
        }
      }
    } else
      p
    pendingOperation = Some(newp)
  }

  def applyPendingOperation = {
    pendingOperation match {
      case None => ()
      case Some(LazySet(v2)) => v = rangeAgg.applyN(v2, keyNode.numKeysInRange)
      case Some(LazyAdd(v2)) => v = valueAgg(v, rangeAgg.applyN(v2, keyNode.numKeysInRange))
    }
    pendingOperation = None
  }

  def getValue = {
    pendingOperation match {
      case None => v
      case Some(LazySet(v2)) => rangeAgg.applyN(v2, keyNode.numKeysInRange)
      case Some(LazyAdd(v2)) => valueAgg(v, rangeAgg.applyN(v2, keyNode.numKeysInRange))
    }
  }
}

abstract class Node[T](k: Double) {
  var keyLower = k
  var keyUpper = k
  var leftChild: Node[T] = null
  var rightChild: Node[T] = null
  var parent: Node[T] = null

  var numKeysInRange = 1
  var numDuplicateKey = 1

  var maxDepth = 1
  var minDepth = 1
  var color = 0


  def deepclone: Node[T]

  def emptyclone: Node[T]

  def getValue: T

  def fixValues

  def mergeWith(n: Node[T])(ignoreL: Double, ignoreR: Double): Node[T] = {
    if (n.keyLower <= ignoreL || n.keyUpper >= ignoreR)
      this
    else if (keyUpper <= n.keyLower) {
      val w = emptyclone
      w.setRight(n.deepclone)
      w.setLeft(deepclone)
      w
    } else if(keyLower >= n.keyUpper) {
      val w = emptyclone
      w.setRight(deepclone)
      w.setLeft(n.deepclone)
      w
    } else if(keyLower <= n.keyLower) {
      val l = leftChild.mergeWith(n)(ignoreL, n.rightChild.keyLower)
      val r = rightChild.mergeWith(n)(n.leftChild.keyUpper, ignoreR)
      leftChild = null
      setRight(r)
      setLeft(l)
      this
    } else {
      val l = n.leftChild.mergeWith(this)(ignoreL, rightChild.keyLower)
      val r = n.rightChild.mergeWith(this)(n.leftChild.keyUpper, ignoreR)
      n.leftChild = null
      n.setRight(r)
      n.setLeft(l)
      n
    }
  }

  def setRight(n: Node[T]): Unit = {
    if (n != null) {
      n.parent = this
    }
    rightChild = n
    fixValues
  }

  def getParentRelation = (parent, parent != null && this == parent.leftChild)

  def setLeft(n: Node[T]): Unit = {
    if (n != null) {
      n.parent = this
    }
    leftChild = n
    fixValues
  }

  def addPendingOperation(po: PendingOperation[T])

  def applyLazyUpdate: Unit

  def blacking(): Node[T] = {
    assert(color >= 1)
    assert(leftChild.color == 0)
    assert(rightChild.color == 0)
    color -= 1
    leftChild.color = 1
    rightChild.color = 1
    this
  }

  def rb1L(): Node[T] = {
    assert(color >= 1)
    assert(rightChild.color >= 1)
    assert(leftChild.color == 0)
    assert(leftChild.leftChild.color == 0)
    val w1 = color
    val n = rightRotate()
    n.color = w1
    n.rightChild.color = 0
    n
  }

  def rb1R(): Node[T] = {
    assert(color >= 1)
    assert(leftChild.color >= 1)
    assert(rightChild.color == 0)
    assert(rightChild.rightChild.color == 0)
    val w1 = color
    val n = leftRotate()
    n.color = w1
    n.leftChild.color = 0
    n
  }

  def rb2L(): Node[T] = {
    assert(color >= 1)
    assert(rightChild.color >= 1)
    assert(leftChild.color == 0)
    assert(leftChild.rightChild.color == 0)
    leftChild.leftRotate()
    rb1L()
  }


  def rb2R(): Node[T] = {
    assert(color >= 1)
    assert(leftChild.color >= 1)
    assert(leftChild.color == 0)
    assert(rightChild.leftChild.color == 0)
    rightChild.rightRotate()
    rb1R()
  }

  def pushL(): Node[T] = {
    assert(leftChild.color > 1)
    assert(rightChild.color == 1)
    assert(rightChild.leftChild.color > 0)
    assert(rightChild.rightChild.color > 0)
    color += 1
    leftChild.color -= 1
    rightChild.color -= 1
    this
  }


  def pushR(): Node[T] = {
    assert(rightChild.color > 1)
    assert(leftChild.color == 1)
    assert(leftChild.rightChild.color > 0)
    assert(leftChild.leftChild.color > 0)
    color += 1
    rightChild.color -= 1
    leftChild.color -= 1
    this
  }

  def w1L(): Node[T] = {
    assert(leftChild.color > 1)
    assert(rightChild.color == 0)
    assert(rightChild.leftChild.color > 1)
    val w0 = color
    val n = leftRotate()
    n.color = w0
    n.leftChild.color = 1
    n.leftChild.leftChild.color -= 1
    n.leftChild.rightChild.color -= 1
    n
  }

  def w1R(): Node[T] = {
    assert(rightChild.color > 1)
    assert(leftChild.color == 0)
    assert(leftChild.rightChild.color > 1)
    val w0 = color
    val n = rightRotate()
    n.color = w0
    n.rightChild.color = 1
    n.rightChild.rightChild.color -= 1
    n.rightChild.leftChild.color -= 1
    n
  }

  def w2L(): Node[T] = {
    assert(leftChild.color > 1)
    assert(rightChild.color == 0)
    assert(rightChild.leftChild.color == 1)
    assert(rightChild.leftChild.leftChild.color > 0)
    assert(rightChild.leftChild.rightChild.color > 0)

    val w0 = color
    val n = leftRotate()
    n.color = w0
    n.leftChild.color = 1
    n.leftChild.leftChild.color -= 1
    n.leftChild.rightChild.color = 0
    n
  }


  def w2R(): Node[T] = {
    assert(rightChild.color > 1)
    assert(leftChild.color == 0)
    assert(leftChild.rightChild.color == 1)
    assert(leftChild.rightChild.rightChild.color > 0)
    assert(leftChild.rightChild.leftChild.color > 0)

    val w0 = color
    val n = rightRotate()
    n.color = w0
    n.rightChild.color = 1
    n.rightChild.rightChild.color -= 1
    n.rightChild.leftChild.color = 0
    n
  }

  def w3L(): Node[T] = {
    assert(leftChild.color > 1)
    assert(rightChild.color == 0)
    assert(rightChild.leftChild.color == 1)
    assert(rightChild.leftChild.leftChild.color == 0)
    assert(rightChild.leftChild.rightChild.color > 0)

    val w0 = color
    val n = leftRotate()
    n.color = w0
    n.leftChild.color = 1
    n.leftChild.leftChild.color -= 1
    n.leftChild.rightChild = n.leftChild.rightChild.rightRotate()
    n.leftChild = n.leftChild.leftRotate()
    n
  }

  def w3R: Node[T] = {
    assert(rightChild.color > 1)
    assert(leftChild.color == 0)
    assert(leftChild.rightChild.color == 1)
    assert(leftChild.rightChild.rightChild.color == 0)
    assert(leftChild.rightChild.leftChild.color > 0)

    val w0 = color
    val n = rightRotate()
    n.color = w0
    n.rightChild.color = 1
    n.rightChild.rightChild.color -= 1
    n.rightChild.leftChild = n.rightChild.leftChild.leftRotate()
    n.rightChild = n.rightChild.rightRotate()
    n
  }

  def w4L = {
    assert(leftChild.color > 1)
    assert(rightChild.color == 0)
    assert(rightChild.leftChild.color == 1)
    assert(rightChild.leftChild.rightChild.color == 0)

    val w0 = color
    rightChild = rightChild.rightRotate()
    val n = leftRotate()
    n.color = w0
    n.leftChild.color = 1
    n.leftChild.leftChild.color -= 1
    n.rightChild.leftChild.color = 1
    n
  }

  def w4R = {
    assert(rightChild.color > 1)
    assert(leftChild.color == 0)
    assert(leftChild.rightChild.color == 1)
    assert(leftChild.rightChild.leftChild.color == 0)

    val w0 = color
    leftChild = leftChild.leftRotate()
    val n = rightRotate()
    n.color = w0
    n.rightChild.color = 1
    n.rightChild.rightChild.color -= 1
    n.leftChild.rightChild.color = 1
    n
  }


  def w5L = {
    assert(leftChild.color > 1)
    assert(rightChild.color == 1)
    assert(rightChild.rightChild.color == 0)

    val w1 = color
    val n = leftRotate()
    n.color = w1
    n.leftChild.color = 1
    n.leftChild.leftChild.color -= 1
    n.rightChild.color = 1
    n
  }


  def w5R = {
    assert(rightChild.color > 1)
    assert(leftChild.color == 1)
    assert(leftChild.leftChild.color == 0)

    val w1 = color
    val n = rightRotate()
    n.color = w1
    n.rightChild.color = 1
    n.rightChild.rightChild.color -= 1
    n.leftChild.color = 1
    n
  }

  def w6L = {
    assert(leftChild.color > 1)
    assert(rightChild.color == 1)
    assert(rightChild.leftChild.color == 0)
    assert(rightChild.rightChild.color > 0)

    val w1 = color
    rightChild = rightChild.rightRotate()
    val n = leftChild
    n.color = w1
    n.leftChild.color = 1
    n.leftChild.leftChild.color -= 1
    n
  }

  def w6R = {
    assert(rightChild.color > 1)
    assert(leftChild.color == 1)
    assert(leftChild.rightChild.color == 0)
    assert(leftChild.leftChild.color > 0)

    val w1 = color
    leftChild = leftChild.leftRotate()
    val n = rightChild
    n.color = w1
    n.rightChild.color = 1
    n.rightChild.rightChild.color -= 1
    n
  }

  def w7 = {
    assert(leftChild.color > 1)
    assert(rightChild.color > 1)

    color += 1
    leftChild.color -= 1
    rightChild.color -= 1
    this
  }

  def leftRotate(): Node[T] = {
    val x = this
    x.applyLazyUpdate
    val xr = x.rightChild
    assert(xr.minDepth >= 2)
    xr.applyLazyUpdate
    val xrl = xr.leftChild
    x.setRight(xrl)
    xr.setLeft(x)
    xr
  }

  def rightRotate(): Node[T] = {
    val x = this
    x.applyLazyUpdate
    val xl = x.leftChild
    assert(xl.minDepth >= 2)
    xl.applyLazyUpdate
    val xlr = xl.rightChild
    x.setLeft(xlr)
    xl.setRight(x)
    xl
  }


  def successor[U](root: Node[U]) = {
    var x = this
    while (x != root && x != x.parent.leftChild)
      x = x.parent
    if (x == root)
      None
    else {
      x = x.parent.rightChild
      //Some(x.keyLower)
      while (x.leftChild != null) {
        x.applyLazyUpdate
        x = x.leftChild
      }
      x.applyLazyUpdate
      Some(x)

    }
  }

  def predecessor[U](root: Node[U]) = {
    var x = this
    while (x != root && x != x.parent.rightChild)
      x = x.parent
    if (x == root)
      None
    else {
      x = x.parent.leftChild
      //Some(x.keyUpper)
      while (x.rightChild != null) {
        x.applyLazyUpdate
        x = x.rightChild
      }
      x.applyLazyUpdate
      Some(x)
    }
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

class LowerNode[T](k: Double) extends Node[T](k) { //(valueAgg: Aggregator[T], rangeAgg: Aggregator[T], op: ComparatorOp[T]) {

  private var value: ValueNode[T] = null


  override def emptyclone: Node[T] = {
    val c = new LowerNode[T](k)
    c.setValueNode(value.emptyclone)
    c
  }

  override def deepclone = {
    applyLazyUpdate
    val c = new LowerNode[T](k)
    c.keyLower = keyLower
    c.keyUpper = keyUpper

    c.numKeysInRange = numKeysInRange
    c.numDuplicateKey = numDuplicateKey

    c.maxDepth = maxDepth
    c.minDepth = minDepth
    c.color = color
    c.setValueNode(value.deepclone)

    if (leftChild != null) {
      val lc = leftChild.deepclone
      val rc = rightChild.deepclone
      c.setRight(rc)
      c.setLeft(lc)
    }
    c
  }

  def getValueNode = value

  def setValueNode(vn: ValueNode[T]) = {
    value = vn
    vn.keyNode = this
  }


  override def addPendingOperation(po: PendingOperation[T]) = value.addPendingOperation(po)

  def getRightChild = rightChild.asInstanceOf[LowerNode[T]]

  def getLeftChild = leftChild.asInstanceOf[LowerNode[T]]

  def getParent = parent.asInstanceOf[LowerNode[T]]

  def getValue = value.getValue

  override def toString: String = {
    s"D($minDepth, $maxDepth)   c=${color} :: [$keyLower:$keyUpper] = ${value.getValue} (+ ${value.pendingOperation}, * $numKeysInRange) @ $hashCode"
  }

  override def applyLazyUpdate(): Unit = {
    if (value.pendingOperation.isDefined) {
      if (leftChild != null) {
        getLeftChild.value.addPendingOperation(value.pendingOperation.get)
        getRightChild.value.addPendingOperation(value.pendingOperation.get)
      }
      value.applyPendingOperation

    }

  }


  def replaceBy(n: LowerNode[T]) = {
    //No change to parent
    n.applyLazyUpdate
    keyUpper = n.keyUpper
    keyLower = n.keyLower

    setValueNode(n.value)

    leftChild = n.leftChild
    if (leftChild != null)
      leftChild.parent = this
    rightChild = n.rightChild
    if (rightChild != null)
      rightChild.parent = this


    numKeysInRange = n.numKeysInRange
    numDuplicateKey = n.numDuplicateKey

    maxDepth = n.maxDepth
    minDepth = n.minDepth
    color = n.color
  }

  /*
  def checkNode: Unit = {
    applyLazyUpdate()
    if (leftChild == null) {
      assert(getSum >= 0)
      assert(getSum == getMax)
      assert(keyUpper == keyLower)
      assert(numKeysInRange == 1)
      assert(maxDepth == 1)
      assert(minDepth == 1)
      assert(rightChild == null)
    }
    else if (rightChild == null) {
      assert(getSum >= 0)
      assert(getMax == maxVal)
      assert(keyUpper == keyLower)
      assert(numKeysInRange == 1)
      assert(maxDepth == 1)
      assert(minDepth == 1)
      assert(rightChild == null)
      assert(leftChild == null)
    } else {
      assert(getSum >= 0)
      assert(getMax >= 0)
      assert(keyLower == leftChild.keyLower)
      assert(keyLower == leftChild.keyLower)
      assert(keyUpper == rightChild.keyUpper)
      assert(keyLower < keyUpper)
      assert(leftChild.parent == this)
      assert(rightChild.parent == this)
      assert(numDuplicateKey == 0)
      assert(numKeysInRange == leftChild.numKeysInRange + rightChild.numKeysInRange)
      assert(maxDepth == Math.max(leftChild.maxDepth, rightChild.maxDepth) + 1)
      assert(minDepth == Math.min(leftChild.minDepth, rightChild.minDepth) + 1)
//      assert(maxDepth <= 2 * minDepth)
      assert(!isRed || (!leftChild.isRed && !rightChild.isRed))
      assert(getSum == leftChild.getSum + rightChild.getSum)
      assert(getMax == Math.max(getMax, getMax))
      leftChild.checkNode
      rightChild.checkNode

    }
  }
*/
  override def fixValues: Unit = {
    if (leftChild != null) {
      assert(!value.pendingOperation.isDefined)
      val ld = leftChild.maxDepth
      val rd = rightChild.maxDepth


      maxDepth = Math.max(ld, rd) + 1
      val ld2 = leftChild.minDepth
      val rd2 = rightChild.minDepth
      minDepth = Math.min(ld2, rd2) + 1

      value.rangeAggregate(getLeftChild.value, getRightChild.value)

      keyLower = leftChild.keyLower
      keyUpper = rightChild.keyUpper
      numKeysInRange = leftChild.numKeysInRange + rightChild.numKeysInRange
    }
  }

}

class RangeTree2DIterator[T](var it1: Iterator[(Double, RangeTreeLayer[T])], var it2: Iterator[(Double, T)]) extends Iterator[((Double, Double), T)] {
  override def hasNext = it1.hasNext || it2.hasNext

  var key1: Double = -1

  override def next(): ((Double, Double), T) = {
    while (it2 == null || !it2.hasNext) {
      val n1 = it1.next()
      key1 = n1._1
      it2 = n1._2.iterator
    }
    val n2 = it2.next()
    val key2 = n2._1
    val value = n2._2
    ((key1, key2), value)
  }
}

class RangeTreeLeafIterator[T](var n: Node[T]) extends Iterator[(Double, T)] {
  assert(n == null || n.keyLower == n.keyUpper)

  override def hasNext: Boolean = (n != null)

  override def next(): (Double, T) = {
    val res = (n.keyLower, n.getValue)
    if (n.parent == null)
      n = null
    else if (n == n.parent.leftChild) {
      n = n.parent.rightChild
      while (n.leftChild != null)
        n = n.leftChild
    }
    else {
      while (n.parent != null && n == n.parent.rightChild)
        n = n.parent
      if (n.parent == null)
        n = null
      else {
        n = n.parent.rightChild
        while (n.leftChild != null)
          n = n.leftChild
      }
    }
    assert(n == null || n.keyLower == n.keyUpper)
    res
  }
}


abstract class RangeTreeLayer[T](op: ComparatorOp[Double]) {
  var root: Node[T] = null

  def colorWeight = {
    var sum = 0
    var n = root
    while (n != null) {
      sum += n.color
      n = n.leftChild
    }
    sum
  }

  def emptyClone: RangeTreeLayer[T]

  def deepClone: RangeTreeLayer[T]

  def map[U](f: T => U): List[U] = {
    def mapRecurse(f: T => U)(n: Node[T]): List[U] = {
      if (n == null) Nil
      else mapRecurse(f)(n.leftChild) ++ List(f(n.getValue)) ++ mapRecurse(f)(n.rightChild)
    }

    mapRecurse(f)(root)

  }


  def add(key: Double, value: T): Unit

  def clear() = {
    root = null
  }

  def propogateChage(orig: Node[T]) = {
    var n = orig
    while (n != null) {
      n.fixValues
      n = n.parent
    }
  }

  def get(key: Double): Option[T] = {
    if (root == null)
      None
    else {
      var n = root
      while (n.leftChild != null) {
        n.applyLazyUpdate
        if (key <= n.leftChild.keyUpper)
          n = n.leftChild
        else
          n = n.rightChild
      }
      n.applyLazyUpdate
      if (key == n.keyUpper)
        Option(n.getValue)
      else
        None
    }
  }


  def iterator: Iterator[(Double, T)] = {
    val left = if (root == null)
      null
    else {
      var n = root
      while (n.leftChild != null) {
        n.applyLazyUpdate
        n = n.leftChild
      }
      n.applyLazyUpdate
      n
    }
    new RangeTreeLeafIterator(left)
  }


  def addLazyOperation(l: Double, u: Double, lo: PendingOperation[T], n: Node[T]): Unit = {
    n.applyLazyUpdate
    if (l > n.keyUpper || u < n.keyLower)
      return
    else if (l <= n.keyLower && u >= n.keyUpper) {
      n.addPendingOperation(lo)
      n.applyLazyUpdate
    }
    else {
      addLazyOperation(l, u, lo, n.leftChild)
      addLazyOperation(l, u, lo, n.rightChild)
      n.fixValues
    }
  }

  def fixColorDelete(orig: Node[T]) {
    var x = orig
    while (x != root && x.color != 0) {
      if (x == x.parent.leftChild) {
        var w = x.parent.rightChild
        x.parent.fixValues
        //case 1 start
        if (w.color == 0) {
          w.color = 1
          x.parent.color = 0
          if (x.parent == root) {
            root = x.parent.leftRotate
            root.parent = null
          } else if (x.parent == x.parent.parent.leftChild) {
            x.parent.parent.setLeft(x.parent.leftRotate)

          } else {
            x.parent.parent.setRight(x.parent.leftRotate)
          }
          w = x.parent.rightChild
        }
        //check for case 2
        if (w.leftChild == null || (w.leftChild.color != 0 && w.rightChild.color != 0)) {
          w.color = 0
          x = x.parent
        } else {
          //case 3 or 4
          if (w.rightChild.color != 0) {
            //case 3
            w.leftChild.color = 1
            w.color = 0
            //w cannot be  root
            if (w == w.parent.leftChild)
              w.parent.setLeft(w.rightRotate)
            else
              w.parent.setRight(w.rightRotate)
            w = x.parent.rightChild
          }
          //case 4 applied after transforming 3 or directly
          w.color = x.parent.color
          x.parent.color = 1
          w.rightChild.color = 1
          if (x.parent == root) {
            root = x.parent.leftRotate
            root.parent = null
          } else if (x.parent == x.parent.parent.leftChild) {
            x.parent.parent.setLeft(x.parent.leftRotate)

          } else {
            x.parent.parent.setRight(x.parent.leftRotate)
          }
        }
        x.color = 0 //terminate loop
      } else {
        var w = x.parent.leftChild
        x.parent.fixValues
        //case 1 start
        if (w.color == 0) {
          w.color = 1
          x.parent.color = 0
          if (x.parent == root) {
            root = x.parent.rightRotate
            root.parent = null
          } else if (x.parent == x.parent.parent.rightChild) {
            x.parent.parent.setRight(x.parent.rightRotate)

          } else {
            x.parent.parent.setLeft(x.parent.rightRotate)
          }
          w = x.parent.leftChild
        }
        //check for case 2
        if (w.leftChild == null || (w.rightChild.color != 0 && w.leftChild != 0)) {
          w.color = 0
          x = x.parent
        } else {
          //case 3 or 4
          if (w.leftChild.color != 0) {
            //case 3
            w.rightChild.color = 1
            w.color = 0
            //w cannot be  root
            if (w == w.parent.rightChild)
              w.parent.setRight(w.leftRotate)
            else
              w.parent.setLeft(w.leftRotate)
            w = x.parent.leftChild
          }
          //case 4 applied after transforming 3 or directly
          w.color = x.parent.color
          x.parent.color = 1
          w.leftChild.color = 1
          if (x.parent == root) {
            root = x.parent.rightRotate()
            root.parent = null
          } else if (x.parent == x.parent.parent.rightChild) {
            x.parent.parent.setRight(x.parent.rightRotate())

          } else {
            x.parent.parent.setLeft(x.parent.rightRotate())
          }
          x.color = 0 //to terminate loop
        }
      }
    }
    x.color = 1
    while (x != root) {
      x.parent.fixValues
      x = x.parent
    }
    root.fixValues

  }

  def setAsChild(c: Node[T], pr: (Node[T], Boolean)) = {
    if (pr._1 == null)
      root = c
    else if (pr._2)
      pr._1.setLeft(c)
    else
      pr._1.setRight(c)
    c
  }

  def fixViolationInsert(orig: Node[T]) = {

    var n = orig
    while (n != root) {
      if (n.color == 0 && n.parent.color == 0) {
        if (n.parent == root)
          n = root //break
        else {

          val nppr = n.parent.parent.getParentRelation
          if (n.parent == n.parent.parent.leftChild) { //left red-red violation
            if (n.parent.parent.rightChild.color == 0) { //n.parent and uncle red
              n = n.parent.parent.blacking
            }
            else { //uncle not red
              if (n == n.parent.leftChild)
                n = setAsChild(n.parent.parent.rb1L, nppr)
              else
                n = setAsChild(n.parent.parent.rb2L, nppr)
            }
          } else { //right red-red violation
            if (n.parent.parent.leftChild.color == 0) //n.parent and uncle red
              n = n.parent.parent.blacking
            else { //uncle not red
              if (n == n.parent.rightChild)
                n = setAsChild(n.parent.parent.rb1R, nppr)
              else
                n = setAsChild(n.parent.parent.rb2R, nppr)
            }
          }
        }
      } else if (n.color > 1) {
        val npr = n.parent.getParentRelation
        if (n == n.parent.leftChild) { //left overweight violation
          if (n.parent.rightChild.color > 1) //sibling also overweight
            n = n.parent.w7
          else if (n.parent.rightChild.color == 1) { //sibling black
            if (n.parent.rightChild.rightChild.color == 0) //right nephew red
              n = setAsChild(n.parent.w5L, npr)
            else if (n.parent.rightChild.leftChild.color == 0) //right nephew non-red , left nephew red
              n = setAsChild(n.parent.w6L, npr)
            else // both nephews non-red
              n = n.parent.pushL
          } else { //sibling red
            if (n.parent.color == 0) { //n.parent red   //Found left overweight, fix red-red first
              if (n.parent == root)
                n = root //Break
              else {
                val nppr = n.parent.parent.getParentRelation
                if (n.parent.parent.color > 0) {
                  if (n.parent == n.parent.parent.leftChild) {
                    if (n.parent.parent.rightChild.color == 0) {
                      n.parent.parent.blacking //n remains same
                    }
                    else {
                      setAsChild(n.parent.parent.rb2L, nppr) //n remains same
                    }
                  } else { //n.parent is right of grandparent
                    if (n.parent.parent.leftChild.color == 0) {
                      n.parent.parent.blacking //n remains same
                    }
                    else {
                      setAsChild(n.parent.parent.rb1R, nppr) //n remains same
                    }
                  }
                }
                else {
                  ???
                }
              }
            } else { //n.parent non-red
              if (n.parent.rightChild.leftChild.color == 0) //left nephew red
                setAsChild(n.parent.rb2R, npr) //n remains same
              else if (n.parent.rightChild.leftChild.color == 1) { //left nephew black
                if (n.parent.rightChild.leftChild.rightChild.color == 0) { //left nephew right child red
                  n = setAsChild(n.parent.w4L, npr)
                } else { //left nephew right child non-red
                  if (n.parent.rightChild.leftChild.leftChild.color == 0) { //left nephew left child red
                    n = setAsChild(n.parent.w3L, npr)
                  } else { //left nephew both child non-red
                    n = setAsChild(n.parent.w2L, npr)
                  }
                }
              } else {
                //left nephew overweight
                n = setAsChild(n.parent.w1L, npr)
              }


            }
          }
        } else { //right overweight violation


          if (n.parent.rightChild.color > 1) //sibling also overweight
            n = n.parent.w7
          else if (n.parent.rightChild.color == 1) { //sibling black
            if (n.parent.rightChild.rightChild.color == 0) //left nephew red
              n = setAsChild(n.parent.w5R, npr)
            else if (n.parent.rightChild.leftChild.color == 0) //left nephew non-red , right nephew red
              n = setAsChild(n.parent.w6R, npr)
            else // both nephews non-red
              n = n.parent.pushR
          } else { //sibling red

            val nppr = n.parent.parent.getParentRelation
            if (n.parent.color == 0) { //n.parent red //Found right overweight, fix red-red first
              if (n.parent == root)
                n = root //break
              else {
                if (n.parent.parent.color > 0) {
                  if (n.parent == n.parent.parent.leftChild) {
                    if (n.parent.parent.rightChild.color == 0) {
                      n.parent.parent.blacking //n remains same
                    }
                    else {
                      setAsChild(n.parent.parent.rb2R, nppr) //n remains same
                    }
                  } else { //n.parent is left of grandparent
                    if (n.parent.parent.leftChild.color == 0) {
                      n.parent.parent.blacking //n remains same
                    }
                    else {
                      setAsChild(n.parent.parent.rb1L, nppr) //n remains same
                    }
                  }
                }
                else {
                  ???
                }
              }
            } else { //n.parent non-red
              if (n.parent.rightChild.leftChild.color == 0) //right nephew red
                setAsChild(n.parent.rb2L, npr) //n remains same
              else if (n.parent.rightChild.leftChild.color == 1) { //right nephew black
                if (n.parent.rightChild.leftChild.rightChild.color == 0) { //right nephew left child red
                  n = setAsChild(n.parent.w4R, npr)
                } else { //right nephew left child non-red
                  if (n.parent.rightChild.leftChild.leftChild.color == 0) { //right nephew right child red
                    n = setAsChild(n.parent.w3R, npr)
                  } else { //right nephew both child non-red
                    n = setAsChild(n.parent.w2R, npr)
                  }
                }
              } else {
                //right nephew overweight
                n = setAsChild(n.parent.w1R, npr)
              }
            }
          }
        }
      }
    }
    root.color = 1
  }

  def fixColorInsert(orig: Node[T]) {
    var n1 = orig
    var tmp: Node[T] = null
    var cnt = 0
    while (n1.parent != null && n1.parent.color == 0) {
      cnt += 1
      var np = n1.parent
      val npp = np.parent
      np.fixValues
      npp.fixValues
      if (np == npp.leftChild) {
        val uncle = npp.rightChild
        if (uncle.color == 0) {
          uncle.color = 1
          np.color = 1
          npp.color = 0
          n1 = npp
        } else {
          if (n1 == np.rightChild) {
            tmp = n1
            n1 = np
            np = tmp
            npp.setLeft(n1.leftRotate)
          }
          np.color = 1
          npp.color = 0
          if (npp == root) {
            root = npp.rightRotate()
            root.parent = null
          } else {
            if (npp == npp.parent.leftChild)
              npp.parent.setLeft(npp.rightRotate())
            else
              npp.parent.setRight(npp.rightRotate())
          }
        }
      } else {
        val uncle = npp.leftChild
        if (uncle.color == 0) {
          uncle.color = 1
          np.color = 1
          npp.color = 0
          n1 = npp
        } else {
          if (n1 == np.leftChild) {
            tmp = n1
            n1 = np
            np = tmp
            npp.setRight(n1.rightRotate())
          }
          np.color = 1
          npp.color = 0
          if (npp == root) {
            root = npp.leftRotate()
            root.parent = null
          } else {
            if (npp == npp.parent.rightChild)
              npp.parent.setRight(npp.leftRotate())
            else
              npp.parent.setLeft(npp.leftRotate())
          }
        }
      }
    }
    while (n1 != null) {
      if (n1.leftChild != null)
        n1.fixValues
      n1 = n1.parent
    }
    root.color = 1

  }

}

class RangeTreeLowestLayer[T](valueAgg: Aggregator[T], rangeAgg: Aggregator[T])(op: ComparatorOp[Double]) extends RangeTreeLayer[T](op) {
  def getRoot = root.asInstanceOf[LowerNode[T]]

  override def emptyClone: RangeTreeLayer[T] = new RangeTreeLowestLayer[T](valueAgg, rangeAgg)(op)

  override def deepClone: RangeTreeLayer[T] = {
    val c = new RangeTreeLowestLayer[T](valueAgg, rangeAgg)(op)
    c.root = root.deepclone
    c
  }

  def rangeQuery(l: Double, r: Double, n: Node[T]): T = {
    if (n == null)
      return rangeAgg.zero
    n.applyLazyUpdate
    if (r < n.keyLower || l > n.keyUpper)
      return rangeAgg.zero
    if (l <= n.keyLower && n.keyUpper <= r)
      return n.getValue
    if (r < n.leftChild.keyUpper)
      return rangeQuery(l, r, n.leftChild)
    if (l > n.rightChild.keyLower)
      return rangeQuery(l, r, n.rightChild)
    val lval = rangeQuery(l, n.leftChild.keyUpper, n.leftChild)
    val rval = rangeQuery(n.rightChild.keyLower, r, n.rightChild)
    return rangeAgg(lval, rval)
  }

  def domainConversion(src: LowerNode[Int], dest: LowerNode[Int]): Unit = {
    src.applyLazyUpdate()
    dest.applyLazyUpdate()
    if (src.getValue == -1)
      ()
    else if (src.getValue == 0) {
      domainConversion(src.getLeftChild, dest)
      domainConversion(src.getRightChild, dest)
    } else {
      if (op == EqualTo) { //For equality we need to compare leaf with leaf
        if (dest.keyLower > src.keyUpper || dest.keyUpper < src.keyLower)
          ()
        else {
          if (src.leftChild != null) {
            domainConversion(src.getLeftChild, dest)
            domainConversion(src.getRightChild, dest)
          } else { //leaf of src
            if (dest.leftChild != null) {
              domainConversion(src, dest.getLeftChild)
              domainConversion(src, dest.getRightChild)
              dest.getValueNode.rangeAggregate(dest.getLeftChild.getValueNode, dest.getRightChild.getValueNode)
            } else { //leaf of dest
              if (dest.keyLower == src.keyLower)
                dest.getValueNode.addPendingOperation(LazySet(1))
            }
          }
        }
      } else {
        val (falseCondition, trueCondition) = op match {
          case LessThan() | GreaterThanEqual() => (dest.keyLower > src.keyUpper || dest.keyLower <= src.predecessor(root).map(_.keyUpper).getOrElse(Double.NegativeInfinity), dest.keyUpper <= src.keyUpper)
          case GreaterThan() | LessThanEqual() => (dest.keyUpper < src.keyLower || dest.keyUpper >= src.successor(root).map(_.keyLower).getOrElse(Double.PositiveInfinity), dest.keyLower >= src.keyLower)
        }
        if (falseCondition)
          ()
        else if (trueCondition)
          dest.getValueNode.addPendingOperation(LazySet(1))
        else {
          domainConversion(src, dest.getLeftChild)
          domainConversion(src, dest.getRightChild)
        }
      }
    }
  }

  def filterValue(cmp: ComparatorOp[T], c: T, src: LowerNode[T], dest: LowerNode[Int]): ValueNode[Int] = {
    val vn = dest.getValueNode
    src.applyLazyUpdate()
    dest.applyLazyUpdate()
    if (cmp.apply(src.getValue, c)) {
      vn.addPendingOperation(LazySet(1))
    } else {
      if (src.leftChild != null) {
        val lvn = filterValue(cmp, c, src.getLeftChild, dest.getLeftChild)
        val rvn = filterValue(cmp, c, src.getRightChild, dest.getRightChild)
        vn.rangeAggregate(lvn, rvn)
      }
    }
    vn
  }

  def listTruthInterval(n: LowerNode[Int]): List[LowerNode[Int]] = n.getValue match {
    case 1 => List(n)
    case -1 => Nil
    case 0 => listTruthInterval(n.getLeftChild) ++ listTruthInterval(n.getRightChild)
  }


  override def add(key: Double, value: T) = {
    if (root == null) {
      op match {
        case LessThan() =>
          val leftValueNode = new ValueNode[T](valueAgg.zero)(valueAgg, rangeAgg)
          val rightValueNode = new ValueNode[T](value)(valueAgg, rangeAgg)
          val rootValueNode = new ValueNode[T](value)(valueAgg, rangeAgg)

          val r = new LowerNode[T](key)
          r.setValueNode(rootValueNode)
          root = r

          val rl = new LowerNode[T](key)
          rl.setValueNode(leftValueNode)
          rl.color = 1
          root.leftChild = rl

          val rr = new LowerNode[T](Double.PositiveInfinity)
          rr.setValueNode(rightValueNode)
          rr.color = 1
          root.rightChild = rr

          root.numDuplicateKey = 0
          root.maxDepth = 2
          root.minDepth = 2

          root.leftChild.parent = root
          root.rightChild.parent = root
          root.keyUpper = Double.PositiveInfinity

        case GreaterThan() =>
          val leftValueNode = new ValueNode[T](value)(valueAgg, rangeAgg)
          val rightValueNode = new ValueNode[T](valueAgg.zero)(valueAgg, rangeAgg)
          val rootValueNode = new ValueNode[T](value)(valueAgg, rangeAgg)

          val r = new LowerNode[T](key)
          r.setValueNode(rootValueNode)
          root = r

          val rl = new LowerNode[T](Double.NegativeInfinity)
          rl.setValueNode(leftValueNode)
          rl.color = 1
          root.leftChild = rl

          val rr = new LowerNode[T](key)
          rr.setValueNode(rightValueNode)
          rr.color = 1
          root.rightChild = rr

          root.numDuplicateKey = 0
          root.maxDepth = 2
          root.minDepth = 2

          root.leftChild.parent = root
          root.rightChild.parent = root
          root.keyLower = Double.NegativeInfinity
        case _ =>
          val valueNode = new ValueNode[T](value)(valueAgg, rangeAgg)
          val r = new LowerNode[T](key)
          r.setValueNode(valueNode)
          r.color = 1
          root = r
      }

    } else {
      var n = getRoot
      while (n.leftChild != null) {
        n.applyLazyUpdate
        if (key <= n.leftChild.keyUpper) { // going left
          n = n.getLeftChild
        } else { //going right
          n = n.getRightChild
        }
      }
      n.applyLazyUpdate

      if (key == n.keyLower) { //Key exists. Only update for = directly
        n.numDuplicateKey += 1
        op match {
          case EqualTo() =>
            n.getValueNode.addValue(value)
            propogateChage(n.getParent)
          case GreaterThanEqual() =>
            addLazyOperation(root.keyLower, key, LazyAdd(value), root)
          case GreaterThan() =>
            if (n.predecessor(root).isDefined) //always defined?
              addLazyOperation(root.keyLower, n.predecessor(root).get.keyUpper, LazyAdd(value), root)
          case LessThan() =>
            if (n.successor(root).isDefined) //always defined?
              addLazyOperation(n.successor(root).get.keyLower, root.keyUpper, LazyAdd(value), root)
          case LessThanEqual() =>
            addLazyOperation(key, root.keyUpper, LazyAdd(value), root)
        }


      } else {
        if (key < n.keyLower) {
          val initValue: T = op match {
            case EqualTo() => value
            case LessThan() | GreaterThanEqual() =>
              n.getValue //successor value
            case GreaterThan() | LessThanEqual() =>
              n.predecessor(root).map(_.asInstanceOf[LowerNode[T]].getValue).getOrElse(valueAgg.zero)
          }
          val leftValueNode = new ValueNode(initValue)(valueAgg, rangeAgg)
          val rightValueNode = new ValueNode(n.getValue)(valueAgg, rangeAgg)

          val nl = new LowerNode[T](key)
          nl.setValueNode(leftValueNode)
          n.leftChild = nl

          val nr = new LowerNode[T](n.keyLower)
          nr.setValueNode(rightValueNode)
          n.rightChild = nr

          n.rightChild.numDuplicateKey = n.numDuplicateKey
          n.numDuplicateKey = 0
          n.maxDepth = 2
          n.minDepth = 2
          n.leftChild.parent = n
          n.rightChild.parent = n
          n.keyLower = key

          n.getValueNode.rangeAggregate(leftValueNode, rightValueNode)
          n = n.getLeftChild

        } else {

          val initValue: T = op match {
            case EqualTo() => value
            case LessThan() | GreaterThanEqual() =>
              n.successor(root).map(_.asInstanceOf[LowerNode[T]].getValue).getOrElse(valueAgg.zero)
            case GreaterThan() | LessThanEqual() =>
              n.getValue //predecessor value
          }

          val leftValueNode = new ValueNode(n.getValue)(valueAgg, rangeAgg)
          val rightValueNode = new ValueNode(initValue)(valueAgg, rangeAgg)

          val nl = new LowerNode[T](n.keyLower)
          nl.setValueNode(leftValueNode)
          n.leftChild = nl

          val nr = new LowerNode[T](key)
          nr.setValueNode(rightValueNode)
          n.rightChild = nr

          n.leftChild.numDuplicateKey = n.numDuplicateKey
          n.numDuplicateKey = 0
          n.maxDepth = 2
          n.minDepth = 2
          n.leftChild.parent = n
          n.rightChild.parent = n
          n.keyUpper = key
          n.getValueNode.rangeAggregate(leftValueNode, rightValueNode)

          n = n.getRightChild

        }

        fixColorInsert(n)
        val successorKey = n.successor(root).map(_.keyLower).getOrElse(Double.PositiveInfinity)
        val predecessorKey = n.predecessor(root).map(_.keyUpper).getOrElse(Double.NegativeInfinity)
        op match {
          case EqualTo() => ()
          case LessThan() => addLazyOperation(successorKey, root.keyUpper, LazyAdd(value), root)
          case LessThanEqual() => addLazyOperation(key, root.keyUpper, LazyAdd(value), root)
          case GreaterThan() => addLazyOperation(root.keyLower, predecessorKey, LazyAdd(value), root)
          case GreaterThanEqual() => addLazyOperation(root.keyLower, key, LazyAdd(value), root)
        }
      }
    }

  }


  def remove(key: Double, value: T): T = {

    var n = getRoot
    while (n.leftChild != null) {
      n.applyLazyUpdate
      assert(n.leftChild.parent == n)
      assert(n.rightChild.parent == n)
      if (key <= n.leftChild.keyUpper) { // going left
        n = n.getLeftChild
      } else { //going right
        n = n.getRightChild
      }
    }
    n.applyLazyUpdate

    val removedValue = n.getValueNode.removeValue(value) //SBJ: Works only for Sum
    n.numDuplicateKey -= 1
    ??? //Add range update on the decrem ent
    val toDelete = (n.numDuplicateKey == 0)
    if (n == root) {
      root = null
    } else {
      if (n == n.parent.leftChild) {
        //          val s = n.parent.rightChild
        if (toDelete) {
          val parIsRed = n.parent.color
          n.getParent.replaceBy(n.getParent.getRightChild)
          if (parIsRed != 0)
            fixColorDelete(n.getParent)
          else
            propogateChage(n.getParent)
        } else
          propogateChage(n.getParent)

      } else {
        //          val s = n.parent.leftChild
        if (toDelete) {
          val parIsRed = n.parent.color
          n.getParent.replaceBy(n.getParent.getLeftChild)
          if (parIsRed != 0)
            fixColorDelete(n.getParent)
          else
            propogateChage(n.getParent)
        }
        else
          propogateChage(n.getParent)
      }
    }
    removedValue
  }

}

//T = Node[V]
class RangeTreeHigherLayer[V](op: ComparatorOp[Double]) extends RangeTreeLayer[RangeTreeLayer[V]](op) {
  type T = RangeTreeLayer[V]


  def getRoot = root.asInstanceOf[HigherNode[V]]

  override def emptyClone: RangeTreeLayer[T] = new RangeTreeHigherLayer[V](op)

  override def deepClone: RangeTreeHigherLayer[V] = {
    val c = new RangeTreeHigherLayer[V](op)
    c.root = root.deepclone
    c
  }

  override def add(key: Double, value: T) = {

    //assuming value is nextlayer tree with single node
    assert(value.root.keyLower == value.root.keyUpper)

    if (root == null) {
      op match {
        case LessThan() =>
          val leftValueNode: T = value.emptyClone
          val rightValueNode = value
          val rootValueNode = value.deepClone

          val r = new HigherNode[V](key)
          r.nextLayer = rootValueNode
          root = r

          val rl = new HigherNode[V](key)
          rl.nextLayer = leftValueNode
          rl.color = 1
          root.leftChild = rl

          val rr = new HigherNode[V](Double.PositiveInfinity)
          rr.nextLayer = rightValueNode
          rr.color = 1
          root.rightChild = rr

          root.numDuplicateKey = 0
          root.maxDepth = 2
          root.minDepth = 2

          root.leftChild.parent = root
          root.rightChild.parent = root
          root.keyUpper = Double.PositiveInfinity

        case GreaterThan() =>
          val leftValueNode = value
          val rightValueNode = value.emptyClone
          val rootValueNode = value.deepClone

          val r = new HigherNode[V](key)
          r.nextLayer = rootValueNode
          root = r

          val rl = new HigherNode[V](Double.NegativeInfinity)
          rl.nextLayer = leftValueNode
          rl.color = 1
          root.leftChild = rl

          val rr = new HigherNode[V](key)
          rr.nextLayer = rightValueNode
          rr.color = 1
          root.rightChild = rr

          root.numDuplicateKey = 0
          root.maxDepth = 2
          root.minDepth = 2

          root.leftChild.parent = root
          root.rightChild.parent = root
          root.keyLower = Double.NegativeInfinity
        case _ =>

          val r = new HigherNode[V](key)
          r.nextLayer = value
          r.color = 1
          root = r
      }

    } else {
      var n = getRoot
      while (n.leftChild != null) {
        n.nextLayer.add(value.root.keyLower, value.root.getValue)
        n.applyLazyUpdate
        if (key <= n.leftChild.keyUpper) { // going left
          n = n.getLeftChild
        } else { //going right
          n = n.getRightChild
        }
      }
      n.applyLazyUpdate

      if (key == n.keyLower) { //Key exists. Only update for = directly
        n.numDuplicateKey += 1
        op match {
          case EqualTo() =>
            n.nextLayer.add(value.root.keyLower, value.root.getValue)
            propogateChage(n.getParent)
          case GreaterThanEqual() =>
            addLazyOperation(root.keyLower, key, LazyAdd(value), root)
          case GreaterThan() =>
            if (n.predecessor(root).isDefined) //always defined?
              addLazyOperation(root.keyLower, n.predecessor(root).get.keyUpper, LazyAdd(value), root)
          case LessThan() =>
            if (n.successor(root).isDefined) //always defined?
              addLazyOperation(n.successor(root).get.keyLower, root.keyUpper, LazyAdd(value), root)
          case LessThanEqual() =>
            addLazyOperation(key, root.keyUpper, LazyAdd(value), root)
        }


      } else {
        if (key < n.keyLower) {
          val initValue: T = op match {
            case EqualTo() => value
            case LessThan() | GreaterThanEqual() =>
              n.getValue.deepClone //successor value
            case GreaterThan() | LessThanEqual() =>
              n.predecessor(root).map(_.asInstanceOf[HigherNode[V]].getValue.deepClone).getOrElse(value.emptyClone)
          }
          val leftValueNode = initValue
          val rightValueNode = n.getValue.deepClone

          val nl = new HigherNode[V](key)
          nl.nextLayer = leftValueNode
          n.leftChild = nl

          val nr = new HigherNode[V](n.keyLower)
          nr.nextLayer = rightValueNode
          n.rightChild = nr

          n.rightChild.numDuplicateKey = n.numDuplicateKey
          n.numDuplicateKey = 0
          n.maxDepth = 2
          n.minDepth = 2
          n.leftChild.parent = n
          n.rightChild.parent = n
          n.keyLower = key

          n.nextLayer.add(value.root.keyLower, value.root.getValue)
          n = n.getLeftChild

        } else {

          val initValue: T = op match {
            case EqualTo() => value
            case LessThan() | GreaterThanEqual() =>
              n.successor(root).map(_.asInstanceOf[HigherNode[V]].getValue.deepClone).getOrElse(value.emptyClone)
            case GreaterThan() | LessThanEqual() =>
              n.getValue.deepClone //predecessor value
          }

          val leftValueNode = n.getValue.deepClone
          val rightValueNode = initValue

          val nl = new HigherNode[V](n.keyLower)
          nl.nextLayer = leftValueNode
          n.leftChild = nl

          val nr = new HigherNode[V](key)
          nr.nextLayer = rightValueNode
          n.rightChild = nr

          n.leftChild.numDuplicateKey = n.numDuplicateKey
          n.numDuplicateKey = 0
          n.maxDepth = 2
          n.minDepth = 2
          n.leftChild.parent = n
          n.rightChild.parent = n
          n.keyUpper = key
          n.nextLayer.add(value.root.keyLower, value.root.getValue)

          n = n.getRightChild

        }

        fixColorInsert(n)
        val successorKey = n.successor(root).map(_.keyLower).getOrElse(Double.PositiveInfinity)
        val predecessorKey = n.predecessor(root).map(_.keyUpper).getOrElse(Double.NegativeInfinity)
        op match {
          case EqualTo() => ()
          case LessThan() => addLazyOperation(successorKey, root.keyUpper, LazyAdd(value), root)
          case LessThanEqual() => addLazyOperation(key, root.keyUpper, LazyAdd(value), root)
          case GreaterThan() => addLazyOperation(root.keyLower, predecessorKey, LazyAdd(value), root)
          case GreaterThanEqual() => addLazyOperation(root.keyLower, key, LazyAdd(value), root)
        }
      }
    }
  }


  def rangeQuery(l: Double, r: Double, n: Node[T]): List[T] = {
    if (n == null)
      return Nil
    n.applyLazyUpdate
    if (r < n.keyLower || l > n.keyUpper)
      return Nil
    if (l <= n.keyLower && n.keyUpper <= r)
      return List(n.getValue)
    if (r < n.leftChild.keyUpper)
      return rangeQuery(l, r, n.leftChild)
    if (l > n.rightChild.keyLower)
      return rangeQuery(l, r, n.rightChild)
    val lval = rangeQuery(l, n.leftChild.keyUpper, n.leftChild)
    val rval = rangeQuery(n.rightChild.keyLower, r, n.rightChild)
    return (lval ++ rval)
  }


}


class RangeTree[T](val name: String)(valueAgg: Aggregator[T], rangeAgg: Aggregator[T], op: ComparatorOp[Double]) extends collection.mutable.Map[Double, T] {

  val layer = new RangeTreeLowestLayer[T](valueAgg, rangeAgg)(op)

  override def clear() = {
    layer.clear()
  }

  override def +=(kv: (Double, T)) = {
    layer.add(kv._1, kv._2)
    this
  }

  override def -=(key: Double) = {
    ???
  }

  override def get(key: Double): Option[T] = {
    layer.get(key)
  }

  override def toString = name + "  " + layer.root.printTree(0)

  override def iterator: Iterator[(Double, T)] = layer.iterator

  def rangeQuery(l: Double, r: Double) = layer.rangeQuery(l, r, layer.root)


  //def makeRange(s: Double, e: Double): List[(Double, Double)] = {
  //  if (s <= e)
  //    List((s, e))
  //  else
  //    Nil
  //}

  def rangeValueIncrement(l: Double, u: Double, v: T): Unit = {
    layer.addLazyOperation(l, u, LazyAdd(v), layer.root)
  }


}


*/
