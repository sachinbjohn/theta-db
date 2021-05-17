package ds

import utils._
import utils.Helper.DoubleComparisons

case class Domain(val arr: Array[Double], val increasing: Boolean = true) {
  var sameAsOuter = true
  var first = if (increasing) Double.NegativeInfinity else Double.PositiveInfinity
  var last = if (increasing) Double.PositiveInfinity else Double.NegativeInfinity

  def apply(i: Int) = {
    assert(i >= -1 && i <= arr.size)
    if (i < 0)
      first
    else if (i < arr.size)
      arr(i)
    else
      last
  }


  def size = arr.size

  def findPredEq(v: Double, op: ComparatorOp[Double]): Double = {
    //domain is sorted in reverse order if op is > or >=
    if (op== EqualTo)
      v
    else {
      var l = 0
      var r = arr.size - 1
      var mid = 0
      while (l <= r) {
        mid = (l + r) / 2
        if (arr(mid) == v)
          return v
        else if (op(v, arr(mid)))
          r = mid - 1
        else
          l = mid + 1
      }
      val index = if (op(v, arr(mid))) mid - 1 else mid
      if (index == -1) op.first else arr(index)
    }
  }
}

case class MultiDomain(val hmarr: Map[Row, Map[Row, Array[Domain]]]) {
  def apply(gbyKey: Row, eqKey: Row) = hmarr(gbyKey)(eqKey)
}

object MultiDomain {
  def fromTable(groupedTable: Map[Row, Map[Row, Seq[Row]]], keyFn: Row => Array[Double], ops: Array[ComparatorOp[Double]]) = {
    def extractDom(rs: Seq[Row]) = {
      ops.zipWithIndex.map { case (op, i) =>
        val (order, isIncr) = op match {
          case GreaterThan | GreaterThanEqual => Ordering[Double].reverse -> false
          case _ => Ordering[Double] -> true
        }
        Domain(rs.map(r => keyFn(r)(i)).distinct.toArray.sorted(order), isIncr)
      }
    }

    val domains = groupedTable.map { kv => kv._1 -> kv._2.map(xy => xy._1 -> extractDom(xy._2)) }
    MultiDomain(domains)
  }
}

case class Row(val a: Array[Double]) {
  val size = a.size

  def apply(i: Int) = a(i)

  def join(that: Row) = Row(a ++ that.a)

  override def toString: String = a.mkString("[", ",", "]")

  override def hashCode(): Int = a.foldLeft(0)(_ * _.toInt)

  override def equals(that: Any): Boolean = {
    if (!that.isInstanceOf[Row])
      false
    else {
      val obj = that.asInstanceOf[Row]
      a.zip(obj.a).map(kv => kv._1 == kv._2).foldLeft(true)(_ && _)
    }
  }
}

object Row {
  def empty = new Row(Array.empty[Double])
}

class Table(val name: String, val rows: Seq[Row]) {
  def iterator = rows.iterator

  def foreach[U](f: (Row => U)) = rows.foreach(f)

}
