package ds

import utils.ComparatorOp
import utils.Helper.DoubleComparisons

import collection.mutable.TreeMap
case class Domain(val arr: Array[Double]) {
  def apply(i: Int) = arr(i)
  def size = arr.size
  def findPredEq(v: Double, op: ComparatorOp[Double]): Double = {
    var l = 0
    var r = arr.size - 1
    var mid = 0
    while(l <= r) {
      mid = (l+r)/2
      if(arr(mid) == v)
        return v
      else if(op(v, arr(mid)))
        r = mid - 1
      else
        l = mid + 1
    }
    val index = if(op(v, arr(mid))) mid - 1 else mid
    if(index == -1) op.first else arr(index)
  }
}
case class Row(val a: Array[Double]) {
  val size = a.size

  def apply(i: Int) = a(i)

  def join(that: Row) = Row(a ++ that.a)
  override def toString: String = a.mkString("[", ",", "]")

  override def equals(that: Any): Boolean = {
    if (!that.isInstanceOf[Row])
      false
    else {
      val obj = that.asInstanceOf[Row]
      a.zip(obj.a).map(kv => kv._1 == kv._2).reduce(_ && _)
    }
  }
}

class Table(val name: String, val rows: Seq[Row]) {
  def iterator = rows.iterator
  def foreach[U](f: (Row => U) )= rows.foreach(f)

}
