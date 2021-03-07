package ds

import collection.mutable.TreeMap

case class Row(val a: Array[Double]) {
  val size = a.size

  def apply(i: Int) = a(i)


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
