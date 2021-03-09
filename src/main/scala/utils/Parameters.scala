package utils

import scala.collection.mutable.ListBuffer

case class Parameters(n: Int, p: Int, t: Int, pt: Int) {
  assert(pt <= p*t)
  override def toString = s"$n:$p:$t:$pt"
}
object Parameters {
  def main(args: Array[String]): Unit = {
    val ps = new ListBuffer[Parameters]()
    ps ++= (8 to 16).map(i => Parameters(1<<i, 1 << (i-6), 1 << (i-6), 1 << (i-4)))
    ps ++= (5 to 13).map(i => Parameters(1<<14, 1 << i, 1 << 5 , 1 << 10))
    ps ++= (5 to 13).map(i => Parameters(1<<14, 1 << 5, 1 << i , 1 << 10))
    ps ++= (6 to 14).map(i => Parameters(1<<(i+2), 1 << 8, 1 << 8 , 1 << i))

    ps.foreach(println)
  }
}
