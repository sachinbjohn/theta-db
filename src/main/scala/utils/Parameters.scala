package utils

import scala.collection.mutable.ListBuffer

case class Parameters(n: Int, p: Int, t: Int, pt: Int, q: Int, a: Int) {
  assert(pt <= p * t, s"PT $pt > P $p * T $t")
  assert(pt <= n, s"PT $pt > N $n")
  assert(pt >= p, s"PT $pt < P $p")
  assert(pt >= t, s"PT $pt < T $t")
  assert(a != 3 || p * t * t < (1 << 27), s"PTT $p ($t)^2 >= 2^27)")
  override def toString = s"$n:$p:$t:$pt:VWAP$q:${1<<(a-1)}"
}

object Parameters {
  def main(args: Array[String]): Unit = {
    val ps = new ListBuffer[Parameters]()

    //n, p, pt increase linearly, t const
    ps ++= (1 to 4).flatMap(a => (1 to 3).flatMap(q => (8 to 16).map(i => Parameters(1 << i, 1 << (i - 6), 1 << 6, 1 << (i - 2), q, a))))

    //n, t, pt const, p increase linearly
    ps ++= (1 to 4).flatMap(a => (1 to 3).flatMap(q => (5 to 12).map(i => Parameters(1 << 14, 1 << i, 1 << 7, 1 << 12, q, a))))

    //n, p, pt const, t increase linearly
    ps ++= (1 to 4).flatMap(a => (1 to 3).flatMap(q => (2 to 9).map(i => Parameters(1 << 14, 1 << 7, 1 << i, 1 << 9, q, a))))

    //p,t const , pt and n increase linearly
    ps ++= (1 to 4).flatMap(a => (1 to 3).flatMap(q =>(8 to 14).map(i => Parameters(1 << (i + 2), 1 << 8, 1 << 8, 1 << i, q, a))))

    //keep pt const for query 2
    ps ++= (1 to 4).flatMap(a => (4 to 12).map(i => Parameters(1 << 14, 1 << i, 1 << (14-i) , 1 << 12, 2, a)))

    //keep pt^2 const for query 3
    ps ++= (1 to 4).flatMap(a => (4 to 12).map(i => Parameters(1 << 14, 1 << i, (1 << 12) / Math.pow(2, i/2.0).toInt , 1 << 12, 3, a)))

    ps.distinct.foreach(println)
  }
}
