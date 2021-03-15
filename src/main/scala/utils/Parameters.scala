package utils

import scala.collection.mutable.ListBuffer

case class Parameters(n: Int, p: Int, t: Int, pt: Int, q: Int, a: Int) {
  assert(pt <= p * t, s"PT $pt > P $p * T $t")
  assert(pt <= n, s"PT $pt > N $n")
  assert(pt >= p, s"PT $pt < P $p")
  assert(pt >= t, s"PT $pt < T $t")
  assert(a != 3 || p * t * t < (1 << 27), s"PTT $p ($t)^2 >= 2^27)")
  override def toString = s"$n:$p:$t:$pt:VWAP${q}Obj:${1<<(a-1)}"
}

object Parameters {
  def main(args: Array[String]): Unit = {
    val ps = new ListBuffer[Parameters]()

    //N = R increasing   P, T const
    ps ++= (3 to 5).flatMap(a => (1 to 3).flatMap(q => (10 to 16).map(i => Parameters(1 << i, 1 << 6, 1 << 10, 1 << i, q, a))))

    ps ++= (3 to 5).flatMap(a => (1 to 3).flatMap(q => (10 to 16).map(i => Parameters(1 << i, 1 << 8, 1 << 8, 1 << i, q, a))))

    //N, R, T const   P increasing
    ps ++= (3 to 5).flatMap(a => (1 to 3).flatMap(q => (4 to 10).map(i => Parameters(1 << 18, 1 << i, 1 << 8, 1 << 12, q, a))))
    ps ++= (3 to 5).flatMap(a => (1 to 3).flatMap(q => (4 to 10).map(i => Parameters(1 << 12, 1 << i, 1 << 8, 1 << 12, q, a))))

    ps ++= (3 to 5).flatMap(a => (1 to 3).flatMap(q => (12 to 18).map(i => Parameters(1 << i, 1 << 8, 1 << 8, 1 << 12, q, a))))




    ps.distinct.foreach(println)
  }
}
