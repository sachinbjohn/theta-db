package exec

import scala.collection.mutable.ListBuffer

case class Parameters(n: Int, p: Int, t: Int, r: Int, q: Int, a: Int) {
  assert(r <= p * t, s"PT $r > P $p * T $t")
  assert(r <= n, s"PT $r > N $n")
  assert(r >= p, s"PT $r < P $p")
  assert(r >= t, s"PT $r < T $t")
  assert(a != 3 || p * t * t < (1 << 27), s"PTT $p ($t)^2 >= 2^27)")
  override def toString = s"$n:$p:$t:$r:VWAP${q}Obj:${1<<(a-1)}"

}
object Parameters {
  def check(c: Boolean, msg: String, rest:  => Unit): Unit ={
    if(c)
      rest
    else {
      ()
      //println(msg)
    //rest
    ()
    }
  }
  def make(n: Int, p: Int, t: Int, r: Int, q: Int, a: Int): Option[Parameters] = {
    var res: Option[Parameters] = None

    val (c1, m1) = (r <= p * t, s"PT $r > P $p * T $t")
    val (c2, m2) = (r <= n, s"PT $r > N $n")
    val (c3, m3) = (r >= p, s"PT $r < P $p")
    val (c4, m4) = (r >= t, s"PT $r < T $t")
    val (c5, m5) = (a != 3 || p * t * t < (1 << 27), s"PTT $p ($t)^2 >= 2^27)")

    def f : Unit  = {
      res = Some(Parameters(n, p, t, r, q, a))
      ()
    }
    check(c1, m1, check(c2, m2, check(c3, m3, check(c4, m4, check(c5, m5, f)))))
    res
  }
  def main(args: Array[String]): Unit = {
    val ps = new ListBuffer[Parameters]()

    val t = 9
    val r = 12
    ps ++= (3 to 5).flatMap(a =>
      (1 to 3).flatMap(q =>
        (10 to 18).flatMap(n =>
          (2 to n/2).flatMap(p =>
            (5 to 9).flatMap(r =>
            make(1 << n, 1 << (p*2), 1 << t, 1 << (r*2), q, a))))))
    //
    //N = R increasing   P, T const
    //ps ++= (3 to 5).flatMap(a => (1 to 3).flatMap(q => (10 to 16).map(i => Parameters(1 << i, 1 << 6, 1 << 10, 1 << i, q, a))))
    //
    //ps ++= (3 to 5).flatMap(a => (1 to 3).flatMap(q => (10 to 16).map(i => Parameters(1 << i, 1 << 8, 1 << 8, 1 << i, q, a))))
    //
    ////N, R, T const   P increasing
    //ps ++= (3 to 5).flatMap(a => (1 to 3).flatMap(q => (4 to 10).map(i => Parameters(1 << 18, 1 << i, 1 << 8, 1 << 12, q, a))))
    //ps ++= (3 to 5).flatMap(a => (1 to 3).flatMap(q => (4 to 10).map(i => Parameters(1 << 12, 1 << i, 1 << 8, 1 << 12, q, a))))
    //
    //ps ++= (3 to 5).flatMap(a => (1 to 3).flatMap(q => (12 to 18).map(i => Parameters(1 << i, 1 << 8, 1 << 8, 1 << 12, q, a))))


    val ps2 = ps.distinct

    ps2.foreach(println)
    println("COUNT = "+ps2.size)
  }
}
