package exec

import queries.{VWAP1Algo1, VWAP1Algo2, VWAP1Naive, VWAP1_DBT_LMS, VWAP2Algo1, VWAP2Algo2, VWAP2_DBT_LMS, VWAP3Algo1, VWAP3Algo2, VWAP3_DBT_LMS}

import scala.collection.mutable.ListBuffer
import scala.util.Random

case class ParamsVWAP(n: Int, p: Int, t: Int, r: Int, qa: VWAPExecutable) {
  assert(r <= p * t, s"PT $r > P $p * T $t")
  assert(r <= n, s"PT $r > N $n")
  assert(r >= p, s"PT $r < P $p")
  assert(r >= t, s"PT $r < T $t")
  assert(p * t * t < (1 << 27), s"PTT $p ($t)^2 >= 2^27)")

  override def toString = s"${qa.query},${qa.algo},$n,$p,$t,$r"
}

object ParamsVWAP {

  def generate(numExecutors: Int) = {
    val ps = new ListBuffer[ParamsVWAP]()
    val t = 9
    val r = 12
    val qas: List[VWAPExecutable] = List(VWAP1_DBT_LMS, VWAP1Algo1, VWAP1Algo2, VWAP2_DBT_LMS, VWAP2Algo1, VWAP2Algo2, VWAP3_DBT_LMS, VWAP3Algo1, VWAP3Algo2)
    ps ++= qas.flatMap(qa =>
      (10 to 18).flatMap(n =>
        (2 to n / 2).flatMap(p =>
          (5 to 9).flatMap(r =>
            make(1 << n, 1 << (p * 2), 1 << t, 1 << (r * 2), qa)))))
    Random.shuffle(ps.distinct.toList).grouped(numExecutors).toArray
  }

  def make(n: Int, p: Int, t: Int, r: Int, qa: VWAPExecutable): Option[ParamsVWAP] = {
    var res: Option[ParamsVWAP] = None

    def check(c: Boolean, msg: String, rest: => Unit): Unit = {
      if (c)
        rest
      else {
        ()
        //println(msg)
        //rest
        ()
      }
    }

    val (c1, m1) = (r <= p * t, s"PT $r > P $p * T $t")
    val (c2, m2) = (r <= n, s"PT $r > N $n")
    val (c3, m3) = (r >= p, s"PT $r < P $p")
    val (c4, m4) = (r >= t, s"PT $r < T $t")
    val (c5, m5) = (p * t * t < (1 << 27), s"PTT $p ($t)^2 >= 2^27)")

    def f: Unit = {
      res = Some(ParamsVWAP(n, p, t, r, qa))
      ()
    }

    check(c1, m1, check(c2, m2, check(c3, m3, check(c4, m4, check(c5, m5, f)))))
    res
  }

}