package exec

import Math._
import queries.{VWAP1Algo1, VWAP1Algo2, VWAP1Naive, VWAP1_DBT_LMS, VWAP2Algo1, VWAP2Algo2, VWAP2_DBT_LMS, VWAP3Algo1, VWAP3Algo2, VWAP3_DBT_LMS}

import scala.collection.mutable.ListBuffer
import scala.util.Random

case class ParamsVWAP(n: Int, r: Int, p: Int, t: Int, qa: BidsExecutable) {
  assert(r <= p * t, s"PT $r > P $p * T $t")
  assert(r <= n, s"PT $r > N $n")
  assert(r >= p, s"PT $r < P $p")
  assert(r >= t, s"PT $r < T $t")
  assert(!(qa equals VWAP3Algo2) || p + t + t <= 32, s"PTT $p 2($t) > 32)")

  def cost = qa.cost(1 << n, 1 << r, 1 << p, 1 << t)

  override def toString = s"${qa.query},${qa.algo},$n,$r,$p,$t"
}

object ParamsVWAP {
  val qas: List[BidsExecutable] = List(VWAP1_DBT_LMS, VWAP1Algo1, VWAP1Algo2, VWAP2_DBT_LMS, VWAP2Algo1, VWAP2Algo2, VWAP3_DBT_LMS, VWAP3Algo1, VWAP3Algo2)

  def genExpPT(p: Int, t: Int, ns: Seq[Int]) = {
    ns.flatMap { n =>
      qas.flatMap { qa =>
        val r = n
        make(n, r, p, t, qa)
      }
    }
  }

  def genExpNRT(n: Int, r: Int, t: Int, ps: Seq[Int]) = {
    ps.flatMap { p =>
      qas.flatMap { qa =>
        make(n, r, p, t, qa)
      }
    }
  }

  def genExpRPT(r: Int, p: Int, t: Int, ns: Seq[Int]) = {
    ns.flatMap { n =>
      qas.flatMap { qa =>
        make(n, r, p, t, qa)
      }
    }
  }

  def genExpNRP(n: Int, r: Int, p: Int, ts: Seq[Int]) = {
    ts.flatMap { t =>
      qas.flatMap { qa =>
        make(n, r, p, t, qa)
      }
    }
  }

  def generate = {
    val ps = new ListBuffer[ParamsVWAP]()

    ps ++= genExpPT(15, 7, 15 until 23)
    ps ++= genExpNRT(22, 17, 7, 10 until 18)
    ps ++= genExpNPT(22, 15, 7, 15 until 23)
    ps ++= genExpNRP(22, 17, 8, 9 until 13)

    ps.distinct
  }

  def genExpNPT(n: Int, p: Int, t: Int, rs: Seq[Int]) = {
    rs.flatMap { r =>
      qas.flatMap { qa =>
        make(n, r, p, t, qa)
      }
    }
  }

  def generateGrid() = {
    val ps = new ListBuffer[ParamsVWAP]()
    val t = 9
    ps ++= qas.flatMap(qa =>
      (5 to 9).flatMap(n =>
        (2 to n).flatMap(p =>
          (5 to 9).flatMap(r =>
            make((2 * n), (r * 2), (p * 2), t, qa)))))

    ps.distinct.toList
  }

  def make(n: Int, r: Int, p: Int, t: Int, qa: BidsExecutable): Option[ParamsVWAP] = {
    var res: Option[ParamsVWAP] = None

    def check(c: Boolean, msg: String, rest: => Unit): Unit = {
      if (c)
        rest
      else {
        ()
        println(msg)
        //rest
        ()
      }
    }

    val (c1, m1) = (r <= p * t, s"PT $r > P $p * T $t")
    val (c2, m2) = (r <= n, s"PT $r > N $n")
    val (c3, m3) = (r >= p, s"PT $r < P $p")
    val (c4, m4) = (r >= t, s"PT $r < T $t")
    val (c5, m5) = (true, "") //(qa != VWAP3Algo2 || p + 2* t <= 30), s"PTT $p ($t)^2 > 2^30)")
    val (c6, m6) = (true, "") //(qa.algo != DBT_LMS || r <= 17), s"R $r > 2^17)")

    def f: Unit = {
      res = Some(ParamsVWAP(n, r, p, t, qa))
      ()
    }

    check(c1, m1, check(c2, m2, check(c3, m3, check(c4, m4, check(c5, m5, check(c6, m6, f))))))
    res
  }

  def main(args: Array[String]): Unit = {

    def f1(p: ParamsVWAP) = p.p ==  8 && p.t ==  9 && p.n == p.r && p.qa == VWAP3Algo2
    //increase p and t  p*t is atleast 2^20  10 10 and 5 15 and  15 5

    def f2(p: ParamsVWAP) = p.t ==  9 && p.n ==  14 && p.r ==  14 && p.qa == VWAP3Algo2
    //increase t.. t = 10, p = 10 to 20   two case n=r=20, r=20, n=27


    def f3(p: ParamsVWAP) = p.p == 8 && p.t ==  9 && p.n ==  18 && p.qa == VWAP3Algo2
    //also repeat for diff comb of p,t?

    def f4(p: ParamsVWAP) = p.p ==  8 && p.t ==  9 && p.r == 14 && p.qa == VWAP3Algo2
    //


    val l1 = genExpPT(15, 7, 15 until 23)
    println("\n\nL1")
    //l1.foreach(println)

    val l2 = genExpNRT(22, 17, 7, 10 until 18)
    println("\n\nL2")
    //l2.foreach(println)

    val l3 = genExpNPT(22, 15, 7, 15 until 23)
    println("\n\nL3")
    //l3.foreach(println)

    val l4 = genExpNRP(22, 17, 8, 9 until 13)
    println("\n\nL4")
    //l4.foreach(println)

    println((l1.size, l2.size, l3.size, l4.size))
    val lall = generate
    println("ALL SIZE = " + lall.size)

    import Executor.split
    split(lall, 20).foreach(p => println(p.takeRight(3).map(_.cost / 60000).mkString(" ")))

  }

}