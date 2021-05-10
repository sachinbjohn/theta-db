package exec

import datagen.Bids
import ds.Table

import java.io.{FileOutputStream, PrintStream}
import scala.util.Random

abstract class BidsExecutable {
  val onemill = 1000000
  def execute(bids: Table): Long
  def cost(n: Int, r: Int, p: Int, t: Int): Double
  def query: String
  def algo: Algorithm
}

abstract class StockExecutable {
  val onemill = 1000000
  def toYear(t: Double) = t % 100
  def toMin(t: Double) = t
  def toDay(t: Double) = t % (365 * 100)
  def execute(stocks: Table): Long
  def cost(n: Int, r: Int, p: Int, t: Int): Double = ???
  def query: String
  def algo: Algorithm
}

class Executor(val id: Int, val folder: String) {
  var numRuns = 1

  val out = new PrintStream(s"$folder/output$id.csv")

  def benchAll(ps: Seq[ParamsVWAP]) = {
    ps.foreach(bench)
  }

  def bench(p: ParamsVWAP) = {
    val bids = new Table("Bids", Bids.generate(p.n, p.r, p.p, p.t))
    (1 to numRuns).foreach { i =>
      val t = p.qa.execute(bids) / 1000000
      out.println(s"$p,$t,Ex$id,$i")
    }
  }
}

object Executor {

  def split(ps: Seq[ParamsVWAP], numExecutors: Int) = {
    val ps2 = ps.sortBy(_.cost)
    if (numExecutors > 1)
      ps2.zipWithIndex.groupBy { case (k, i) => i % numExecutors }.map(_._2.map(_._1)).toArray
    else
      Array(ps2)
  }

  def main(args: Array[String]) = {
    val id = args(0).toInt - 1
    val total = args(1).toInt
    val params = split(ParamsVWAP.generate, total)
    val folder = args(2)
    val ex = new Executor(id, folder)
    ex.benchAll(params(id))
  }
}

class DummyExecutor(val id: Int, val folder: String) {
  val out = new PrintStream(s"$folder/output$id.csv")

  def benchAll() =
    (0 to 999).foreach(i => out.println(id * 1000 + i))
}

object DummyExecutor {
  def main(args: Array[String]) = {
    val id = args(0).toInt - 1
    val total = args(1).toInt
    val folder = args(2)
    val ex = new DummyExecutor(id, folder)
    ex.benchAll()
  }
}