package exec

import datagen.Bids
import ds.Table

import java.io.{FileOutputStream, PrintStream}

trait VWAPExecutable {
  def execute(bids: Table): Long

  def query: String

  def algo: Algorithm
}

class Executor(val id: Int, val folder:String) {
  var numRuns = 3

  val out = new PrintStream(s"$folder/output$id.csv")

  def benchAll(ps: Seq[ParamsVWAP]) = {
    ps.foreach(bench)
  }

  def bench(p: ParamsVWAP) = {
    val bids = new Table("Bids", Bids.generate(p.n, p.p, p.t, p.r))
    (1 to numRuns).foreach { i =>
      val t = p.qa.execute(bids)
      out.println(s"$p,$t")
    }
  }
}

object Executor {
  def main(args: Array[String]) = {
    val id = args(0).toInt-1
    val total = args(1).toInt
    val params = ParamsVWAP.generate(total)
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

object DummyExecutor{
  def main(args: Array[String]) = {
    val id = args(0).toInt -1
    val total = args(1).toInt
    val folder = args(2)
    val ex = new DummyExecutor(id, folder)
    ex.benchAll()
  }
}