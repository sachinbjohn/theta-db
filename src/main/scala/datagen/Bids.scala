package datagen

import ds.{Row, Table}
import utils.Helper._
import utils.LessThan
import scala.io.Source
import java.io.PrintStream
import scala.util.Random

object Bids {
  def filename(logn: Int, logp: Int, logt: Int, logr: Int) = s"csvdata/bids_${logn}_${logr}_${logp}_${logt}.csv"

  def writeToFile(logn: Int, logp: Int, logt: Int, logr: Int): Unit = {
    val file = new PrintStream(filename(logn, logp, logt, logr))
    val cols = List("Price", "Time", "Volume")
    file.println(cols.mkString(","))
    val data = generate(logn, logp, logt, logr).map(r => List(r(0), r(1), r(2)))
    data.foreach(r => file.println(r.mkString(",")))
    file.close()

  }

  def loadFromFile(logn: Int, logp: Int, logt: Int, logr: Int) = {
    val lines = Source.fromFile(filename(logn, logp, logt, logr)).getLines()
    lines.next()
    lines.map { l =>
      val cs = l.split(",").map(_.toDouble)
      Row(cs)
    }.toList
  }

  def generate(logn: Int, logp: Int, logt: Int, logr: Int) = {
    Random.setSeed(0)

    assert(logp < 32);
    assert(logt < 32);
    assert(logr < 32);
    assert(logn < 32);

    val price = 1 << logp;
    val time = 1 << logt;
    val pricetime = 1 << logr;
    val total = 1 << logn;

    //assert(PTsize < total)
    val allP = Random.shuffle((0 until price).toList).toArray
    val allT = Random.shuffle((0 until time).toList).toArray
    val allPT = collection.mutable.ListBuffer[(Int, Int)]()

    val max = if (price < time) time else price
    (0 until max).map(i => allPT += allP(i % price) -> allT(i % time));
    (max until pricetime).map(i => allPT += allP(Random.nextInt(price)) -> allT(Random.nextInt(time)))

    val rows = collection.mutable.ListBuffer[Row]()
    rows ++= allPT.map(pt => Row(Array(pt._1, pt._2, Random.nextInt(100))))
    (pricetime until total).map { i =>
      val vol = Random.nextInt(100)
      val pt = allPT(Random.nextInt(pricetime))
      val p = pt._1
      val t = pt._2
      rows += Row(Array(p, t, vol))
    }
    rows
  }

  def main(args: Array[String]): Unit = {
    val all = args(0).toInt
    val logn = all
    val logp = all
    val logt = all
    val logr = all
    writeToFile(logn, logp, logt, logr)

  }
}
