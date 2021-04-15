package datagen

import ds.{Row, Table}
import utils.Helper._
import utils.LessThan

import java.io.PrintStream
import scala.util.Random

object Bids {
  def generate(total: Int, price: Int, time: Int, pricetime: Int) = {
    Random.setSeed(0)


    //assert(PTsize < total)
    val allPT = Random.shuffle((0 until price * time).toList).take(pricetime).toArray
    val rows = (0 until total).map { i =>
      val vol = Random.nextInt(100)
      val pt = allPT(Random.nextInt(pricetime))
      val p = pt/time
      val t = pt % time
      Row(Array(p,t,vol))
    }
    rows
  }
 def main(args: Array[String]): Unit = {
   val all = 17
   val total = all
   val price = all
   val time = 1
   val pricetime = all
   val file = new PrintStream(s"csvdata/bids_${total}_${pricetime}_${price}_${time}.csv")
   val cols = List("Price", "Time", "Volume")
   file.println(cols.mkString(","))
   val data = generate(1 << total, 1 << price, 1 << time, 1<<pricetime).map(r => List(r(0), r(1), r(2)))
   data.foreach(r => file.println(r.mkString(",")))
   file.close()
 }
}
