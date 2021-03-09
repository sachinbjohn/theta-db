package datagen

import ds.{Row, Table}
import utils.Helper._
import utils.LessThan

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

}
