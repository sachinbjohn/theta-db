package utils

trait Aggregator[T] {
  def zero: T
  def apply(v1: T, v2: T): T
  def applyN(v: T, n: Int): T = v
  //Returns new value and removed value
  def minus(v1: T, v2: T) = (v1, v1)  // SBJ: DOES NOTHING for aggregators other than Plus
}


object AggPlus extends Aggregator[Double] {
  override def apply(v1: Double, v2: Double): Double = v1 + v2
  override def zero: Double = 0
  override def applyN(v: Double, n: Int): Double = v * n
  override def minus(v1: Double, v2: Double) = (v1 - v2, v2)
}

object AggMax extends Aggregator[Double] {
  override def apply(v1: Double, v2: Double): Double = if (v1 < v2) v2 else v1
  def zero = Double.NegativeInfinity
}

object AggMin extends Aggregator[Double] {
  override def apply(v1: Double, v2: Double): Double = if (v1 < v2) v1 else v2
  def zero = Double.PositiveInfinity
}

object AggMinMax extends Aggregator[(Double, Double)] {
  override def apply(v1: (Double, Double), v2: (Double, Double)): (Double, Double) = {
    val min = if (v1._1 < v2._1) v1._1 else v2._1
    val max = if (v1._2 < v2._2) v2._2 else v1._2
    (min, max)
  }
  def zero = (Double.PositiveInfinity, Double.NegativeInfinity)

}

object TriBoolean extends Aggregator[Int] {
  override def apply(v1: Int, v2: Int): Int = if (v1 == v2) v1 else 0
  def zero = -1
}