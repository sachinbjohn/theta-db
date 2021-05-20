package utils

import ds.{Domain, Row}

import javax.xml.crypto.dsig.keyinfo.KeyValue

abstract class ComparatorOp[T: Ordering] {
  def reverse(): ComparatorOp[T]

  def sorting(): ComparatorOp[T]

  def complement(): ComparatorOp[T]

  def withEq: ComparatorOp[T]

  def apply(a: T, b: T): Boolean
}

object LessThan extends ComparatorOp[Double] {

  override def reverse() = GreaterThan

  override def sorting() = LessThan

  override def complement() = GreaterThanEqual

  override def toString: String = "<"

  override def withEq = LessThanEqual

  override def apply(a: Double, b: Double) = a < b

}

object LessThanEqual extends ComparatorOp[Double] {

  override def reverse() = GreaterThanEqual

  override def sorting() = LessThan

  override def apply(a: Double, b: Double): Boolean = a <= b

  override def complement() = GreaterThan

  override def toString: String = "<="

  override def withEq = this
}

object GreaterThan extends ComparatorOp[Double] {
  override def toString: String = ">"

  override def reverse() = LessThan

  override def complement() = LessThanEqual

  override def apply(a: Double, b: Double) = a > b

  override def withEq = GreaterThanEqual

  override def sorting() = GreaterThan
}

object GreaterThanEqual extends ComparatorOp[Double] {
  override def toString: String = ">="

  override def reverse() = LessThanEqual

  override def complement() = LessThan

  override def apply(a: Double, b: Double) = a >= b

  override def withEq = this

  override def sorting() = GreaterThan
}

object NotEqualTo extends ComparatorOp[Double] {
  override def reverse() = NotEqualTo

  override def complement() = EqualTo

  override def toString: String = "!="

  override def apply(a: Double, b: Double) = !a.equals(b)

  override def withEq = EqualTo

  override def sorting() = LessThan
}


object EqualTo extends ComparatorOp[Double] {

  override def reverse() = EqualTo

  override def complement() = NotEqualTo

  override def toString: String = "=="

  override def apply(a: Double, b: Double) = a.equals(b)

  override def withEq = this

  override def sorting() = LessThan
}


object Helper {

  implicit class DoubleComparisons(op: ComparatorOp[Double]) {
    def first = op match {
      case GreaterThanEqual => Double.PositiveInfinity
      case GreaterThan => Double.PositiveInfinity
      case _ => Double.NegativeInfinity
    }

    def last = op match {
      case GreaterThanEqual => Double.NegativeInfinity
      case GreaterThan => Double.NegativeInfinity
      case _ => Double.PositiveInfinity
    }
  }

  def sortingOther(domains: Array[Domain], keyVector: Row => Array[Double], op: Seq[ComparatorOp[Double]]): Ordering[Row] = {
    val op2 = op.map(_.sorting)
    new Ordering[Row] {
      override def compare(r1: Row, r2: Row): Int = {

        val k1 = keyVector(r1).zip(domains.zip(op)).map { case (i, (d, o)) => if (!d.sameAsOuter) d.findPredEq(i, o) else i }
        val k2 = keyVector(r2).zip(domains.zip(op)).map { case (i, (d, o)) => if (!d.sameAsOuter) d.findPredEq(i, o) else i }
        val (af, bf) = k1.zip(k2).zip(op2).foldLeft((true, false))({ case ((a, b), ((x, y), o)) =>
          (a && (x == y), b || (a && o(x, y)))
        })
        val res = if (af) 0 else if (bf) -1 else 1
        //println(s"r1=$r1 r2=$r2 k1=${k1.mkString("[", ",", "]")}  k2=${k2.mkString("[", ",", "]")} res=$res")
        res
      }
    }
  }


  def sorting(keyVector: Row => Array[Double], op: Seq[ComparatorOp[Double]]): Ordering[Row] = {

    val op2 = op.map(_.sorting)

    new Ordering[Row] {
      override def compare(r1: Row, r2: Row): Int = {
        val k1 = keyVector(r1)
        val k2 = keyVector(r2)
        val (af, bf) = k1.zip(k2).zip(op2).foldLeft((true, false))({ case ((a, b), ((x, y), o)) =>
          (a && (x == y), b || (a && o(x, y)))
        })
        if (af) 0 else if (bf) -1 else 1
      }
    }
  }
}