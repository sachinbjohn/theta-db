package utils

import ds.{Domain, Row}

import javax.xml.crypto.dsig.keyinfo.KeyValue

abstract class ComparatorOp[T: Ordering] {
  def reverse(): ComparatorOp[T]

  def complement(): ComparatorOp[T]

  def withEq: ComparatorOp[T]

  def apply(a: T, b: T): Boolean
}

case class LessThan[T: Ordering]() extends ComparatorOp[T] {

  override def reverse() = GreaterThan()

  override def complement() = GreaterThanEqual()

  override def toString: String = "<"

  override def withEq: ComparatorOp[T] = LessThanEqual[T]

  override def apply(a: T, b: T) = implicitly[Ordering[T]].lt(a, b)

}

case class LessThanEqual[T: Ordering]() extends ComparatorOp[T] {

  override def reverse() = GreaterThanEqual[T]()

  override def complement() = GreaterThan()

  override def toString: String = "<="

  override def apply(a: T, b: T) = implicitly[Ordering[T]].lteq(a, b)

  override def withEq: ComparatorOp[T] = LessThanEqual[T]
}

case class GreaterThan[T: Ordering]() extends ComparatorOp[T] {
  override def toString: String = ">"

  override def reverse() = LessThan()

  override def complement() = LessThanEqual()

  override def apply(a: T, b: T) = implicitly[Ordering[T]].gt(a, b)

  override def withEq: ComparatorOp[T] = GreaterThanEqual[T]
}

case class GreaterThanEqual[T: Ordering]() extends ComparatorOp[T] {
  override def toString: String = ">="

  override def reverse() = LessThanEqual()

  override def complement() = LessThan()

  override def apply(a: T, b: T) = implicitly[Ordering[T]].gteq(a, b)

  override def withEq: ComparatorOp[T] = GreaterThanEqual[T]
}

case class NotEqualTo[T: Ordering]() extends ComparatorOp[T] {
  override def reverse() = NotEqualTo()

  override def complement() = EqualTo()

  override def toString: String = "!="

  override def apply(a: T, b: T) = !a.equals(b)

  override def withEq: ComparatorOp[T] = EqualTo[T]
}


case class EqualTo[T: Ordering]() extends ComparatorOp[T] {

  override def reverse() = EqualTo()

  override def complement() = NotEqualTo()

  override def toString: String = "=="

  override def apply(a: T, b: T) = a.equals(b)

  override def withEq: ComparatorOp[T] = EqualTo[T]
}


object Helper {

  implicit class DoubleComparisons(op: ComparatorOp[Double]) {
    def first = op match {
      case GreaterThanEqual() => Double.PositiveInfinity
      case GreaterThan() => Double.PositiveInfinity
      case _ => Double.NegativeInfinity
    }

    def last = op match {
      case GreaterThanEqual() => Double.NegativeInfinity
      case GreaterThan() => Double.NegativeInfinity
      case _ => Double.PositiveInfinity
    }
  }

  def sortingOther(domains: Array[Domain], keyVector: Array[Int], op: List[ComparatorOp[Double]]): Ordering[Row] = {
    new Ordering[Row] {
      override def compare(r1: Row, r2: Row): Int = {

        val k1 = keyVector.zip(domains.zip(op)).map{case (i, (d, o))  => d.findPredEq(r1(i), o)}
        val k2 =  keyVector.zip(domains.zip(op)).map{case (i, (d, o))  => d.findPredEq(r2(i), o)}
        val (af, bf) = k1.zip(k2).zip(op).foldLeft((true, false))({ case ((a, b), ((x, y), o)) =>
          (a && (x == y), b || (a && o.withEq(x, y)))
        })
        if (af) 0 else if (bf) -1 else 1
      }
    }
  }

  def sorting(keyVector: Array[Int], op: List[ComparatorOp[Double]]): Ordering[Row] = {
    new Ordering[Row] {
      override def compare(r1: Row, r2: Row): Int = {
        val k1 = keyVector.map(i => r1(i))
        val k2 = keyVector.map(i => r2(i))
        val (af, bf) = k1.zip(k2).zip(op).foldLeft((true, false))({ case ((a, b), ((x, y), o)) =>
          (a && (x == y), b || (a && o.withEq(x, y)))
        })
        if (af) 0 else if (bf) -1 else 1
      }
    }
  }
}