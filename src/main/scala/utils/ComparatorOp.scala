package utils

abstract class ComparatorOp[T:Ordering] {
  def reverse(): ComparatorOp[T]

  def complement(): ComparatorOp[T]

  def apply(a: T, b: T): Boolean
}

case class LessThan[T:Ordering]() extends ComparatorOp[T] {

  override def reverse() = GreaterThan()

  override def complement() = GreaterThanEqual()

  override def toString: String = "<"

  override def apply(a: T, b: T) = implicitly[Ordering[T]].lt(a, b)

}

case class LessThanEqual[T:Ordering]() extends ComparatorOp[T] {

  override def reverse() = GreaterThanEqual[T]()

  override def complement() = GreaterThan()

  override def toString: String = "<="

  override def apply(a: T, b: T) = implicitly[Ordering[T]].lteq(a, b)
}

case class GreaterThan[T:Ordering]() extends ComparatorOp[T] {
  override def toString: String = ">"

  override def reverse() = LessThan()

  override def complement() = LessThanEqual()

  override def apply(a: T, b: T) = implicitly[Ordering[T]].gt(a, b)
}

case class GreaterThanEqual[T:Ordering]() extends ComparatorOp[T] {
  override def toString: String = ">="

  override def reverse() = LessThanEqual()

  override def complement() = LessThan()

  override def apply(a: T, b: T) = implicitly[Ordering[T]].gteq(a, b)
}

case class NotEqualTo[T:Ordering]() extends ComparatorOp[T] {
  override def reverse() = NotEqualTo()

  override def complement() = EqualTo()

  override def toString: String = "!="

  override def apply(a: T, b: T) = !a.equals(b)
}


case class EqualTo[T:Ordering]() extends ComparatorOp[T] {

  override def reverse() = EqualTo()

  override def complement() = NotEqualTo()

  override def toString: String = "=="

  override def apply(a: T, b: T) = a.equals(b)
}

object Sorting {
  def apply(op: List[ComparatorOp[Double]]): (List[Double], List[Double]) => Boolean = {
    (x: List[Double], y: List[Double]) => {
      x.zip(y).zip(op).foldLeft((true, false))({case ((a,b), ((x,y),o)) =>
        (a && (x == y), b || (a && o(x,y)))
      })._2
    }
  }
}