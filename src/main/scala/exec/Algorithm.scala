package exec

class Algorithm(name: String) {
  override def toString = name
}

object Naive extends Algorithm("Naive,Scala")
object DBT extends Algorithm("DBT,Scala")
object DBT_LMS extends Algorithm("DBT,Scala")
object Inner extends Algorithm("Range,Scala")
object Merge extends Algorithm("Merge,Scala")
