package exec

class Algorithm(name: String) {
  override def toString = name
}

object Naive extends Algorithm("Naive")
object DBT extends Algorithm("DBT")
object DBT_LMS extends Algorithm("DBT_LMS")
object Inner extends Algorithm("Inner")
object Merge extends Algorithm("Merge")
