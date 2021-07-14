package queries.sql

import sqlgen.Generator.{SourceExpr, generateAll}
import sqlgen.SQL._
import sqlgen.TypeDouble
import utils.{ComparatorOp, GreaterThan, LessThan}

import java.io.PrintStream

object MB5 {
  def main(args: Array[String]) {
    val file = new PrintStream("postgres/sql/MB5/gen.sql")
    var keysGby = Map[Int, SourceExpr]()
    var innerEqkeys = Map[Int, SourceExpr]()
    var outerEqKeys = innerEqkeys
    var innerIneqKeys = Map[Int, SourceExpr](1 -> (s => Field("time", s)), 2 -> (s => Field("time", s)))
    var ineqTheta = Map[Int, ComparatorOp[Double]](1 -> LessThan, 2 -> GreaterThan)
    var outerIneqKeys = Map[Int, SourceExpr](1 -> (s => Field("time", s)), 2 -> (s => Sub(Field("time", s), Const("5", TypeDouble))))
    var value: SourceExpr = (s => Field("agg", s))
    var opagg = OpSum
    var outer_name = "bids"
    var inner_name = "aggbids"
    var ds_name = "b2"
    file.println(generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name))
    file.close()
  }
}
