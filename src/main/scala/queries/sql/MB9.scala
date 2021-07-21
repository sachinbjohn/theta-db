package queries.sql

import sqlgen.Generator.{SourceExpr, generateAll}
import sqlgen.SQL.{Field, OpSum}
import utils.{ComparatorOp, GreaterThan, LessThan}

import java.io.PrintStream

object MB9 {
  def main(args: Array[String]) {
    val file = new PrintStream("postgres/sql/MB9/gen.sql")
    var keysGby = Map[Int, SourceExpr](1 -> (s => Field("time", s)))
    var innerEqkeys = Map[Int, SourceExpr]()
    var outerEqKeys = innerEqkeys
    var innerIneqKeys = Map[Int, SourceExpr]( 1 -> (s => Field("price", s)))
    var ineqTheta = Map[Int, ComparatorOp[Double]](1 -> LessThan)
    var outerIneqKeys = innerIneqKeys
    var value: SourceExpr = (s => Field("agg", s))
    var opagg = OpSum
    var outer_name = "aggbidsR"
    var inner_name = "aggbidsS"
    var ds_name = "bS"
    file.println(generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name, false, true))

    keysGby = Map()
    ineqTheta = Map[Int, ComparatorOp[Double]](1 -> GreaterThan)
    inner_name = "aggbidsR"
    outer_name = "aggbidsS"
    ds_name = "bR"
    file.println(generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name, false, true))
    file.close()
  }
}
