package queries.sql

import sqlgen.Generator.{SourceExpr, generateAll}
import sqlgen.SQL.{Field, OpSum}
import utils.{ComparatorOp, LessThan}

import java.io.PrintStream

object MB2 {
  def main(args: Array[String]) {
    val file = new PrintStream("postgres/sql/MB2/gen.sql")
    var keysGby = Map[Int, SourceExpr]()
    var innerEqkeys = Map[Int, SourceExpr]()
    var outerEqKeys = innerEqkeys
    var innerIneqKeys = Map[Int, SourceExpr](1 -> (s => Field("time", s)))
    var ineqTheta = Map[Int, ComparatorOp[Double]](1 -> LessThan)
    var outerIneqKeys = innerIneqKeys
    var value: SourceExpr = (s => Field("agg", s))
    var opagg = OpSum
    var outer_name = "bids"
    var inner_name = "aggbids"
    var ds_name = "b2"
    file.println(generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name))
    file.close()
  }
}
