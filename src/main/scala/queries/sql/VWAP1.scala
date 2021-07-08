package queries.sql

import sqlgen.Generator.{SourceExpr, generateAll}
import sqlgen.SQL.{Field, OpSum}
import utils.LessThan

import java.io.PrintStream

object VWAP1 {
  def main(args: Array[String]) {
    val file = new PrintStream("postgres/queries/VWAP1/vwap1gen.sql")
    var keysGby = Map[Int, SourceExpr]()
    var innerEqkeys = Map[Int, SourceExpr]()
    var outerEqKeys = Map[Int, SourceExpr]()
    var innerIneqKeys = Map[Int, SourceExpr](1 -> (s => Field("price", s)))
    var ineqTheta = Map(1 -> LessThan)
    var outerIneqKeys = Map[Int, SourceExpr](1 -> (s => Field("price", s)))
    var value: SourceExpr = (s => Field("volume", s))
    var opagg = OpSum
    var outer_name = "aggbids"
    var inner_name = "aggbids"
    var ds_name = "b2"
    file.println(generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name))
    file.close()
  }
}
