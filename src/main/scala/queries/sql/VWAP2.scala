package queries.sql

import sqlgen.Generator.{SourceExpr, generateAll}
import sqlgen.SQL._
import sqlgen._
import utils.{LessThan, LessThanEqual}

import java.io.PrintStream

object VWAP2 {
  def main(args: Array[String]) {
    val file = new PrintStream("postgres/sql/VWAP2/vwap2gen.sql")
    var keysGby = Map[Int, SourceExpr]()
    var innerEqkeys = Map[Int, SourceExpr]()
    var outerEqKeys = Map[Int, SourceExpr]()
    var innerIneqKeys = Map[Int, SourceExpr](2 -> (s => Field("price", s)), 1 -> (s=> Field("time", s)))
    var ineqTheta = Map(2 -> LessThan, 1 -> LessThanEqual)
    var outerIneqKeys = innerIneqKeys
    var value: SourceExpr = (s => Field("volume", s))
    var opagg = OpSum
    var outer_name = "bids"
    var inner_name = "bids"
    var ds_name = "b2"
    file.println(generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name))

     innerIneqKeys = Map[Int, SourceExpr](1 -> (s=> Field("time", s)))
     ineqTheta = Map(1 -> LessThanEqual)
     outerIneqKeys = innerIneqKeys
     value = (s => Mul(Const("0.25", TypeDouble), Field("volume", s)))
     ds_name = "b3"
    file.println(generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name))

    file.close()
  }
}
