package queries.sql

import sqlgen.Generator.{SourceExpr, generateAll}
import sqlgen.SQL._
import sqlgen._
import utils._

import java.io.PrintStream

object Bids72 {
  def main(args: Array[String]) {
    val file = new PrintStream("postgres/sql/Bids72/gen.sql")
    var keysGby = Map[Int, SourceExpr](1 -> (s => Field("time", s)))
    var innerEqkeys = Map[Int, SourceExpr]()
    var outerEqKeys = Map[Int, SourceExpr]()
    var innerIneqKeys = Map[Int, SourceExpr](1 -> (s => Field("price", s)))
    var ineqTheta = Map(1 -> LessThan)
    var outerIneqKeys = innerIneqKeys
    var value: SourceExpr = (s => Add(Const("1.0", TypeDouble), Sub(Field("price", s), Field("price", s))))  //for handling null value correctly
    var opagg = OpSum
    var outer_name = "bids"
    var inner_name = "bids"
    var ds_name = "b2"
    file.println(generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name, false))
    keysGby = Map()
    innerIneqKeys = Map[Int, SourceExpr](1 -> (s=> Field("b2time", s)))
    ineqTheta = Map(1 -> LessThan)
    outerIneqKeys = Map[Int, SourceExpr](1 -> (s => Field("time", s)))
    value = (s => Const("b2agg", TypeDouble))
    ds_name = "b32"
    inner_name = "b32"
    file.println(generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name))

    file.close()
  }
}
