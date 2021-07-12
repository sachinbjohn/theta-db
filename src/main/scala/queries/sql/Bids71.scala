package queries.sql

import sqlgen.Generator.{SourceExpr, generateAll}
import sqlgen.SQL._
import sqlgen._
import utils._

import java.io.PrintStream

object Bids71 {
  def main(args: Array[String]) {
    val file = new PrintStream("postgres/sql/Bids71/gen.sql")
    var keysGby = Map[Int, SourceExpr]()
    var innerEqkeys = Map[Int, SourceExpr]()
    var outerEqKeys = Map[Int, SourceExpr]()
    var innerIneqKeys = Map[Int, SourceExpr](2 -> (s => Field("price", s)), 1 -> (s=> Field("time", s)))
    var ineqTheta = Map(1 -> LessThan, 2 -> GreaterThan)
    var outerIneqKeys = innerIneqKeys
    var value: SourceExpr = (s => Add(Const("1.0", TypeDouble), Sub(Field("price", s), Field("price", s))))  //for handling null value correctly
    var opagg = OpSum
    var outer_name = "bids"
    var inner_name = "bids"
    var ds_name = "b3"
    file.println(generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name, false))

    innerIneqKeys = Map[Int, SourceExpr](1 -> (s=> Field("time", s)))
    ineqTheta = Map(1 -> LessThan)
    outerIneqKeys = innerIneqKeys
    value = (s => Const("b3agg", TypeDouble))
    ds_name = "b23"
    inner_name = "b23"
    file.println(generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name))

    file.close()
  }
}
