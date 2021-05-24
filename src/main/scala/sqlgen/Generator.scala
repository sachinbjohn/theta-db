package sqlgen

import SQL._
import utils._

import java.io.PrintStream

object Generator {
  def main(args: Array[String]): Unit = {
    val vwap1 = collection.mutable.ListBuffer[Statement]()

    vwap1 += TableDef("bids", List("price" -> TypeDouble, "time" -> TypeDouble, "volume" -> TypeDouble))
    vwap1 += TableDef("cubeb2g0", List("price" -> TypeDouble, "agg" -> TypeDouble))
    vwap1 += TableDef("rtb2", List("l1" -> TypeInt, "px" -> TypeDouble, "py" -> TypeDouble, "v" -> TypeDouble, "r1" -> TypeInt))
    vwap1 += IndexDef("ib2py", true, "rtb2", List("l1", "py"), List("px", "v"))
    vwap1 += IndexDef("ib2px", true, "rtb2", List("l1", "px"), List("py", "v"))
    vwap1 += TableDef("rtb2new", List("l1" -> TypeInt, "px" -> TypeDouble, "py" -> TypeDouble, "v" -> TypeDouble, "r1" -> TypeInt))
    vwap1 += TableDef("b1b2", List("price" -> TypeDouble, "volume" -> TypeDouble, "aggb2" -> TypeDouble))

    val rangeLookupDecl: List[VarDecl] = List(
      NumericVariableDecl("_sum", TypeDouble, Some("0")),
      NumericVariableDecl("_xmin", TypeDouble, Some("_p")),
      NumericVariableDecl("_ymax", TypeDouble, Some("0")),
      RowVariableDecl("_row", "rtb2")
    )
    val rangeLookupBody = List(
      ForLoop("i", "_levels", "0", true, List(
        SelectInto(false, List(
          Variable("i"),
          Agg(Field("px", None), OpMin),
          Agg(Field("py", None), OpMax),
          Agg(Field("v", None), OpSum),
          Const("0", TypeDouble)), Variable("_row"), List(TableNamed("rtb2")), Some(
          And(Cmp(Field("l1", None), Variable("i"), EqualTo),
            And(Cmp(Field("px", None), Variable("_xmin"), LessThan),
              Cmp(Field("py", None), Variable("_p"), LessThan)
            ))), None, None),
        If(IsNotNull(Variable("_row")), List(
          Assign(Variable("_sum"), Add(Variable("_sum"), RowField("v", "_row"))),
          Assign(Variable("_xmin"), RowField("px", "_row")),
          If(Cmp(RowField("py", "_row"), Variable("_ymax"), GreaterThan), List(
            Assign(Variable("_ymax"), RowField("py", "_row"))), Nil)), Nil),
        SelectInto(false, List(
          Variable("i"),
          Agg(Field("px", None), OpMin),
          Agg(Field("py", None), OpMax),
          Agg(Field("v", None), OpSum),
          Const("0", TypeDouble)), Variable("_row"), List(TableNamed("rtb2")), Some(
          And(Cmp(Field("l1", None), Variable("i"), EqualTo),
            And(Cmp(Variable("_ymax"), Field("py", None), LessThan),
              Cmp(Field("py", None), Variable("_p"), LessThan)
            ))), None, None),
        If(IsNotNull(Variable("_row")), List(
          Assign(Variable("_sum"), Add(Variable("_sum"), RowField("v", "_row"))),
          Assign(Variable("_ymax"), RowField("py", "_row")),
          If(Cmp(RowField("px", "_row"), Variable("_xmin"), LessThan), List(
            Assign(Variable("_xmin"), RowField("px", "_row"))
          ), Nil)
        ), Nil)
      )),
      Return(Variable("_sum"))
    )
    vwap1 += FunctionDef("rangelookup_b2", List("_p" -> TypeDouble, "_levels" -> TypeInt), TypeDouble, rangeLookupDecl, rangeLookupBody)

    val mergeLookupDecl = List(RowVariableDecl("_inner", "cubeb2g0"))
    val mergeLookupBody = List(
      FetchCursor("_succ", CurCurrent, "_inner"),
      WhileLoop(Cmp(RowField("price", "_inner"), Variable("_outer"), LessThan), List(
        FetchCursor("_succ", CurNext, "_inner"),
        MoveCursor("_cur", CurNext)
      )),
      FetchCursor("_cur", CurCurrent, "_inner"),
      If(IsNull(RowField("agg", "_inner")), List(Return(Const("0", TypeDouble))), List(Return(RowField("agg", "_inner"))))
    )
    vwap1 += FunctionDef("mergelookup_b2", List("_outer" -> TypeDouble, "_cur" -> TypeCursor, "_succ" -> TypeCursor), TypeDouble, mergeLookupDecl, mergeLookupBody)
    val file = new PrintStream("postgres/queries/vwap1/vwap1gen.sql")

    vwap1 += ProcedureDef("querynaive", Nil, List(NumericVariableDecl("vwap1res", TypeDouble, None)), List(
      SelectInto(false, List(Agg(Mul(Field("price", Some("b1")), Field("volume", Some("b1"))), OpSum)), Variable("vwap1res"), List(TableAlias(TableNamed("bids"), "b1")), Some(
        Cmp(Nested(Select(false, List(Agg(Mul(Const("0.25", TypeDouble), Field("volume", Some("b3"))), OpSum)), List(TableAlias(TableNamed("bids"), "b3")), None, None, None)),
          Nested(Select(false, List(Agg(Field("volume", Some("b2")), OpSum)), List(TableAlias(TableNamed("bids"), "b2")), Some(Cmp(Field("price", Some("b2")), Field("price", Some("b1")), LessThan)), None, None))
          , LessThan)
      ), None, None)
    ))

    //FIXME : Change variable * to fields
    vwap1 += ProcedureDef("querymerge", Nil, List(
      CursorDecl("curb2", Some(Select(false, List(Variable("*")), List(TableNamed("cubeb2g0")), None, None, None))),
      CursorDecl("succb2", Some(Select(false, List(Variable("*")), List(TableNamed("cubeb2g0")), None, None, None))),
      NumericVariableDecl("vwap1res", TypeDouble, None)
    ), List(
      ViewDef("prices", Select(true, List(Field("price", None)), List(TableNamed("bids")), None, None, Some(OrderBy(List(Field("price", None) -> false))))),
      ViewDef("aggbids", Select(false, List(Field("price", None), Alias(Agg(Field("volume", None), OpSum), "volume")), List(TableNamed("bids")), None, Some(GroupBy(List(Field("price", None)), None)), Some(OrderBy(List(Field("price", None) -> false))))),
      ViewDef("cubeb2g1", Select(false, List(Field("price", Some("p")), Alias(Agg(Field("volume", Some("b")), OpSum), "agg")), List(TableJoin(TableAlias(TableNamed("prices"), "p"), TableAlias(TableNamed("aggbids"), "b"), JoinLeft, Some(Cmp(Field("price", Some("p")), Field("price", Some("b")), EqualTo)))), None, Some(GroupBy(List(Field("price", None)), None)), Some(OrderBy(List(Field("price", None) -> false))))),
      Delete("cubeb2g0", None),
      InsertInto("cubeb2g0", Select(false, List(Field("price", None), Alias(Agg(Field("agg", None), OpSum, Some(Window(PartitionBy(Nil), OrderBy(List(Field("price", None) -> false)), None))), "agg")),
        List(TableNamed("cubeb2g1")), None, None, Some(OrderBy(List(Field("price", None) -> false)))
      )),
      OpenCursor("curb2", None),
      OpenCursor("succb2", None),
      MoveCursor("succb2", CurNext),
      Delete("b1b2", None),
      InsertInto("b1b2", Select(false, List(Field("price", None), Field("volume", None), FunCall("mergelookup_b2", List(Field("price", None), Variable("curb2"), Variable("succb2")))), List(TableNamed("aggbids")), None, None, None)),
      CloseCursor("curb2"),
      CloseCursor("succb2"),
      SelectInto(false, List(Agg(Mul(Field("price", Some("b1b2")), Field("volume", Some("b1b2"))), OpSum)), Variable("vwap1res"), List(TableNamed("b1b2")), Some(Cmp(
        Nested(Select(false, List(Agg(Mul(Const("0.25", TypeDouble), Field("volume", Some("b3"))), OpSum)), List(TableAlias(TableNamed("bids"), "b3")), None, None, None)),
        Field("aggb2", Some("b1b2")),
        LessThan
      )), None, None)

    )
    )

    vwap1.foreach(file.println)
    file.println(generateMerge(List("price" -> LessThan), "bids", Field("volume", Some("bids")), OpSum))
    file.close()
  }

  def generateMerge(keysTheta: List[(String, ComparatorOp[Double])], relSname: String, value: Expr, agg: OpAgg) = {
    val D = keysTheta.size
    val keys = keysTheta.zipWithIndex.map { case ((k, t), i) => i -> k }.toMap
    val theta = keysTheta.zipWithIndex.map { case ((k, t), i) => i -> t }.toMap

    def isIncr(op: ComparatorOp[Double]) = op match {
      case EqualTo | LessThan | LessThanEqual => true
      case GreaterThan | GreaterThanEqual => false
    }

    def domainName(k: String) = "domain" + k

    val crossTable = "cross" + relSname

    def cube(i: Int) = s"cube${relSname}g$i"

    def columnList(tbl: Option[String]) = keysTheta.map(_._1).map(k => Field(k, tbl))


    val domainDefs = keysTheta.map { case (k, t) =>
      ViewDef(domainName(k), Select(true, List(Field(k, None)), List(TableNamed(relSname)), None, None, Some(OrderBy(List(Field(k, None) -> !isIncr(t))))))
    }
    val orderByAll = Some(OrderBy(keysTheta.map { case (k, t) => Field(k, Some(crossTable)) -> !isIncr(t) }))
    val crossDef = ViewDef(crossTable, Select(false, columnList(None), keysTheta.map { case (k, t) => TableNamed(domainName(k)) }, None, None, None))
    val joinCond: List[Cond] = columnList(Some(crossTable)).zip(columnList(Some(relSname))).map { case (c1, c2) => Cmp(c1, c2, EqualTo) }
    val gD = ViewDef(cube(D), Select(false, columnList(Some(crossTable)) ++ List(Agg(value, agg)), List(TableJoin(TableNamed(crossTable), TableNamed(relSname), JoinLeft, Some(joinCond.reduce(And(_, _))))), None, Some(GroupBy(columnList(Some(crossTable)), None)), orderByAll))
    val cubeGs = collection.mutable.ListBuffer[Statement]()
    cubeGs += gD
    (0 to D - 1).map(D - 1 - _).map { i =>
      val frame = theta(i) match {
        case LessThan | GreaterThan => Some(Frame(FrameRows, UnboundedPreceding, Preceding(1)))
        case _ => None
      }
      val partitionBy = PartitionBy(keys.flatMap { case (i2, k) => if (i == i2) None else Some(k) }.map(Field(_, None)).toList)
      if (i != 0)
        cubeGs += ViewDef(cube(i), Select(false, columnList(None) ++ List(Agg(Field("agg", None), agg, Some(Window(partitionBy, OrderBy(List(Field(keys(i), None) -> !isIncr(theta(i)))), frame)))), List(TableNamed(cube(i + 1))), None, None, orderByAll))
      else
        cubeGs += InsertInto(cube(i), Select(false, columnList(None) ++ List(Agg(Field("agg", None), agg, Some(Window(partitionBy, OrderBy(List(Field(keys(i), None) -> !isIncr(theta(i)))), frame)))), List(TableNamed(cube(i + 1))), None, None, orderByAll))
    }

    "--------------------- AUTO GEN MERGE ----------------------- \n" + domainDefs.mkString("\n\n", "\n\n", "\n\n") + crossDef  + cubeGs.mkString("\n\n","\n\n","\n\n")
  }
}
