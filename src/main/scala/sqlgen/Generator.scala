package sqlgen

import SQL._
import utils._

import java.io.PrintStream

object Generator {

  def generateAll(keysTheta: List[(String, ComparatorOp[Double])], relSname: String, alias: String, value: Expr, opagg: OpAgg) = {
    generateMerge(keysTheta, relSname, alias, value, opagg) + "\n\n" + generateRange(keysTheta, relSname, alias, value, opagg)
  }

  def generateMerge(keysTheta: List[(String, ComparatorOp[Double])], relSname: String, alias: String, value: Expr, opagg: OpAgg) = {
    val D = keysTheta.size
    val keys = keysTheta.zipWithIndex.map { case ((k, t), i) => i -> k }.toMap
    val theta = keysTheta.zipWithIndex.map { case ((k, t), i) => i -> t }.toMap

    def isIncr(op: ComparatorOp[Double]) = op match {
      case EqualTo | LessThan | LessThanEqual => true
      case GreaterThan | GreaterThanEqual => false
    }

    def domainName(k: String) = "domain_" + k

    val crossTable = "cross_" + relSname + alias

    def cube(i: Int) = "cube_" + relSname + alias + "_g" + i

    val cubeDef = TableDef(cube(0), keysTheta.map(_._1 -> TypeDouble).:+("agg" -> TypeDouble))

    def columnList(tbl: Option[String]) = keysTheta.map(_._1).map(k => Field(k, tbl))

    val constructStatements = collection.mutable.ListBuffer[Statement]()
    keysTheta.foreach { case (k, t) =>
      constructStatements += ViewDef(domainName(k), Select(true, List(Field(k, None)), List(TableNamed(relSname)), None, None, Some(OrderBy(List(Field(k, None) -> !isIncr(t))))))
    }

    def orderByAll(tbl: Option[String]) = Some(OrderBy(keysTheta.map { case (k, t) => Field(k, tbl) -> !isIncr(t) }))

    constructStatements += ViewDef(crossTable, Select(false, columnList(None), keysTheta.map { case (k, t) => TableNamed(domainName(k)) }, None, None, None))
    val joinCond: List[Cond] = columnList(Some(crossTable)).zip(columnList(Some(relSname))).map { case (c1, c2) => Cmp(c1, c2, EqualTo) }
    constructStatements += ViewDef(cube(D), Select(false, columnList(Some(crossTable)) ++ List(Alias(Agg(value, opagg), "agg")), List(TableJoin(TableNamed(crossTable), TableNamed(relSname), JoinLeft, Some(joinCond.reduce(And(_, _))))), None, Some(GroupBy(columnList(Some(crossTable)), None)), orderByAll(Some(crossTable))))

    (0 to D - 1).map(D - 1 - _).map { i =>
      val frame = theta(i) match {
        case LessThan | GreaterThan => Some(Frame(FrameRows, UnboundedPreceding, Preceding(1)))
        case _ => None
      }
      val partitionBy = PartitionBy(keys.flatMap { case (i2, k) => if (i == i2) None else Some(k) }.map(Field(_, None)).toList)
      if (i != 0)
        constructStatements += ViewDef(cube(i), Select(false, columnList(None) ++ List(Alias(Agg(Field("agg", None), opagg, Some(Window(partitionBy, OrderBy(List(Field(keys(i), None) -> !isIncr(theta(i)))), frame))), "agg")), List(TableNamed(cube(i + 1))), None, None, orderByAll(None)))
      else {
        constructStatements += Delete(cube(i), None)
        constructStatements += InsertInto(cube(i), Select(false, columnList(None) ++ List(Alias(Agg(Field("agg", None), opagg, Some(Window(partitionBy, OrderBy(List(Field(keys(i), None) -> !isIncr(theta(i)))), frame))), "agg")), List(TableNamed(cube(i + 1))), None, None, orderByAll(None)))
      }
    }

    val construct = ProcedureDef("construct_" + cube(0), Nil, Nil, constructStatements.toList)

    def orderingTheta(t: ComparatorOp[Double]) = t match {
      case LessThan | LessThanEqual => LessThan
      case GreaterThan | GreaterThanEqual => GreaterThan
    }

    val mergeLookupDecl = List(VariableDecl("_innercur", TypeRow(cube(0)), None), VariableDecl("_innersucc", TypeRow(cube(0)), None))
    val (k1, t1) = keysTheta.head
    val c1 = Cmp(Field(k1, Some("_innersucc")), Field(k1, Some("_outer")), orderingTheta(t1))
    val c2 = Cmp(Field(k1, Some("_innersucc")), Field(k1, Some("_outer")), EqualTo)
    val condition = keysTheta.tail.foldLeft[(Cond, Cond)]((c1, c2)) { case ((l1, l2), (k, t)) =>
      val theta2 = orderingTheta(t)
      (Or(c1, And(c2, Cmp(Field(k, Some("_innersucc")), Field(k, Some("_outer")), theta2))),
        And(c2, Cmp(Field(k, Some("_innersucc")), Field(k, Some("_outer")), EqualTo)))
    }
    val zero = opagg match {
      case OpSum => Const("0", TypeDouble)
      case OpMax => Const("float8 '-infinity'", TypeDouble)
      case OpMin => Const("float8 '+infinity'", TypeDouble)
    }
    val mergeLookupBody = List(
      FetchCursor("_succ", CurCurrent, "_innersucc"),
      WhileLoop(Or(condition._1, condition._2), List(
        FetchCursor("_succ", CurNext, "_innersucc"),
        MoveCursor("_cur", CurNext)
      )),
      FetchCursor("_cur", CurCurrent, "_innercur"),
      If(IsNull(RowField("agg", "_innercur")), List(Return(zero)), List(Return(RowField("agg", "_innercur"))))
    )
    val lookup = FunctionDef("lookup_" + cube(0), List("_outer" -> TypeRecord, "_cur" -> TypeCursor, "_succ" -> TypeCursor), TypeDouble, mergeLookupDecl, mergeLookupBody)


    "--------------------- AUTO GEN MERGE ----------------------- \n" + cubeDef + "\n\n" + construct + "\n\n" + lookup
  }

  def generateVerify(tbls: List[String], algos: List[String]) = {

    def diff(t: String, a1: String, a2: String) = Except(Select(false, List(AllCols), List(TableNamed(s"${t}_result_${a1}")), None, None, None),
      Select(false, List(AllCols), List(TableNamed(s"${t}_result_${a2}")), None, None, None))

    tbls.flatMap { t =>
      algos.tail.flatMap { a =>
        List(diff(t, a, algos.head), diff(t, algos.head, a))
      }
    }.mkString(";\n\n")
  }

  def generateRange(keysTheta: List[(String, ComparatorOp[Double])], relSname: String, alias: String, value: Expr, opagg: OpAgg) = {
    val D = keysTheta.size
    val keys = keysTheta.zipWithIndex.map { case ((k, t), i) => i -> k }.toMap
    val theta = keysTheta.zipWithIndex.map { case ((k, t), i) => i -> t }.toMap

    def rt = "rt_" + relSname + alias

    def rtnew = rt + "_new"

    def rnk(i: Int) = "rnk" + i

    def lvl(i: Int) = "lvl" + i

    def lower(i: Int) = "lower" + i

    def lowermin(i: Int) = lower(i) + "_min"

    def upper(i: Int) = "upper" + i

    def uppermax(i: Int) = upper(i) + "_max"

    def rowvar(i: Int) = "row" + i

    def height(i: Int) = "height_d" + i

    def bf(i: Int) = "bf_d" + i

    def loopvar(i: Int) = "v" + i

    val aggcol = "agg"
    val aggvar = "_agg"

    def columnsForRT = ((0 until D).toList.flatMap(j => List(lvl(j) -> TypeInt, rnk(j) -> TypeInt, lower(j) -> TypeDouble, upper(j) -> TypeDouble)) :+ aggcol -> TypeDouble)

    //scema for RTi : (lvl, rnk, start, end)_j j = 0 to i
    val RTdef = TableDef(rt, columnsForRT)
    val RTnewdef = TableDef(rtnew, columnsForRT)
    val indexDefs = (0 until D).map { i =>
      val (f1, f2) = theta(i) match {
        case LessThan | LessThanEqual => (upper(i), lower(i))
        case GreaterThan | GreaterThanEqual => (lower(i), upper(i))
      }
      val lvls = (0 to i).toList.map { j => lvl(j) }
      val uls = (0 until i).toList.flatMap { j => List(lower(j), upper(j)) } :+ f1
      val incl = List(f2) ++ (if (i == D - 1) List(aggcol) else Nil)
      IndexDef(rt + "_idx" + i, false, rt, lvls ++ uls, incl)
    }


    val constructStatements = collection.mutable.ListBuffer[Statement]()

    def initrank(j: Int) = {
      val pby = PartitionBy((0 until j).toList.map(jv => Field(keys(jv), None)))
      DenseRank(Window(pby, OrderBy(List(Field(keys(j), None) -> false)), None))
    }

    val initSelect: List[Expr] = (0 until D).toList.flatMap(j => List(Const("0", TypeInt), initrank(j), Field(keys(j), None), Field(keys(j), None))) :+ Agg(value, opagg)
    constructStatements += Delete(rt, None)
    constructStatements += InsertInto(rt, Select(false, initSelect, List(TableNamed(relSname)), None, Some(GroupBy(keys.values.toList.map(Field(_, None)), None)), None))


    def build(i: Int) = {

      //j > i
      def window(j: Int, withOrderBy: Boolean) = {
        val pby = PartitionBy((0 until i).toList.flatMap(iv => List(Field(lvl(iv), None), Field(rnk(iv), None))) ++ List(Div(Field(rnk(i), None), Variable(bf(i)))) ++ (i + 1 until j).toList.map(jv => Field(lower(jv), None)))
        val orderby = OrderBy(if (withOrderBy) List(Field(lower(j), None) -> false) else Nil)
        Window(pby, orderby, None)
      }

      val select: List[Expr] = {

        (0 until i).toList.flatMap { j => List(lvl(j), rnk(j), lower(j), upper(j)).map(Field(_, None)) } ++
          List(Variable(loopvar(i)), Div(Field(rnk(i), None), Variable(bf(i))), Agg(Agg(Field(lower(i), None), OpMin), OpMin, Some(window(i, false))), Agg(Agg(Field(upper(i), None), OpMax), OpMax, Some(window(i, false)))) ++
          (i + 1 until D).toList.flatMap { j => List(Const("0", TypeInt), DenseRank(window(j, true)), Field(lower(j), None), Field(lower(j), None)) } :+ Agg(Field(aggcol, None), opagg)
      }

      val where = Some(Cmp(Field(lvl(i), None), Sub(Variable(loopvar(i)), Const("1", TypeDouble)), EqualTo))

      val gby = {
        val exprbeforei = (0 until i).toList.flatMap { j => List(lvl(j), rnk(j), lower(j), upper(j)).map(Field(_, None)) }
        val expri = Div(Field(rnk(i), None), Variable(bf(i)))
        val exprafteri = (i + 1 until D).toList.map { j => (Field(lower(j), None)) }
        val allexpr = (exprbeforei :+ (expri)) ++ exprafteri
        Some(GroupBy(allexpr, None))
      }

      ForLoop(loopvar(i), "1", height(i), false, List(
        Delete(rtnew, None),
        InsertInto(rtnew, Select(false, select, List(TableNamed(rt)), where, gby, None)),
        InsertInto(rt, Select(false, List(AllCols), List(TableNamed(rtnew)), None, None, None))
      ))
    }

    constructStatements ++= (0 until D).map(build(_))

    def zero(op2: OpAgg) = Const(op2 match {
      case OpMax => "float8 '-infinity'"
      case OpMin => "float8 '+infinity'"
      case OpSum => "0"
    }, TypeDouble)

    val construct = ProcedureDef("construct_" + rt, (0 until D).flatMap { i => List(height(i) -> TypeInt, bf(i) -> TypeInt) }.toList, Nil, constructStatements.toList)
    val lookupVar: List[VarDecl] =
      VariableDecl(aggvar, TypeDouble, Some(zero(opagg).v)) ::
        (0 until D).toList.flatMap(j => List(
          VariableDecl(lowermin(j), TypeDouble, Some(zero(OpMin).v)),
          VariableDecl(uppermax(j), TypeDouble, Some(zero(OpMax).v)),
          VariableDecl(rowvar(j), TypeRecord, None)
        ))
    val lookupStatements = collection.mutable.ListBuffer[Statement]()


    def rec(flag: Int, i: Int, lvls: Cond, upperlower: Cond): ForLoop = {

      val cols = (List(lower(i), upper(i)) ++ (if (i < D - 1) Nil else List(aggcol))).map(Field(_, None))
      val field = Field(theta(i) match {
        case LessThan | LessThanEqual => upper(i)
        case GreaterThan | GreaterThanEqual => lower(i)
      }, None)
      val maincond = Cmp(field, Field(keys(i), Some("_outer")), theta(i))
      val orcond = Array(Cmp(field, Variable(lowermin(i)), LessThan), Cmp(field, Variable(uppermax(i)), GreaterThan))
      val updateminmax = List(
        If(Cmp(Field(lower(i), Some(rowvar(i))), Variable(lowermin(i)), LessThan), List(Assign(Variable(lowermin(i)), Field(lower(i), Some(rowvar(i))))), Nil),
        If(Cmp(Field(upper(i), Some(rowvar(i))), Variable(uppermax(i)), GreaterThan), List(Assign(Variable(uppermax(i)), Field(upper(i), Some(rowvar(i))))), Nil)
      )

      val newlvl = Cmp(Field(lvl(i), None), Variable(loopvar(i)), EqualTo)
      val newul = And(Cmp(Field(lower(i), None), Field(lower(i), Some(rowvar(i))), EqualTo), Cmp(Field(upper(i), None), Field(upper(i), Some(rowvar(i))), EqualTo))

      val nextDim = if (i == D - 1) {
        List(opagg match {
          case OpSum => Assign(Variable(aggvar), Add(Variable(aggvar), Field(aggcol, Some(rowvar(i)))))
          case OpMax => If(Cmp(Field(aggcol, Some(rowvar(i))), Variable(aggvar), GreaterThan), List(Assign(Variable(aggvar), Field(aggcol, Some(rowvar(i))))), Nil)
          case OpMin => If(Cmp(Field(aggcol, Some(rowvar(i))), Variable(aggvar), LessThan), List(Assign(Variable(aggvar), Field(aggcol, Some(rowvar(i))))), Nil)
        })
      } else {
        List(
          Assign(Variable(lowermin(i + 1)), zero(OpMin)),
          Assign(Variable(uppermax(i + 1)), zero(OpMax)),
          rec(flag, i + 1, And(lvls, newlvl), And(upperlower, newul)))
      }
      val processRow = If(IsNotNull(Field(lower(i), Some(rowvar(i)))), updateminmax ++ (nextDim), Nil)
      val orc = if ((flag & (1 << i)) == 0) orcond(0) else orcond(1)
      val body = {
        val where = Some(And(And(And(lvls, newlvl), upperlower), And(maincond, orc)))
        val getRow = SelectInto(false, cols, Variable(rowvar(i)), List(TableNamed(rt)), where, None, None, Some(1))
        List(getRow, processRow)
      }
      ForLoop(loopvar(i), height(i), "0", true, body)
    }

    lookupStatements ++= (0 until 1 << D).map(fl => rec(fl, 0, TrueCond, TrueCond))
    lookupStatements += Return(Variable("_agg"))
    val lookup = FunctionDef("lookup_" + rt, (0 until D).toList.map { i => height(i) -> TypeInt } :+ "_outer" -> TypeRecord, TypeDouble, lookupVar, lookupStatements.toList)

    "--------------------- AUTO GEN RANGE ----------------------- \n " + RTdef + "\n" + RTnewdef + indexDefs.mkString("\n", "\n", "\n") + construct + "\n" + lookup
  }

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
      VariableDecl("_sum", TypeDouble, Some("0")),
      VariableDecl("_xmin", TypeDouble, Some("_p")),
      VariableDecl("_ymax", TypeDouble, Some("0")),
      VariableDecl("_row", TypeRow("rtb2"), None)
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

    val mergeLookupDecl = List(VariableDecl("_inner", TypeRow("cubeb2g0"), None))
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

    vwap1 += ProcedureDef("querynaive", Nil, List(VariableDecl("vwap1res", TypeDouble, None)), List(
      SelectInto(false, List(Agg(Mul(Field("price", Some("b1")), Field("volume", Some("b1"))), OpSum)), Variable("vwap1res"), List(TableAlias(TableNamed("bids"), "b1")), Some(
        Cmp(Nested(Select(false, List(Agg(Mul(Const("0.25", TypeDouble), Field("volume", Some("b3"))), OpSum)), List(TableAlias(TableNamed("bids"), "b3")), None, None, None)),
          Nested(Select(false, List(Agg(Field("volume", Some("b2")), OpSum)), List(TableAlias(TableNamed("bids"), "b2")), Some(Cmp(Field("price", Some("b2")), Field("price", Some("b1")), LessThan)), None, None))
          , LessThan)
      ), None, None)
    ))

    //FIXME : Change variable * to fields
    vwap1 += ProcedureDef("querymerge", Nil, List(
      CursorDecl("curb2", Some(Select(false, List(AllCols), List(TableNamed("cubeb2g0")), None, None, None))),
      CursorDecl("succb2", Some(Select(false, List(AllCols), List(TableNamed("cubeb2g0")), None, None, None))),
      VariableDecl("vwap1res", TypeDouble, None)
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

    //vwap1.foreach(file.println)
    //file.println(generateMerge(List("price" -> LessThan), "bids", "b2", Field("volume", Some("bids")), OpSum))
    //file.println(generateRange(List("A" -> LessThan, "B" -> GreaterThan), "relt", "", Field("C", Some("relt")), OpSum))


    file.println(generateAll(List("time" -> LessThanEqual), "bids", "b3", Mul(Const("0.25", TypeDouble), Field("volume", Some("bids"))), OpSum))
    file.println(generateAll(List("time" -> LessThanEqual, "price" -> LessThan), "bids", "b2", Field("volume", Some("bids")), OpSum))


    val algos = List("naive", "mergeauto", "rangeauto")
    val tables = (10 to 14).toList.map(i => s"bids_${i}_${i}_${i}_10")
    file.println(generateVerify(tables, algos))
    file.close()
  }

}