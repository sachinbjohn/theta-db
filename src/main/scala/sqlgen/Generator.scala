package sqlgen

import SQL._
import utils._

import java.io.PrintStream

object Generator {

  type SourceExpr = Option[String] => Expr

  def generateAll(keysGby: List[(String, SourceExpr)], keysEq: List[(String, SourceExpr, SourceExpr)], keysTheta: List[(String, SourceExpr, ComparatorOp[Double], SourceExpr)], value: SourceExpr, opagg: OpAgg, outer_name: String, inner_name: String, ds_name: String) = {
    generateMerge(keysGby, keysEq, keysTheta, value, opagg, outer_name, inner_name, ds_name) + "\n\n" //+ generateRange(keysGby, keysEq, keysTheta, value, opagg, outer_name, inner_name, ds_name)
  }

  def generateMerge(keysGby: List[(String, SourceExpr)], keysEq: List[(String, SourceExpr, SourceExpr)], keysTheta: List[(String, SourceExpr, ComparatorOp[Double], SourceExpr)], value: SourceExpr, opagg: OpAgg, outer_name: String, inner_name: String, ds_name: String) = {
    val D = keysTheta.size

    val keysIneqNames = keysTheta.zipWithIndex.map { case ((ni, ei, t, eo), i) => (i + 1) -> ni }.toMap
    val keysIneqExpr_inner = keysTheta.zipWithIndex.map { case ((ni, ei, t, eo), i) => (i + 1) -> ei }.toMap
    val theta = keysTheta.zipWithIndex.map { case ((ni, ei, t, eo), i) => (i + 1) -> t }.toMap

    def isIncr(op: ComparatorOp[Double]) = op match {
      case EqualTo | LessThan | LessThanEqual => true
      case GreaterThan | GreaterThanEqual => false
    }

    val keysGbyNames = keysGby.map(_._1)
    val keysGbyExpr = keysGby.map(_._2)
    val keysEqNames = keysEq.map({ case (ni, ei, eo) => ni })
    val keysEqExpr_inner = keysEq.map({ case (ni, ei, eo) => ei })


    def domainName(n: String) = "domain_" + n

    val crossTable = "cross_" + ds_name

    def cube(i: Int) = "cube_" + ds_name + (if (i > 0) "_delta" + i else "")

    val aggCol = "agg"
    val innervar = "_inner"
    val outervar = "_outer"
    val cursorvar = "_cursor"

    val cubeDef = TableDef(cube(0), (keysEqNames ++ keysIneqNames.values ++ keysGbyNames :+ aggCol).map(_ -> TypeDouble))

    //def columnList(tbl: Option[String]) = (keysGbyNames ++ keysEqNames_inner ++ keysIneqNames_inner.values).map(k => Field(k, tbl))
    //def columnExprList(tbl: Option[String]) =  (keysGbyExpr ++ keysEqExpr_inner ++ keysIneqExpr_inner.values).map(e => e(tbl))

    def domain_select_cols(name: String, expr: SourceExpr) = (keysGby ++ keysEq.map(x => x._1 -> x._2) :+ (name -> expr)).map {
      case (n, e) => Alias(e(None), n)
    }

    def domain_orderyBy_cols(expr: SourceExpr, theta: ComparatorOp[Double]) = Some(OrderBy((keysGbyExpr ++ keysEqExpr_inner).map(ei => ei(None) -> false) :+ (expr(None) -> !isIncr(theta))))

    val constructStatements = collection.mutable.ListBuffer[Statement]()

    //Domain definitions
    keysTheta.foreach { case (ni, ei, t, eo) =>
      constructStatements += ViewDef(domainName(ni), Select(true, domain_select_cols(ni, ei),
        List(TableNamed(inner_name)), None, None, domain_orderyBy_cols(ei, t)))
    }

    /*
    def orderByAll(tbl: Option[String]) = Some(OrderBy(
      keysEqNames_inner.map(Field(_, tbl) -> false) ++
        keysTheta.map { case (ni, ei, t, eo) => Field(ni, tbl) -> !isIncr(t) } ++
        keysGbyNames.map(Field(_, tbl) -> false)
    ))*/


    //Cross definition
    val domainjoin = {
      val list = keysIneqNames.values.map { n => TableNamed(domainName(n)) }
      list.tail.foldLeft[Table](list.head)((acc, cur) => TableJoin(acc, cur, JoinInner, None))
    }
    constructStatements += ViewDef(crossTable, Select(false, List(AllCols), List(domainjoin), None, None, None))


    //First cubelet
    val crossjoin = {
      val list = keysTheta.map { case (ni, ei, t, eo) => Cmp(Field(ni, Some(crossTable)), ei(Some(inner_name)), EqualTo) }
      val cond = list.tail.foldLeft[Cond](list.head)((acc, cur) => And(acc, cur))
      List(TableJoin(TableNamed(crossTable), TableNamed(inner_name), JoinLeft, Some(cond)))
    }
    val first_cubelet_cols = keysEq.map { case (ni, ei, eo) => Alias(ei(None), ni) } ++
      keysTheta.map { case (ni, ei, t, eo) => Field(ni, None) } ++
      keysGby.map { case (ni, ei) => Alias(ei(None), ni) } :+
      Alias(Agg(value(Some(inner_name)), opagg), aggCol)
    val first_cubelet_gby = Some(GroupBy((keysEqExpr_inner ++ keysIneqExpr_inner.values ++ keysGbyExpr).map(_ (None)), None))
    val first_cubelet_orderby = Some(OrderBy(
      keysEqExpr_inner.map(_ (None) -> false) ++
        keysTheta.map { case (ni, ei, t, no) => Field(ni, None) -> !isIncr(t) } ++
        keysGbyExpr.map(_ (None) -> false)))
    constructStatements += ViewDef(cube(D), Select(false, first_cubelet_cols, crossjoin, None, first_cubelet_gby, first_cubelet_orderby))




    //Accumulation
    (1 to D).reverse.toList.map { i => // for i in D...i read cube(D) K(D) and output cube(D-1)
      val frame = theta(i) match {
        case LessThan | GreaterThan => Some(Frame(FrameRows, UnboundedPreceding, Preceding(1)))
        case _ => None
      }
      val partitionBy = PartitionBy((keysEqNames ++ keysIneqNames.filter(_._1 != i).values ++ keysGbyNames).map(Field(_, None)))
      val window = Some(Window(partitionBy, OrderBy(List(Field(keysIneqNames(i), None) -> !isIncr(theta(i)))), frame))
      val othercubelets_cols = (keysEqNames ++ keysIneqNames.values ++ keysGbyNames).map(Field(_, None)) :+
        Alias(Agg(Field(aggCol, None), opagg, window), aggCol)
      val othercubelets_orderby = Some(OrderBy(
        keysEqNames.map(Field(_, None) -> false) ++
          keysTheta.map { case (ni, ei, t, no) => Field(ni, None) -> !isIncr(t) } ++
          keysGbyNames.map(Field(_, None) -> false)))

      if (i != 1)
        constructStatements += ViewDef(cube(i - 1), Select(false, othercubelets_cols, List(TableNamed(cube(i))), None, None, othercubelets_orderby))
      else {
        constructStatements += Delete(cube(0), None)
        constructStatements += InsertInto(cube(0), Select(false, othercubelets_cols, List(TableNamed(cube(i))), None, None, othercubelets_orderby))
      }
    }

    val construct = ProcedureDef("construct_" + cube(0), Nil, Nil, constructStatements.toList)


    val mergeLookupDecl = List(VariableDecl(innervar, TypeRow(cube(0)), None))
    val condition1 = {
      val list = keysEq.map { case (ni, ei, eo) => Cmp(RowField(ni, innervar), eo(Some(outervar)), LessThan) } ++
        keysTheta.map { case (ni, ei, t, eo) => Cmp(RowField(ni, innervar), eo(Some(outervar)), NotEqualTo) }
      list.tail.foldLeft[Cond](list.head)((acc, cur) => Or(acc, cur))
    }
    val condition2 = {
      val list = keysEq.map { case (ni, ei, eo) => Cmp(RowField(ni, innervar), eo(Some(outervar)), EqualTo) } ++
        keysTheta.map { case (ni, ei, t, eo) => Cmp(RowField(ni, innervar), eo(Some(outervar)), EqualTo) }
      list.tail.foldLeft[Cond](list.head)((acc, cur) => And(acc, cur))
    }

    val zero = opagg match {
      case OpSum => Const("0", TypeDouble)
      case OpMax => Const("float8 '-infinity'", TypeDouble)
      case OpMin => Const("float8 '+infinity'", TypeDouble)
    }


    val returnval = {
      val cols = if (keysGby.isEmpty) {
        List(aggCol)
      }
      else {
        keysGbyNames :+ aggCol
      }
      MakeRow(cols.map(RowField(_, innervar)))
    }


    val mergeLookupBody = List(
      FetchCursor(cursorvar, CurCurrent, innervar),
      WhileLoop(condition1, List(
        FetchCursor(cursorvar, CurNext, innervar)
      )),
      WhileLoop(condition2, List( //Won't return anything if the equality condition doesn't match
        If(IsNotNull(RowField(aggCol, innervar)), List(ReturnNext(returnval)), Nil), //Won't return anything if aggvalue is null (inequality condition doesn't match)
        FetchCursor(cursorvar, CurNext, innervar)
      )),
      ReturnNone
    )

    val lookup = FunctionDef("lookup_" + cube(0), List(outervar -> TypeRecord, cursorvar -> TypeCursor), TypeDouble, mergeLookupDecl, mergeLookupBody)


    "--------------------- AUTO GEN MERGE ----------------------- \n" + cubeDef + "\n\n" + construct + "\n\n" + lookup
  }

  def generateVerify(tbls: List[String], algos: List[String]) = {

    def diff(t: String, a1: String, a2: String) = Except(Select(false, List(AllCols), List(TableNamed(s"${t}_result_${a1}")), None, None, None),
      Select(false, List(AllCols), List(TableNamed(s"${t}_result_${a2}")), None, None, None))

    tbls.flatMap {
      t =>
        algos.tail.flatMap {
          a =>
            List(diff(t, a, algos.head), diff(t, algos.head, a))
        }
    }.mkString(";\n\n")
  }

  def generateRange(keysGby: List[(String, SourceExpr)], keysEq: List[(String, SourceExpr, SourceExpr)], keysTheta: List[(String, SourceExpr, ComparatorOp[Double], SourceExpr)], value: SourceExpr, opagg: OpAgg, outer_name: String, inner_name: String, ds_name: String) = {
    val D = keysTheta.size

    val keysIneqNames = keysTheta.zipWithIndex.map { case ((ni, ei, t, eo), i) => (i + 1) -> ni }.toMap
    val keysIneqExpr_inner = keysTheta.zipWithIndex.map { case ((ni, ei, t, eo), i) => (i + 1) -> ei }.toMap
    val keysIneqExpr_outer = keysTheta.zipWithIndex.map { case ((ni, ei, t, eo), i) => (i + 1) -> eo }.toMap
    val theta = keysTheta.zipWithIndex.map { case ((ni, ei, t, eo), i) => (i + 1) -> t }.toMap

    val keysGbyNames = keysGby.map(_._1)
    val keysGbyExpr = keysGby.map(_._2)
    val keysEqNames = keysEq.map({ case (ni, ei, eo) => ni })
    val keysEqExpr_inner = keysEq.map({ case (ni, ei, eo) => ei })
    val keysEqExpr_outer = keysEq.map({ case (ni, ei, eo) => eo })


    val rt = "rt_" + ds_name

    val rtnew = rt + "_new"

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
    val aggtype = rt + "_aggtype"
    val gbyvar = "_groupvar"

    val outervar = "_outer"

    def columnsForRT = (keysGbyNames ++ keysEqNames).map(_ -> TypeDouble) ++
      (1 to D).toList.flatMap(j => List(lvl(j) -> TypeInt, rnk(j) -> TypeInt, lower(j) -> TypeDouble, upper(j) -> TypeDouble)) :+
      aggcol -> TypeDouble

    //scema for RTi : (lvl, rnk, start, end)_j j = 0 to i
    val RTdef = TableDef(rt, columnsForRT)
    val RTnewdef = TableDef(rtnew, columnsForRT)
    val indexDefs = (1 to D).toList.map { i =>
      val (f1, f2) = theta(i) match {
        case LessThan | LessThanEqual => (upper(i), lower(i))
        case GreaterThan | GreaterThanEqual => (lower(i), upper(i))
      }
      val lvls = (1 to i).toList.map(j => lvl(j))
      val uls = (1 until i).toList.flatMap {
        j => List(lower(j), upper(j))
      } :+ f1
      val incl = List(f2) ++ (if (i == D) keysGbyNames :+ aggcol else Nil)
      IndexDef(rt + "_idx" + i, false, rt, keysEqNames ++ lvls ++ uls, incl)
    }

    val typeDef = TypeDef(aggtype, (keysGbyNames :+ (aggcol + ds_name)).map(_ -> TypeDouble))


    val constructStatements = collection.mutable.ListBuffer[Statement]()

    def initrank(j: Int) = {
      val pby = PartitionBy((keysEqNames ++ (1 until j).toList.map(jv => keysIneqNames(jv))).map(Field(_, None)))
      DenseRank(Window(pby, OrderBy(List(Field(keysIneqNames(j), None) -> false)), None))
    }

    val initSelect: List[Expr] = {
      keysEqExpr_inner.map(_ (None)) ++
        (1 to D).toList.flatMap(j => List[Expr](Const("0", TypeInt), initrank(j), keysEqExpr_inner(j).apply(None), keysEqExpr_inner(j)(None))) ++
        keysGbyExpr.map(_ (None)) :+
        Agg(value(None), opagg)
    }
    val initGby = Some(GroupBy(
      keysEqExpr_inner.map(_ (None)) ++
        keysEqExpr_inner.map(_ (None)) ++
        keysGbyExpr.map(_ (None))
      , None))

    constructStatements += Delete(rt, None)
    constructStatements += InsertInto(rt, Select(false, initSelect, List(TableNamed(inner_name)), None, initGby, None))


    def build(i: Int) = {

      //j > i
      def window(j: Int, withOrderBy: Boolean) = {
        val pby = PartitionBy(keysEqNames.map(Field(_, None)) ++
          (1 until i).toList.flatMap(iv => List(Field(lvl(iv), None), Field(rnk(iv), None))) ++
          List(Div(Field(rnk(i), None), Variable(bf(i)))) ++
          (i + 1 until j).toList.map(jv => Field(lower(jv), None)))
        val orderby = OrderBy(if (withOrderBy) List(Field(lower(j), None) -> false) else Nil)
        Window(pby, orderby, None)
      }

      val select: List[Expr] = {
        (keysEqNames.map(Field(_, None)) ++
          (1 until i).toList.flatMap {
            j => List(lvl(j), rnk(j), lower(j), upper(j)).map(Field(_, None))
          } ++
          List(Variable(loopvar(i)), Div(Field(rnk(i), None), Variable(bf(i))), Agg(Agg(Field(lower(i), None), OpMin), OpMin, Some(window(i, false))), Agg(Agg(Field(upper(i), None), OpMax), OpMax, Some(window(i, false)))) ++
          (i + 1 to D).toList.flatMap {
            j => List(Const("0", TypeInt), DenseRank(window(j, true)), Field(lower(j), None), Field(lower(j), None))
          } ++
          keysGbyNames.map(Field(_, None)) :+ Agg(Field(aggcol, None), opagg)
      }

      val where = Some(Cmp(Field(lvl(i), None), Sub(Variable(loopvar(i)), Const("1", TypeDouble)), EqualTo))

      val gby = {
        val exprbeforei = (1 until i).toList.flatMap {
          j => List(lvl(j), rnk(j), lower(j), upper(j)).map(Field(_, None))
        }
        val expri = Div(Field(rnk(i), None), Variable(bf(i)))
        val exprafteri = (i + 1 to D).toList.map {
          j => (Field(lower(j), None))
        }
        val allexpr = keysEqNames.map(Field(_, None)) ++ (exprbeforei :+ (expri)) ++ exprafteri ++
          keysGbyNames.map(Field(_, None))
        Some(GroupBy(allexpr, None))
      }

      ForLoop(loopvar(i), "1", height(i), false, List(
        Delete(rtnew, None),
        InsertInto(rtnew, Select(false, select, List(TableNamed(rt)), where, gby, None)),
        InsertInto(rt, Select(false, List(AllCols), List(TableNamed(rtnew)), None, None, None))
      ))
    }

    constructStatements ++= (1 to D).map(build(_))

    def zero(op2: OpAgg) = Const(op2 match {
      case OpMax => "float8 '-infinity'"
      case OpMin => "float8 '+infinity'"
      case OpSum => "0"
    }, TypeDouble)

    val construct = ProcedureDef("construct_" + rt, (1 to D).flatMap {
      i => List(height(i) -> TypeInt, bf(i) -> TypeInt)
    }.toList, Nil, constructStatements.toList)


    val lookupVar: List[VarDecl] = {
      val aggvardecl = VariableDecl(aggvar, TypeDouble, Some(zero(opagg).v))
      val ineqvardec = (1 to D).toList.flatMap(j => List(
        VariableDecl(lowermin(j), TypeDouble, Some(zero(OpMin).v)),
        VariableDecl(uppermax(j), TypeDouble, Some(zero(OpMax).v)),
        VariableDecl(rowvar(j), TypeRecord, None)
      ))
        aggvardecl :: ineqvardec
    }


    def rec(i: Int, lvls: Cond, upperlower: Cond): List[Statement] = {

      val cols = (List(lower(i), upper(i)) ++ (if (i < D - 1) Nil else List(aggcol))).map(Field(_, None))
      val field = Field(theta(i) match {
        case LessThan | LessThanEqual => upper(i)
        case GreaterThan | GreaterThanEqual => lower(i)
      }, None)
      val maincond = Cmp(field, keysIneqExpr_outer(i)(Some(outervar)), theta(i))
      val orcond = Array(Cmp(field, Variable(lowermin(i)), LessThan), Cmp(field, Variable(uppermax(i)), GreaterThan))
      val updateminmax = List(
        If(Cmp(Field(lower(i), Some(rowvar(i))), Variable(lowermin(i)), LessThan), List(Assign(Variable(lowermin(i)), Field(lower(i), Some(rowvar(i))))), Nil),
        If(Cmp(Field(upper(i), Some(rowvar(i))), Variable(uppermax(i)), GreaterThan), List(Assign(Variable(uppermax(i)), Field(upper(i), Some(rowvar(i))))), Nil)
      )

      val newlvl = Cmp(Field(lvl(i), None), Variable(loopvar(i)), EqualTo)
      val newul = And(Cmp(Field(lower(i), None), Field(lower(i), Some(rowvar(i))), EqualTo), Cmp(Field(upper(i), None), Field(upper(i), Some(rowvar(i))), EqualTo))

      val nextDim = if (i == D) {
        List(opagg match {
          case OpSum => Assign(Variable(aggvar), Add(Variable(aggvar), Field(aggcol, Some(rowvar(i)))))
          case OpMax => If(Cmp(Field(aggcol, Some(rowvar(i))), Variable(aggvar), GreaterThan), List(Assign(Variable(aggvar), Field(aggcol, Some(rowvar(i))))), Nil)
          case OpMin => If(Cmp(Field(aggcol, Some(rowvar(i))), Variable(aggvar), LessThan), List(Assign(Variable(aggvar), Field(aggcol, Some(rowvar(i))))), Nil)
        })
      } else {
        rec(i + 1, And(lvls, newlvl), And(upperlower, newul))
      }
      val processRow = If(IsNotNull(Field(lower(i), Some(rowvar(i)))), updateminmax ++ (nextDim), Nil)

      val body = orcond.toList.flatMap {
        orc =>
          val orderBy = Some(OrderBy(List(field -> (orc.op == LessThan))))
          val where = Some(And(And(And(lvls, newlvl), upperlower), And(maincond, orc)))
          val getRow = SelectInto(false, cols, Variable(rowvar(i)), List(TableNamed(rt)), where, None, orderBy, Some(1))
          List(getRow, processRow)
      }

      List(Assign(Variable(lowermin(i)), zero(OpMin)),
        Assign(Variable(uppermax(i)), zero(OpMax)),
        ForLoop(loopvar(i), height(i), "0", true, body))
    }

    val initConditions = {
      val eq = keysEq.map(col => Cmp(Field(col, None), Field(col, Some(outervar)), EqualTo))
      val gby = keysGby.map(col => Cmp(Field(col, None), Field(col, Some(gbyvar)), EqualTo))
      val both = eq ++ gby

      if (both.isEmpty) TrueCond
      both.tail.foldLeft[Cond](gby.head)((acc, cur) => And(acc, cur))
    }

    val gbyQuery = Select(true, keysGby.map(Field(_, None)), List(TableNamed(rt)), None, None, None)
    val ineqbody = rec(0, initConditions, TrueCond)

    val lookupBody = if (keysGby.isEmpty)
      ineqbody :+ Return(MakeRow(List(Variable(aggvar))))
    else {
      val retvalues = MakeRow(keysGby.map(Field(_, Some(gbyvar))) :+ Variable(aggvar))
      List(QueryForLoop(gbyvar, gbyQuery, ineqbody :+ ReturnNext(retvalues)), ReturnNone)
    }

    val lookupRetType = if (keysGby.isEmpty) {
      TypeRow(aggtype)
    } else {
      TypeSet(TypeRow(aggtype))
    }

    val lookup = FunctionDef("lookup_" + rt, outervar -> TypeRecord :: (0 until D).toList.map {
      i => height(i) -> TypeInt
    }, lookupRetType, lookupVar, lookupBody)

    "--------------------- AUTO GEN RANGE ----------------------- \n " + RTdef + "\n" + RTnewdef + indexDefs.mkString("\n", "\n", "\n") + typeDef + "\n" + construct + "\n" + lookup
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


    //file.println(generateAll(Nil, Nil, List("time" -> LessThanEqual), Mul(Const("0.25", TypeDouble), Field("volume", Some("bids"))), OpSum, "bids", "bids", "b3"))
    //file.println(generateAll(Nil, Nil, List("time" -> LessThanEqual, "price" -> LessThan), Field("volume", Some("bids")), OpSum, "bids", "bids", "b2"))


    file.println(generateAll(List("time"), Nil, List("time" -> GreaterThan, "price" -> LessThan), Const("1.0", TypeDouble), OpSum, "bids", "bids", "b2"))
    val algos = List("naive", "range", "mergeauto", "rangeauto")
    val tables = (10 to 14).toList.map(i => s"bids_${i}_${i}_${i}_10")
    file.println(generateVerify(tables, algos))
    file.close()
  }

}