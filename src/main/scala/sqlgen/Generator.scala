package sqlgen

import SQL._
import utils._

import java.io.PrintStream

object Generator {

  type SourceExpr = Option[String] => Expr

  def generateAll(keysGby: Map[Int, SourceExpr], innerEqkeys: Map[Int, SourceExpr], outerEqKeys: Map[Int, SourceExpr], innerIneqKeys: Map[Int, SourceExpr], ineqTheta: Map[Int, ComparatorOp[Double]], outerIneqKeys: Map[Int, SourceExpr], value: SourceExpr, opagg: OpAgg, outer_name: String, inner_name: String, ds_name: String, isOuterJoin: Boolean = true, useTempTable: Boolean = true) = {
    generateMerge(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name, isOuterJoin, useTempTable) + "\n\n" +
      generateRange(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name, isOuterJoin, useTempTable)
  }

  def generateMerge(keysGby: Map[Int, SourceExpr], innerEqkeys: Map[Int, SourceExpr], outerEqKeys: Map[Int, SourceExpr], innerIneqKeys: Map[Int, SourceExpr], ineqTheta: Map[Int, ComparatorOp[Double]], outerIneqKeys: Map[Int, SourceExpr], value: SourceExpr, opagg: OpAgg, outer_name: String, inner_name: String, ds_name: String, isOuterJoin: Boolean, useTempTable: Boolean) = {
    val D = ineqTheta.size
    val E = innerEqkeys.size
    val G = keysGby.size

    val innerTableAlias = "s"
    val outerTableAlias = "r"
    val crossTableAlias = "x"

    def keysIneqName(i: Int) = s"ineqkey$i"

    def keysEqName(i: Int) = s"eqkey$i"

    def keysGbyName(i: Int) = s"gbykey$i"

    def isIncr(op: ComparatorOp[Double]) = op match {
      case EqualTo | LessThan | LessThanEqual => true
      case GreaterThan | GreaterThanEqual => false
    }

    def domainName(i: Int) = s"domain_${ds_name}_dim$i"

    val crossTable = "cross_" + ds_name

    def cube(i: Int) = "cube_" + ds_name + (if (i > 0) "_delta" + i else "")

    def groupViewName = "groups_" + ds_name

    val aggCol = "agg"
    val innervar = "_inner"
    val outervar = "_outer"
    val cursorvar = "_cursor"
    val grpcountvar = "_grpcount"


    val constructStatements = collection.mutable.ListBuffer[Statement]()
    val defStatements = collection.mutable.ListBuffer[Statement]()
    val destroyStatements = collection.mutable.ListBuffer[Statement]()


    val cubeColDefs = {
      (1 to E).map(keysEqName) ++
        (1 to D).map(keysIneqName) ++
        (1 to G).map(keysGbyName) :+
        aggCol
    }.toList.map(_ -> TypeDouble)

    if (useTempTable)
      defStatements += TempTableDef(cube(0), cubeColDefs)
    else {
      defStatements += TableDef(cube(0), cubeColDefs)
      destroyStatements += DropTable(cube(0))
    }


    //def columnList(tbl: Option[String]) = (keysGbyNames ++ keysEqNames_inner ++ keysIneqNames_inner.values).map(k => Field(k, tbl))
    //def columnExprList(tbl: Option[String]) =  (keysGbyExpr ++ keysEqExpr_inner ++ keysIneqExpr_inner.values).map(e => e(tbl))

    def domain_select_cols(i: Int, source: Option[String]) = {
      (1 to E).map(j => Alias(innerEqkeys(j)(source), keysEqName(j))).toList ++
        List(Alias(innerIneqKeys(i)(source), keysIneqName(i))) ++
        (1 to G).map(j => Alias(keysGby(j)(source), keysGbyName(j))).toList
    }

    def domain_outer_select_cols(i: Int, sourceouter: Option[String]): List[Expr] = {
      (1 to E).map(j => Alias(outerEqKeys(j)(sourceouter), keysEqName(j))).toList ++
        List(Alias(outerIneqKeys(i)(sourceouter), keysIneqName(i))) ++
        (1 to G).map(j => Field(keysGbyName(j), None)).toList
    }

    def domain_orderyBy_cols(i: Int) = Some(OrderBy({
      (1 to E).map(j => Field(keysEqName(j), None) -> false).toList ++
        List(Field(keysIneqName(i), None) -> !isIncr(ineqTheta(i))) ++
        (1 to G).map(j => Field(keysGbyName(j), None) -> false).toList
    }))


    if (G > 0) {
      constructStatements += TempTableDefQuery(groupViewName, Select(true, {
        (1 to G).map(j => Alias(keysGby(j)(Some(inner_name)), keysGbyName(j))).toList
      }, List(TableNamed(inner_name)), None, None, None))
      constructStatements += NOP(1)
    }

    def innerdomain(i: Int) = Select(true, domain_select_cols(i, Some(innerTableAlias)),
      List(TableAlias(TableNamed(inner_name), innerTableAlias)), None, None, domain_orderyBy_cols(i))

    def outerdomain(i: Int) = Select(true, domain_outer_select_cols(i, Some(outerTableAlias)),
      {
        val outer = List(TableAlias(TableNamed(outer_name), outerTableAlias))
        if (G == 0) outer else outer :+ TableNamed(groupViewName)
      }, None, None, None)


    //Domain definitions
    constructStatements ++= (1 to D).toList.flatMap { i =>
      List(TempTableDefQuery(domainName(i), Union(outerdomain(i), innerdomain(i))), NOP(1))
    }
    constructStatements += NOP(2)
    //Cross definition
    val domainjoin = {
      val list = (1 to D).map { i => TableNamed(domainName(i)) }.toList
      list.tail.foldLeft[Table](list.head)((acc, cur) => TableJoin(acc, cur, JoinInner, None))
    }
    constructStatements += TempTableDefQuery(crossTable, Select(false, List(AllCols), List(domainjoin), None, None, None))
    constructStatements += NOP(2)

    //First cubelet
    val crossjoin = {
      val list = (1 to D).map { i => Cmp(Field(keysIneqName(i), Some(crossTableAlias)), innerIneqKeys(i)(Some(innerTableAlias)), EqualTo) } ++
        (1 to E).map(i => Cmp(Field(keysEqName(i), Some(crossTableAlias)), innerEqkeys(i)(Some(innerTableAlias)), EqualTo)) ++
        (1 to G).map(i => Cmp(Field(keysGbyName(i), Some(crossTableAlias)), keysGby(i)(Some(innerTableAlias)), EqualTo))
      val cond = list.tail.foldLeft[Cond](list.head)((acc, cur) => And(acc, cur))
      List(TableJoin(TableAlias(TableNamed(crossTable), crossTableAlias), TableAlias(TableNamed(inner_name), innerTableAlias), JoinLeft, Some(cond)))
    }

    val first_cubelet_cols = ((1 to E).map(i => keysEqName(i)) ++
      (1 to D).map(i => keysIneqName(i)) ++
      (1 to G).map(i => keysGbyName(i))).map(Field(_, Some(crossTableAlias))).toList :+
      Alias(Agg(value(Some(innerTableAlias)), opagg), aggCol)

    val first_cubelet_gby = Some(GroupBy({
      (1 to E).map(i => keysEqName(i)) ++
        (1 to D).map(i => keysIneqName(i)) ++
        (1 to G).map(i => keysGbyName(i))
    }.toList.map(Field(_, Some(crossTableAlias))), None))

    val first_cubelet_orderby = Some(OrderBy({
      (1 to E).map(i => Field(keysEqName(i), Some(crossTableAlias)) -> false).toList ++
        (1 to D).map(i => Field(keysIneqName(i), Some(crossTableAlias)) -> !isIncr(ineqTheta(i))).toList ++
        (1 to G).map(i => Field(keysGbyName(i), Some(crossTableAlias)) -> false).toList
    }))

    if (useTempTable)
      constructStatements += TempTableDefQuery(cube(D), Select(false, first_cubelet_cols, crossjoin, None, first_cubelet_gby, first_cubelet_orderby))
    else {
      constructStatements += TableDefQuery(cube(D), Select(false, first_cubelet_cols, crossjoin, None, first_cubelet_gby, first_cubelet_orderby))
      destroyStatements += DropTable(cube(D))
    }
    constructStatements += NOP(1)

    //Accumulation
    (1 to D).reverse.toList.map { i => // for i in D...i read cube(D) K(D) and output cube(D-1)
      val frame = ineqTheta(i) match {
        case LessThan | GreaterThan => Some(Frame(FrameRows, UnboundedPreceding, Preceding(1)))
        case _ => None
      }
      val partitionBy = PartitionBy({
        (1 to E).map(j => keysEqName(j)) ++
          (1 to D).filter(_ != i).map(j => keysIneqName(j)) ++
          (1 to G).map(j => keysGbyName(j))
      }.toList.map(Field(_, None)))
      val window = Some(Window(partitionBy, OrderBy(List(Field(keysIneqName(i), None) -> !isIncr(ineqTheta(i)))), frame))

      val othercubelets_cols = {
        (1 to E).map(j => keysEqName(j)) ++
          (1 to D).map(j => keysIneqName(j)) ++
          (1 to G).map(j => keysGbyName(j))
      }.toList.map(Field(_, None)) :+
        Alias(Agg(Field(aggCol, None), opagg, window), aggCol)

      val othercubelets_orderby = Some(OrderBy({
        (1 to E).map(i => Field(keysEqName(i), None) -> false).toList ++
          (1 to D).map(i => Field(keysIneqName(i), None) -> !isIncr(ineqTheta(i))).toList ++
          (1 to G).map(i => Field(keysGbyName(i), None) -> false).toList
      }))

      if (useTempTable)
        constructStatements += TempTableDefQuery(cube(i - 1), Select(false, othercubelets_cols, List(TableNamed(cube(i))), None, None, othercubelets_orderby))
      else {
        constructStatements += TableDefQuery(cube(i - 1), Select(false, othercubelets_cols, List(TableNamed(cube(i))), None, None, othercubelets_orderby))
        destroyStatements += DropTable(cube(i - 1))
      }
      constructStatements += NOP(1)
    }

    val construct = s"drop procedure if exists construct_${cube(0)};\n" +
      ProcedureDef("construct_" + cube(0), Nil, Nil, destroyStatements.reverse.toList ++ constructStatements.toList)

    //-----------------------------
    val mergeLookupDecl = List(
      VariableDecl(innervar, TypeRecord, None),
      VariableDecl(grpcountvar, TypeInt, None)
    )
    val condition = {
      val list = (1 to D).map { i => Cmp(RowField(keysIneqName(i), innervar), outerIneqKeys(i)(Some(outervar)), EqualTo) } ++
        (1 to E).map(i => Cmp(RowField(keysEqName(i), innervar), innerEqkeys(i)(Some(outervar)), EqualTo))
      list.tail.foldLeft[Cond](list.head)(And(_, _))
    }

    val zero = opagg match {
      case OpSum => Const("0", TypeDouble)
      case OpMax => Const("float8 '-infinity'", TypeDouble)
      case OpMin => Const("float8 '+infinity'", TypeDouble)
    }

    val returnval = MakeRow(((1 to G).map(i => keysGbyName(i)).toList).map(RowField(_, innervar)) :+ Cast(RowField(aggCol, innervar), TypeDouble))
    val returnStatement =
      if (isOuterJoin)
        ReturnNext(returnval)
      else If(IsNotNull(RowField(aggCol, innervar)), List(ReturnNext(returnval)), Nil)

    val mergeLookupBody = List(
      FetchCursor(cursorvar, CurCurrent, innervar),
      WhileLoop(Not(condition), List(
        FetchCursor(cursorvar, CurNext, innervar)
      )),
      Assign(Variable(grpcountvar), Const("0", TypeInt)),
      WhileLoop(condition, List( //Won't return anything if the equality condition doesn't match
        returnStatement,
        FetchCursor(cursorvar, CurNext, innervar),
        Assign(Variable(grpcountvar), Add(Variable(grpcountvar), Const("-1", TypeInt)))
      )),
      MoveCursor(cursorvar, CurRelative(Variable(grpcountvar))),
      ReturnNone
    )

    val aggtype = cube(0) + "_aggtype"
    val typeDef = s"drop type if exists $aggtype cascade; \n" +
      TypeDef(aggtype, ((1 to G).map(j => keysGbyName(j)).toList :+ (aggCol + ds_name)).map(_ -> TypeDouble))

    val lookupRetType = TypeSet(TypeRow(aggtype))
    val lookup = s"drop function if exists lookup_${cube(0)};\n" +
      FunctionDef("lookup_" + cube(0), List(outervar -> TypeRecord, cursorvar -> TypeCursor), lookupRetType, mergeLookupDecl, mergeLookupBody)

    "--------------------- AUTO GEN MERGE ----------------------- \n" +
      construct + "\n\n" + typeDef + "\n\n" + lookup
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

  def generateRange(keysGby: Map[Int, SourceExpr], innerEqkeys: Map[Int, SourceExpr], outerEqKeys: Map[Int, SourceExpr], innerIneqKeys: Map[Int, SourceExpr], ineqTheta: Map[Int, ComparatorOp[Double]], outerIneqKeys: Map[Int, SourceExpr], value: SourceExpr, opagg: OpAgg, outer_name: String, inner_name: String, ds_name: String, isOuterJoin: Boolean, useTempTable: Boolean) = {
    val D = ineqTheta.size
    val E = innerEqkeys.size
    val G = keysGby.size

    val innerTableAlias = "s"
    val outerTableAlias = "r"

    def keysEqName(i: Int) = s"eqkey$i"

    def keysGbyName(i: Int) = s"gbykey$i"


    val rt = "rt_" + ds_name

    val rtdst = rt + "_dst"
    val rtsrc = rt + "_src"

    def idxName(i: Int) = rt + "_idx" + i

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
    val gbyeqvar = rowvar(0)
    val countvar = "_count"
    val outervar = "_outer"

    def columnsForRT =
      (1 to G).toList.map(j => keysGbyName(j) -> TypeDouble) ++
        (1 to E).toList.map(j => keysEqName(j) -> TypeDouble) ++
        (1 to D).toList.flatMap(j => List(lvl(j) -> TypeInt, rnk(j) -> TypeInt, lower(j) -> TypeDouble, upper(j) -> TypeDouble)) :+
        aggcol -> TypeDouble

    val constructStatements = collection.mutable.ListBuffer[Statement]()
    val destroyStatements = collection.mutable.ListBuffer[Statement]()

    constructStatements += NOP(1)
    //scema for RTi : (lvl, rnk, start, end)_j j = 0 to i
    if (useTempTable)
      constructStatements += TempTableDef(rt, columnsForRT)
    else {
      constructStatements += TableDef(rt, columnsForRT)
      destroyStatements += DropTable(rt)
    }
    constructStatements += NOP(1)
    if (G > 0) {
      constructStatements += IndexDef(idxName(0), false, rt, (1 to G).map(keysGbyName(_)).toList, Nil)
      destroyStatements += DropIndex(idxName(0))
    }
    (1 to D).toList.foreach { i =>
      val (f1, f2) = ineqTheta(i) match {
        case LessThan | LessThanEqual => (upper(i), lower(i))
        case GreaterThan | GreaterThanEqual => (lower(i), upper(i))
      }
      val lvls = (1 to i).toList.map(j => lvl(j))
      //TODO: NO need of both lower and upper when lvl is included
      val uls = (1 until i).toList.flatMap {
        j => List(lower(j), upper(j))
      } :+ f1
      val incl = List(f2) ++ (if (i == D) List(aggcol) else Nil)
      val idxCols = {
        (1 to G).map(j => keysGbyName(j)).toList ++
          (1 to E).map(j => keysEqName(j)).toList ++
          lvls ++ uls
      }
      constructStatements += IndexDef(idxName(i), false, rt, idxCols, incl)
      destroyStatements += DropIndex(idxName(i))
    }

    constructStatements += NOP(1)

    def initrank(j: Int) = {
      val pby = PartitionBy({
        (1 to G).map(jv => keysGby(jv)(None)) ++
          (1 to E).map(jv => innerEqkeys(jv)(None)) ++
          (1 until j).toList.map(jv => innerIneqKeys(jv)(None))
      }.toList)
      val window = Window(pby, OrderBy(List(innerIneqKeys(j)(None) -> false)), None)
      DenseRank(window)
    }

    def initlvl(j: Int) = {
      val pby = PartitionBy({
        (1 to G).map(jv => Field(keysGbyName(jv), None)) ++
          (1 to E).map(jv => Field(keysEqName(jv), None)) ++
          (1 until j).toList.map(jv => Field(rnk(jv), None))
      }.toList)
      val window = Window(pby, OrderBy(Nil), None)
      Log(Add(Agg(Field(rnk(j), None), OpMax, Some(window)), Const("1", TypeInt)))
    }

    val initSelect1: List[Expr] = {
      (1 to G).map(j => Alias(keysGby(j)(None), keysGbyName(j))).toList ++
        (1 to E).toList.map(j => Alias(innerEqkeys(j)(None), keysEqName(j))).toList ++
        (1 to D).toList.flatMap(j => List[Expr](
          Alias(initrank(j), rnk(j)),
          Alias(innerIneqKeys(j)(None), lower(j)),
          Alias(innerIneqKeys(j)(None), upper(j)))) :+
        Alias(Agg(value(None), opagg), aggcol)
    }
    val initSelect2: List[Expr] = {
      (1 to G).map(j => Field(keysGbyName(j), None)).toList ++
        (1 to E).toList.map(j => Field(keysEqName(j), None)).toList ++
        (1 to D).toList.flatMap(j => List[Expr](
          initlvl(j),
          Field(rnk(j), None),
          Field(lower(j), None),
          Field(upper(j), None))) :+
        Field(aggcol, None)
    }

    val initGby = Some(GroupBy({
      (1 to G).map(j => keysGby(j)(None)) ++
        (1 to E).map(j => innerEqkeys(j)(None)) ++
        (1 to D).map(j => innerIneqKeys(j)(None))
    }.toList, None))

    //constructStatements += Delete(rt, None)
    constructStatements += InsertInto(rt, Select(false, initSelect2, List(TableAlias(TableQuery(
      Select(false, initSelect1, List(TableNamed(inner_name)), None, initGby, None)),
      "sub")), None, None, None))
    constructStatements += NOP(1)

    def build(i: Int) = {

      //j > i
      def window(j: Int, withOrderBy: Boolean) = {
        val pby = PartitionBy({
          (1 to G).map(j => Field(keysGbyName(j), None)).toList ++
            (1 to E).map(j => Field(keysEqName(j), None)).toList ++
            (1 until i).toList.flatMap(iv => List(Field(lvl(iv), None), Field(rnk(iv), None))) ++
            List(Div(Field(rnk(i), None), Variable(bf(i)))) ++
            (i + 1 until j).toList.map(jv => Field(lower(jv), None))
        })
        val orderby = OrderBy(if (withOrderBy) List(Field(lower(j), None) -> false) else Nil)
        Window(pby, orderby, None)
      }

      def newwindow(j: Int, withOrderBy: Boolean) = {
        val pby = PartitionBy({
          (1 to G).map(j => Field(keysGbyName(j), None)).toList ++
            (1 to E).map(j => Field(keysEqName(j), None)).toList ++
            (1 until i).toList.flatMap(iv => List(Field(lvl(iv), None), Field(rnk(iv), None))) ++
            List(Field(rnk(i), None)) ++
            (i + 1 until j).toList.map(jv => Field(lower(jv), None))
        })
        val orderby = OrderBy(if (withOrderBy) List(Field(lower(j), None) -> false) else Nil)
        Window(pby, orderby, None)
      }

      val select1: List[Expr] = {
        (1 to G).map(j => Field(keysGbyName(j), None)).toList ++
          (1 to E).map(j => Field(keysEqName(j), None)).toList ++
          (1 until i).toList.flatMap(j => List(lvl(j), rnk(j), lower(j), upper(j)).map(Field(_, None))) ++
          List(
            Alias(Sub(Field(lvl(i), None), Const("1", TypeInt)), lvl(i)),
            Alias(Div(Field(rnk(i), None), Variable(bf(i))), rnk(i)),
            Alias(Agg(Agg(Field(lower(i), None), OpMin), OpMin, Some(window(i, false))), lower(i)),
            Alias(Agg(Agg(Field(upper(i), None), OpMax), OpMax, Some(window(i, false))), upper(i))) ++
          (i + 1 to D).toList.flatMap(j => List(
            Alias(DenseRank(window(j, true)), rnk(j)),
            Alias(Field(lower(j), None), lower(j)),
            Alias(Field(lower(j), None), upper(j)))) :+
          Alias(Agg(Field(aggcol, None), opagg), aggcol)
      }
      val select2: List[Expr] = {
        (1 to G).map(j => Field(keysGbyName(j), None)).toList ++
          (1 to E).map(j => Field(keysEqName(j), None)).toList ++
          (1 until i).toList.flatMap(j => List(lvl(j), rnk(j), lower(j), upper(j)).map(Field(_, None))) ++
          List(
            Field(lvl(i), None),
            Field(rnk(i), None),
            Field(lower(i), None),
            Field(upper(i), None)) ++
          (i + 1 to D).toList.flatMap(j => List(
            Alias(Log(Add(Const("1", TypeInt), Agg(Field(rnk(j), None), OpMax, Some(newwindow(j, false))))), lvl(j)),
            Field(rnk(j), None),
            Field(lower(j), None),
            Field(upper(j), None))) :+
          Field(aggcol, None)
      }

      val where = Some(Cmp(Field(lvl(i), None), Const("0", TypeInt), GreaterThan))

      val gby = {
        val exprbeforei = (1 until i).toList.flatMap {
          j => List(lvl(j), rnk(j), lower(j), upper(j)).map(Field(_, None))
        }
        val expri = List(Field(lvl(i), None), Div(Field(rnk(i), None), Variable(bf(i))))
        val exprafteri = (i + 1 to D).toList.map {
          j => (Field(lower(j), None))
        }
        val allexpr = {
          (1 to G).map(j => Field(keysGbyName(j), None)) ++
            (1 to E).map(j => Field(keysEqName(j), None)) ++
            (exprbeforei ++ (expri)) ++ exprafteri
        }.toList
        Some(GroupBy(allexpr, None))
      }

      val query1 = Select(false, select1, List(TableNamed(rtsrc)), where, gby, None)
      val query2 = if (i < D)
        Select(false, select2, List(TableAlias(TableQuery(query1), "sub")), None, None, None)
      else
        query1
      List(
        TempTableDefQuery(rtsrc, Select(false, List(AllCols), List(TableNamed(rt)), None, None, None)),
        Loop(List(
          TempTableDefQuery(rtdst, query2),
          NOP(1),
          SelectInto(false, List(Const("1", TypeInt)), Variable(countvar), List(TableNamed(rtdst)), None, None, None, Some(1)),
          DropTable(rtsrc),
          Exit(Some(IsNull(Variable(countvar)))),
          NOP(1),
          InsertInto(rt, Select(false, List(AllCols), List(TableNamed(rtdst)), None, None, None)),
          NOP(1),
          RenameTable(rtdst, rtsrc)
        )),
        DropTable(rtdst))
    }

    constructStatements ++= (1 to D).flatMap(build(_))
    constructStatements += Analyze(rt)

    def zero(op2: OpAgg) = Const(op2 match {
      case OpMax => "float8 '-infinity'"
      case OpMin => "float8 '+infinity'"
      case OpSum => "0"
    }, TypeDouble)

    val construct = s"drop procedure if exists construct_$rt;\n" +
      ProcedureDef("construct_" + rt, Nil, (1 to D).toList.map(i => VariableDecl(bf(i), TypeInt, Some("2"))) :+ VariableDecl(countvar, TypeInt, None), destroyStatements.reverse.toList ++ constructStatements.toList)


    val lookupVar: List[VarDecl] = {
      val aggvardecl = VariableDecl(aggvar, TypeDouble, None)
      val row0vardecl = VariableDecl(gbyeqvar, TypeRecord, None)
      val ineqvardec = (1 to D).toList.flatMap(j => List(
        VariableDecl(lowermin(j), TypeDouble, Some(zero(OpMin).v)),
        VariableDecl(uppermax(j), TypeDouble, Some(zero(OpMax).v)),
        VariableDecl(rowvar(j), TypeRecord, None),
        VariableDecl(loopvar(j), TypeInt, None)
      ))
      aggvardecl :: row0vardecl :: ineqvardec
    }

    val initConditions = {
      val gby = (1 to G).map(j => Cmp(Field(keysGbyName(j), None), Field(keysGbyName(j), Some(gbyeqvar)), EqualTo))
      val eq = (1 to E).map(j => Cmp(Field(keysEqName(j), None), outerEqKeys(j)(Some(outervar)), EqualTo))
      (gby ++ eq).toList
    }

    def rec(i: Int, lvls: List[Cond], upperlower: List[Cond]): List[Statement] = {

      val cols = {
        List(lower(i), upper(i)) ++
          (if (i != D)
            Nil
          else
            List(aggcol))
      }.map(Field(_, None))
      val field = Field(ineqTheta(i) match {
        case LessThan | LessThanEqual => upper(i)
        case GreaterThan | GreaterThanEqual => lower(i)
      }, None)
      val maincond = Cmp(field, outerIneqKeys(i)(Some(outervar)), ineqTheta(i))
      val orcond = Array(Cmp(field, Variable(lowermin(i)), LessThan), Cmp(field, Variable(uppermax(i)), GreaterThan))
      val updateminmax = List(
        If(Cmp(Field(lower(i), Some(rowvar(i))), Variable(lowermin(i)), LessThan), List(Assign(Variable(lowermin(i)), Field(lower(i), Some(rowvar(i))))), Nil),
        If(Cmp(Field(upper(i), Some(rowvar(i))), Variable(uppermax(i)), GreaterThan), List(Assign(Variable(uppermax(i)), Field(upper(i), Some(rowvar(i))))), Nil)
      )

      val newlvl = Cmp(Field(lvl(i), None), Variable(loopvar(i)), EqualTo)
      //TODO: NO need of both lower and upper when lvl is included
      val newul = And(Cmp(Field(lower(i), None), Field(lower(i), Some(rowvar(i))), EqualTo), Cmp(Field(upper(i), None), Field(upper(i), Some(rowvar(i))), EqualTo))

      val nextDim = if (i == D) {
        val nonnull = opagg match {
          case OpSum => Assign(Variable(aggvar), Add(Variable(aggvar), Field(aggcol, Some(rowvar(i)))))
          case OpMax => If(Cmp(Field(aggcol, Some(rowvar(i))), Variable(aggvar), GreaterThan), List(Assign(Variable(aggvar), Field(aggcol, Some(rowvar(i))))), Nil)
          case OpMin => If(Cmp(Field(aggcol, Some(rowvar(i))), Variable(aggvar), LessThan), List(Assign(Variable(aggvar), Field(aggcol, Some(rowvar(i))))), Nil)
        }
        val ifnull = Assign(Variable(aggvar), Field(aggcol, Some(rowvar(i))))
        List(If(IsNull(Variable(aggvar)), List(ifnull), List(nonnull)))
      } else {
        rec(i + 1, lvls :+ newlvl, upperlower :+ newul)
      }
      val processRow = If(IsNotNull(Field(lower(i), Some(rowvar(i)))), updateminmax ++ (nextDim), Nil)
      val upperlevelconds = {
        val list = (initConditions ++ (lvls :+ newlvl) ++ upperlower)
        list.tail.foldLeft[Cond](list.head)(And(_, _))
      }
      val body = orcond.toList.flatMap {
        orc =>
          val orderBy = Some(OrderBy(List(field -> (orc.op == LessThan))))
          val where = Some(And(upperlevelconds, And(maincond, orc)))
          val getRow = SelectInto(false, cols, Variable(rowvar(i)), List(TableNamed(rt)), where, None, orderBy, Some(1))
          List(NOP(1), getRow, NOP(1), processRow)
      } ++ List(
        NOP(1),
        Assign(Variable(loopvar(i)), Add(Variable(loopvar(i)), Const("1", TypeInt))),
        //Query very slow without the orderby; Weird
        //TODO: Add maincond as well?
        SelectInto(false, List(field), Variable(rowvar(i)), List(TableNamed(rt)), Some(upperlevelconds), None, Some(OrderBy(List(field -> false))), Some(1)),
        Exit(Some(IsNull(Variable(rowvar(i)))))
      )

      List(
        NOP(1),
        Assign(Variable(lowermin(i)), zero(OpMin)),
        Assign(Variable(uppermax(i)), zero(OpMax)),
        Assign(Variable(loopvar(i)), Const("0", TypeInt)),
        Loop(body))
    }


    val ineqbody = rec(1, Nil, Nil)
    val gbyQuery = Select(true, {
      (1 to G).map(j => keysGbyName(j))
    }.toList.map(Field(_, None)), List(TableNamed(rt)), None, None, None)

    val retvalues = if (G == 0)
      MakeRow(List(Variable(aggvar)))
    else
      MakeRow((1 to G).map(j => Field(keysGbyName(j), Some(gbyeqvar))).toList :+ Variable(aggvar))

    val returnStatement =
      if (isOuterJoin)
        ReturnNext(retvalues)
      else If(IsNotNull(Variable(aggvar)), List(ReturnNext(retvalues)), Nil)


    val lookupBody = (if (G == 0)
      ineqbody :+ returnStatement
    else {
      val initagg = Assign(Variable(aggvar), Const("NULL", TypeDouble))
      List(QueryForLoop(gbyeqvar, gbyQuery, (initagg :: ineqbody) :+ returnStatement))
    }) :+ ReturnNone


    val lookupRetType = TypeSet(TypeRow(aggtype))
    val typeDef = s"drop type if exists $aggtype cascade;\n" +
      TypeDef(aggtype, ((1 to G).map(j => keysGbyName(j)).toList :+ (aggcol + ds_name)).map(_ -> TypeDouble))


    val lookup = s"drop function if exists lookup_$rt;\n" +
      FunctionDef("lookup_" + rt, List(outervar -> TypeRecord), lookupRetType, lookupVar, lookupBody)

    "--------------------- AUTO GEN RANGE ----------------------- \n " +
      typeDef + "\n\n" + construct + "\n\n" + lookup
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


    var keysGby = Map[Int, SourceExpr]()
    var innerEqkeys = Map[Int, SourceExpr]()
    var outerEqKeys = Map[Int, SourceExpr]()
    var innerIneqKeys = Map(1 -> ((s: Option[String]) => Field("price", s)))
    var ineqTheta = Map(1 -> LessThan)
    var outerIneqKeys = Map(1 -> ((s: Option[String]) => Field("price", s)))
    var value = (s: Option[String]) => Field("volume", s)
    var opagg = OpSum
    var outer_name = "aggbids"
    var inner_name = "bids"
    var ds_name = "b2"
    var table = TableDef(outer_name, List("price", "volume").map(_ -> TypeDouble)).toString + NOP(2).toString
    //file.println(table + generateAll(keysGby, innerEqkeys, outerEqKeys, innerIneqKeys, ineqTheta, outerIneqKeys, value, opagg, outer_name, inner_name, ds_name))
    val algos = List("naive", "range", "merge")
    val tables = (10 to 14).toList.map(i => s"bids_${i}_${i}_${i}_10")
    println(generateVerify(tables, algos))
    file.close()
  }

}