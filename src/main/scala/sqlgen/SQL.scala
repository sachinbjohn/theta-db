package sqlgen

import utils.ComparatorOp
import utils.Utils._


abstract class Type

object TypeRecord extends Type {
  override def toString: String = "record"
}

case class TypeRow(tblName: String) extends Type {
  override def toString: String = tblName
}

case class TypeArray(t: Type) extends Type {
  override def toString = t + "[]"
}

case class TypeSet(t: Type) extends Type {
  override def toString = s"SETOF $t"
}

object TypeCursor extends Type {
  override def toString: String = "refcursor"
}

object TypeDouble extends Type {
  override def toString: String = "double precision"
}

object TypeInt extends Type {
  override def toString: String = "integer"
}

object TypeString extends Type {
  override def toString: String = "varchar"
}

abstract class SQL

object SQL {

  sealed abstract class OpAgg extends SQL

  case object OpSum extends OpAgg

  case object OpMin extends OpAgg

  case object OpMax extends OpAgg

  case object OpAvg extends OpAgg

  case object OpCount extends OpAgg

  case object OpCountDistinct extends OpAgg

  sealed abstract class Join extends SQL

  case object JoinInner extends Join

  case object JoinLeft extends Join

  case object JoinRight extends Join

  case object JoinFull extends Join


  case class TableDef(name: String, fields: List[(String, Type)]) extends Statement {
    override def toString: String = s"create table $name" + fields.map { case (n, t) => s"$n $t" }.mkString("(\n", ",\n", "\n);")
  }

  case class TableDefQuery(name: String, q: Query) extends Statement {
    override def toString: String = s"create table $name as \n $q;"
  }

  case class DropTable(name: String) extends Statement {
    override def toString: String = s"drop table if exists $name;"
  }

  case class RenameTable(oldname: String, newname: String) extends Statement {
    override def toString: String = s"alter table $oldname rename to $newname;"
  }
  case class DropIndex(name: String) extends Statement {
    override def toString: String = s"drop index if exists $name;"
  }

  case class Analyze(name: String) extends Statement {
    override def toString: String = s"analyze $name;"
  }

  case class Truncate(name: String) extends Statement {
    override def toString: String = s"truncate $name;"
  }

  case class TempTableDef(name: String, fields: List[(String, Type)]) extends Statement {
    override def toString: String = s"create temp table $name" + fields.map { case (n, t) => s"$n $t" }.mkString("(\n", ",\n", "\n)") + "on commit drop;"
  }

  case class TempTableDefQuery(name: String, q: Query) extends Statement {
    override def toString: String = s"create temp table $name on commit drop as \n $q;"
  }

  case class ViewDef(name: String, q: Query) extends Statement {
    override def toString: String = s"create or replace view $name as $q;"
  }

  case class IndexDef(name: String, unique: Boolean, tblName: String, cols: List[String], include: List[String]) extends Statement {
    override def toString: String = "create " + (if (unique) "unique " else "") + s"index $name on $tblName" + cols.mkString("(", ",", ")") +
      (if (include.isEmpty) "" else include.mkString(" include(", ",", ")")) + ";"
  }

  case class TypeDef(name: String, fields: List[(String, Type)]) extends Statement {
    override def toString: String = s"create type $name as " + fields.map { case (n, t) => s"$n $t" }.mkString("(", ",", ");")
  }

  //-----pg/PLSQL
  abstract class VarDecl

  case class VariableDecl(name: String, typ: Type, init: Option[String]) extends VarDecl {
    override def toString: String = s"$name $typ" + init.map(" := " + _).getOrElse("") + ";"
  }


  case class CursorDecl(name: String, init: Option[Query]) extends VarDecl {
    override def toString: String = name + init.map(" cursor for " + _).getOrElse("refcursor") + ";"
  }

  abstract sealed class CursorDirection

  object CurCurrent extends CursorDirection {
    override def toString: String = "relative 0"
  }

  object CurNext extends CursorDirection {
    override def toString: String = "next"
  }

  case class CurRelative(n: Expr) extends CursorDirection {
    override def toString: String = s"relative $n"
  }

  abstract class Statement

  case class NOP(n: Int) extends Statement {
    override def toString: String = "\n" * (n-1)
  }

  case class FunctionDef(name: String, args: List[(String, Type)], returnType: Type, vars: List[VarDecl], stmts: List[Statement]) extends Statement {
    override def toString: String = "create function " + name + " " + args.map { case (n, t) => s"$n $t" }.mkString("(", ",", ")") +
      " returns " + returnType + " \n language plpgsql as \n $$\n" +
      "declare" + vars.mkString("\n", "\n", "\n") + "begin" + stmts.mkString("\n", "\n", "\n") + "end\n$$;"
  }

  case class ProcedureDef(name: String, args: List[(String, Type)], vars: List[VarDecl], stmts: List[Statement]) extends Statement {
    override def toString: String = "create procedure " + name + " " + args.map { case (n, t) => s"$n $t" }.mkString("(", ",", ")") +
      " \n language plpgsql as \n $$\n" +
      (if (vars.isEmpty) "" else "declare" + vars.mkString("\n", "\n", "\n")) +
      "begin" + stmts.mkString("\n", "\n", "\n") + "end\n$$;"
  }

  case class ProcedureCall(name: String, args: List[Expr]) extends Statement {
    override def toString: String = s"call $name" + args.mkString("(", ",", ");")
  }

  case class ForLoop(v: String, start: String, end: String, reverse: Boolean, body: List[Statement]) extends Statement {
    override def toString: String = s"for $v in " + (if (reverse) "reverse " else "") + s"$start..$end\nloop\n" + body.mkString("\n") +
      "\nend loop;"
  }

  case class QueryForLoop(rv: String, q: Query, body: List[Statement]) extends Statement {
    override def toString = s"for $rv in \n $q \n loop\n" + body.mkString("\n") +
      "\nend loop;"
  }

  case class WhileLoop(cond: Cond, body: List[Statement]) extends Statement {
    override def toString: String = "while " + cond + "\nloop\n" + body.mkString("\n") +
      "\nend loop;"
  }

  case class Loop(body: List[Statement]) extends Statement {
    override def toString: String = "loop\n" + body.mkString("\n") +
      "\nend loop;"
  }

  case class Exit(cond: Option[Cond]) extends Statement {
    override def toString: String = "exit" + cond.map(" when " + _).getOrElse("") + ";"
  }

  case class If(cond: Cond, t: List[Statement], f: List[Statement]) extends Statement {
    override def toString: String = "if " + cond + " \nthen" + t.mkString("\n", "\n", "\n") + (if (f.isEmpty) "" else "else" + f.mkString("\n", "\n", "\n")) + "end if;"
  }

  case class OpenCursor(n: String, qs: Option[(Query, Boolean)]) extends Statement {
    override def toString: String = s"open $n" + qs.map { case (q, s) => (if (s) "scroll " else "") + "for " + q }.getOrElse("") + ";"
  }

  case class CloseCursor(n: String) extends Statement {
    override def toString: String = s"close $n;"
  }

  case class MoveCursor(n: String, direction: CursorDirection) extends Statement {
    override def toString: String = s"move $direction from $n;"
  }

  case class FetchCursor(n: String, direction: CursorDirection, v: String) extends Statement {
    override def toString: String = s"fetch $direction from $n into $v;"
  }

  case class Delete(tbl: String, wh: Option[Cond]) extends Statement {
    override def toString: String = s"delete from $tbl" + (if (wh.isDefined) s"WHERE ${wh.get}" else "") + ";"
  }

  case class Return(expr: Expr) extends Statement {
    override def toString: String = s"return $expr;"
  }

  object ReturnNone extends Statement {
    override def toString: String = s"return;"
  }

  case class ReturnNext(expr: Expr) extends Statement {
    override def toString: String = s"return next $expr;"
  }

  case class SelectInto(distinct: Boolean, cs: List[Expr], v: Variable, ts: List[Table], wh: Option[Cond], gb: Option[GroupBy], ob: Option[OrderBy], limit: Option[Int] = None) extends Statement {
    override def toString =
      "select " + (if (distinct) "distinct " else "") + cs.mkString(", ") +
        s"\ninto $v" +
        "\nfrom " + ts.mkString(", ") +
        wh.map("\nwhere " + _).getOrElse("") +
        gb.map("\n" + _).getOrElse("") +
        ob.map("\n" + _).getOrElse("") +
        limit.map("\nlimit " + _).getOrElse("") + ";"
  }

  case class InsertInto(tbl: String, q: Query) extends Statement {
    override def toString: String = s"insert into $tbl\n" + q + ";"
  }

  case class Assign(v: Variable, e: Expr) extends Statement {
    override def toString: String = s"$v :=  $e;"
  }

  // ---------- Queries
  abstract sealed class Query extends SQL

  case class Lst(es: List[Expr]) extends Query {
    override def toString = es.mkString(", ")
  }

  case class Union(q1: Query, q2: Query, all: Boolean = false) extends Query {
    override def toString =
      q1 + " UNION" + (if (all) " ALL " else " ") + q2
  }

  case class Inter(q1: Query, q2: Query) extends Query {
    override def toString = "(" + q1 + ") INTERSECT (" + q2 + ")"
  }

  case class Except(q1: Query, q2: Query) extends Query {
    override def toString: String = "(" + q1 + ") EXCEPT (" + q2 + ")"
  }


  case class Select(distinct: Boolean, cs: List[Expr], ts: List[Table],
                    wh: Option[Cond], gb: Option[GroupBy], ob: Option[OrderBy]) extends Query {
    override def toString =
      "SELECT " + (if (distinct) "DISTINCT " else "") + cs.mkString(", ") +
        "\nFROM " + ts.mkString(", ") +
        wh.map("\nWHERE " + _).getOrElse("") +
        gb.map("\n" + _).getOrElse("") +
        ob.map("\n" + _).getOrElse("")
  }

  case class GroupBy(fs: List[Expr], cond: Option[Cond]) extends SQL {
    override def toString =
      "GROUP BY " + fs.mkString(", ") +
        cond.map(" HAVING " + _).getOrElse("")
  }

  case class OrderBy(cs: List[(Expr, Boolean)]) extends SQL {
    override def toString =
      if (cs.isEmpty) "" else "ORDER BY " + cs.map { case (f, d) =>
        f + " " + (if (d) "DESC" else "ASC")
      }.mkString(", ")
  }

  case class PartitionBy(cs: List[Expr]) extends SQL {
    override def toString: String = if (cs.isEmpty) "" else "partition by " + cs.mkString(",")
  }

  abstract class FrameStartEnd

  case class Preceding(n: Int) extends FrameStartEnd {
    override def toString = s"$n preceding"
  }

  object UnboundedPreceding extends FrameStartEnd {
    override def toString: String = "unbounded preceding"
  }

  case class Following(n: Int) extends FrameStartEnd {
    override def toString: String = s"$n following"
  }

  object UnboundedFollowing extends FrameStartEnd {
    override def toString: String = "unbounded following"
  }

  object CurrentRow extends FrameStartEnd {
    override def toString: String = "current row"
  }

  abstract class FrameType

  case object FrameRows extends FrameType {
    override def toString: String = "rows"
  }

  case object FrameRange extends FrameType {
    override def toString: String = "range"
  }

  case class Frame(t: FrameType, start: FrameStartEnd, end: FrameStartEnd) extends SQL {
    override def toString: String = s"$t between $start and $end"
  }

  case class Window(partitionBy: PartitionBy, orderBy: OrderBy, frame: Option[Frame]) extends SQL {
    override def toString: String = partitionBy + " " + orderBy + " " + frame.getOrElse("")
  }

  // ---------- Tables
  abstract sealed class Table extends SQL

  case class TableQuery(q: Query) extends Table {
    override def toString = "(" + ind("\n" + q.toString) + "\n)"
  }

  case class TableNamed(n: String) extends Table {
    override def toString = n
  }

  case class TableAlias(t: Table, n: String) extends Table {
    override def toString = t + " " + n
  }

  case class TableJoin(t1: Table, t2: Table, j: Join, c: Option[Cond]) extends Table {
    // empty condition = natural join
    override def toString = t1 + "\n  " + (j match {
      case JoinInner => if (c == None) "NATURAL JOIN" else "JOIN"
      case JoinLeft => "LEFT JOIN"
      case JoinRight => "RIGHT JOIN"
      case JoinFull => "FULL JOIN"
    }) + " " + t2 + c.map(" ON " + _).getOrElse("")
  }

  // ---------- Expressions
  abstract sealed class Expr extends SQL

  case class Cast(e: Expr, t: Type) extends Expr {
    override def toString: String = s"($e)::$t"
  }

  case class Variable(n: String) extends Expr {
    override def toString: String = n
  }

  case class FunCall(name: String, args: List[Expr]) extends Expr {
    override def toString: String = name + args.mkString("(", ",", ")")
  }

  case class Alias(e: Expr, n: String) extends Expr {
    override def toString = e + " AS " + n
  }


  case class RowField(n: String, rv: String) extends Expr {
    override def toString = s"$rv.$n"
  }

  case class MakeRow(cs: List[Expr]) extends Expr {
    override def toString: String = "ROW" + cs.mkString("(", ",", ")")
  }

  case class MakeArray(q: Query) extends Expr {
    override def toString = "ARRAY(" + q + ")"
  }

  case class Field(n: String, t: Option[String]) extends Expr {
    override def toString = t.map(_ + "." + n).getOrElse(n)
  }

  case class Const(v: String, tp: Type) extends Expr {
    override def toString = if (tp == TypeString) "'" + v + "'" else v
  }

  case class Apply(fun: String, args: List[Expr]) extends Expr {
    override def toString = fun + "(" + args.mkString(", ") + ")"
  }

  case class Nested(q: Query) extends Expr {
    override def toString = "(" + ind("\n" + q.toString) + "\n)"
  }

  case class Case(ce: List[(Cond, Expr)], d: Expr) extends Expr {
    override def toString =
      "CASE" + ind(
        ce.map { case (c, t) => "\nWHEN " + c + " THEN " + t }.mkString +
          "\nELSE " + d) +
        "\nEND"
  }

  object AllCols extends Expr {
    override def toString: String = "*"
  }

  // ---------- Arithmetic
  case class Add(l: Expr, r: Expr) extends Expr {
    override def toString = "(" + l + " + " + r + ")"
  }

  case class Sub(l: Expr, r: Expr) extends Expr {
    override def toString = "(" + l + " - " + r + ")"
  }

  case class Mul(l: Expr, r: Expr) extends Expr {
    override def toString = "(" + l + " * " + r + ")"
  }

  case class Div(l: Expr, r: Expr) extends Expr {
    override def toString = "(" + l + " / " + r + ")"
  }

  case class Mod(l: Expr, r: Expr) extends Expr {
    override def toString = "(" + l + " % " + r + ")"
  }

  // ---------- Aggregation


  case class Agg(e: Expr, op: OpAgg, window: Option[Window] = None) extends Expr {
    override def toString = (op match {
      case OpCountDistinct => "COUNT(DISTINCT "
      case _ => op.toString.substring(2).toUpperCase + "("
    }) + e + ")" + window.map(" OVER (" + _ + ")").getOrElse("")

  }

  case class DenseRank(window: Window) extends Expr {
    override def toString: String = s"dense_rank() over($window) - 1"
  }

  case class Log(e: Expr) extends Expr {
    override def toString: String = s"ceil(log(2, $e))"
  }

  case class All(q: Query) extends Expr {
    override def toString = "ALL(" + ind("\n" + q) + "\n)"
  }

  case class Som(q: Query) extends Expr {
    override def toString = "SOME(" + ind("\n" + q) + "\n)"
  }

  // ---------- Conditions
  sealed abstract class Cond

  object TrueCond extends Cond {
    override def toString: String = "TRUE"
  }

  object FalseCond extends Cond {
    override def toString: String = "FALSE"
  }

  case class And(l: Cond, r: Cond) extends Cond {
    override def toString = "(" + l + " AND " + r + ")"
  }

  case class Or(l: Cond, r: Cond) extends Cond {
    override def toString = "(" + l + " OR " + r + ")"
  }

  case class Exists(q: Query) extends Cond {
    override def toString = "EXISTS(" + ind("\n" + q) + "\n)"
  }

  case class In(e: Expr, q: Query) extends Cond {
    override def toString = e + " IN (" + ind("\n" + q) + "\n)"
  }

  case class Not(e: Cond) extends Cond {
    override def toString = "NOT(" + e + ")"
  }

  case class Like(l: Expr, p: String) extends Cond {
    override def toString = l + " LIKE '" + p + "'"
  }

  case class IsNull(e: Expr) extends Cond {
    override def toString: String = s"$e IS NULL"
  }

  case class IsNotNull(e: Expr) extends Cond {
    override def toString: String = s"$e IS NOT NULL"
  }

  case class Cmp(l: Expr, r: Expr, op: ComparatorOp[Double]) extends Cond {
    override def toString = l + " " + op + " " + r
  }

}