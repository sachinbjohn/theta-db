package sqlgen

import sqlgen.SQL._
import utils._

object MBGen {
  type SourceExpr = Option[String] => Expr

  def gbyKeyName(i: Int) = s"gbykey$i"

  def ineqKeyName(i: Int) = s"ineqkey$i"

  def eqKeyName(i: Int) = s"eqkey$i"

  def main(args: Array[String]): Unit = {
    var innername = "bids"
    var outername = "bids"
    var innergby = Nil
    var outergby = List[SourceExpr](s => Field("price", s), s => Field("time", s))
    var innerval: SourceExpr = s => Field("volume", s)
    var outerval: SourceExpr = s => Field("volume", s)
    var innereqkeys = List[SourceExpr](s => Field("time", s))
    var outereqkeys = List[SourceExpr](s => Field("time", s))
    var innerineqkeys = List[SourceExpr](s => Field("price", s))
    var outerineqkeys = List[SourceExpr](s => Field("price", s))
    var ineqtheta = List[ComparatorOp[Double]](LessThan)

    println(generateRange(innername, outername, innergby, outergby, innerval, outerval, innereqkeys, outereqkeys, innerineqkeys, outerineqkeys, ineqtheta))
  }

  def generateNaive(innername: String, outername: String, innergby: Seq[SourceExpr], outergby: Seq[SourceExpr], innerval: SourceExpr, outerval: SourceExpr, innereqkeys: Seq[SourceExpr], outereqkeys: Seq[SourceExpr], innerineqkeys: Seq[SourceExpr], outerineqkeys: Seq[SourceExpr], ineqtheta: Seq[ComparatorOp[Double]]) = {
    val inneralias = "S"
    val outeralias = "R"

    val gby = (outergby.map(_ (Some(outeralias))).toList ++ innergby.map(_ (Some(inneralias))).toList)
    val cols = gby :+ Agg(Mul(outerval(Some(outeralias)), innerval(Some(inneralias))), OpSum)
    val joinc = {
      val list = innereqkeys.map(_ (Some(inneralias))).zip(outereqkeys.map(_ (Some(outeralias)))).map { case (i, o) => Cmp(i, o, EqualTo) } ++
        innerineqkeys.map(_ (Some(inneralias))).zip(outerineqkeys.map(_ (Some(outeralias)))).zip(ineqtheta).map { case ((i, o), t) => Cmp(i, o, t) }
      val cond = if (list.isEmpty) TrueCond else list.tail.foldLeft[Cond](list.head)((acc, cur) => And(acc, cur))
      Some(cond)
    }
    val table = TableJoin(TableAlias(TableNamed(outername), outeralias), TableAlias(TableNamed(innername), inneralias), JoinInner, joinc)
    val query = InsertInto("result", Select(false, cols, List(table), None, Some(GroupBy(gby, None)), None))
    val vars = List(
      VariableDecl("StartTime", TypeTimestamp, None),
      VariableDecl("EndTime", TypeTimestamp, None),
      VariableDecl("Delta", TypeDouble, None)
    )
    ProcedureDef("queryNaive", Nil, vars, List(
      MeasureTime(List(query)),
      Return(Cast(Variable("Delta"), TypeInt))
    ))

  }

  def generateSmart(innername: String, outername: String, innergby: Seq[SourceExpr], outergby: Seq[SourceExpr], innerval: SourceExpr, outerval: SourceExpr, innereqkeys: Seq[SourceExpr], outereqkeys: Seq[SourceExpr], innerineqkeys: Seq[SourceExpr], outerineqkeys: Seq[SourceExpr], ineqtheta: Seq[ComparatorOp[Double]]) = {

    val inneralias = "S"
    val outeralias = "R"
    val joinalias = "X"
    val D = innerineqkeys.size
    val E = innereqkeys.size
    val GS = innergby.size
    val GR = outergby.size
    val body = collection.mutable.ListBuffer[Statement]()

    body += ViewDef("aggviewR", {
      val gby = {
        outergby.zipWithIndex.map { case (e, i) => Alias(e(None), gbyKeyName(i+1)) } ++
          outereqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), eqKeyName(i+1)) } ++
          outerineqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), ineqKeyName(i+1)) }
      }.toList
      val gbyNoAlias = {
        outergby.zipWithIndex.map { case (e, i) => e(None) } ++
          outereqkeys.zipWithIndex.map { case (e, i) => e(None) } ++
          outerineqkeys.zipWithIndex.map { case (e, i) => e(None) }
      }.toList
      val cols = gby :+ Alias(Agg(outerval(None), OpSum), "aggR")
      Select(false, cols, List(TableNamed(outername)), None, Some(GroupBy(gbyNoAlias, None)), None)
    })
    body += NOP(1)

    body += ViewDef("aggviewS", {
      val gby = {
        innergby.zipWithIndex.map { case (e, i) => Alias(e(None), gbyKeyName(i+1)) } ++
          innereqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), eqKeyName(i+1)) } ++
          innerineqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), ineqKeyName(i+1)) }
      }.toList
      val gbyNoAlias = {
        innergby.zipWithIndex.map { case (e, i) => e(None) } ++
          innereqkeys.zipWithIndex.map { case (e, i) => e(None) } ++
          innerineqkeys.zipWithIndex.map { case (e, i) => e(None) }
      }.toList
      val cols = gby :+ Alias(Agg(innerval(None), OpSum), "aggS")
      Select(false, cols, List(TableNamed(innername)), None, Some(GroupBy(gbyNoAlias, None)), None)
    })

    body += NOP(1)
    body += ViewDef("keyviewR", {
      val keys = {
        outereqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), eqKeyName(i+1)) } ++
          outerineqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), ineqKeyName(i+1)) }
      }.toList
      Select(true, keys, List(TableNamed(outername)), None, None, None)
    })
    body += NOP(1)
    body += ViewDef("RSJoin", {
      val gby = {
        (1 to E).map(i => Field(eqKeyName(i), Some(outeralias))) ++
          (1 to D).map(i => Field(ineqKeyName(i), Some(outeralias))) ++
          (1 to GS).map(i => Field(gbyKeyName(i), Some(inneralias)))
      }.toList
      val cols = gby :+ Alias(Agg(Field("aggS", Some(inneralias)), OpSum), "aggS")
      val joinc = {
        val list = (1 to E).map { i => Cmp(Field(eqKeyName(i), Some(inneralias)), Field(eqKeyName(i), Some(outeralias)), EqualTo) } ++
          (1 to D).zip(ineqtheta).map { case (i, t) => Cmp(Field(ineqKeyName(i), Some(inneralias)), Field(ineqKeyName(i), Some(outeralias)), t) }
        val cond = if (list.isEmpty) TrueCond else list.tail.foldLeft[Cond](list.head)((acc, cur) => And(acc, cur))
        Some(cond)
      }
      val table = TableJoin(TableAlias(TableNamed(outername), outeralias), TableAlias(TableNamed(innername), inneralias), JoinInner, joinc)
      Select(false, cols, List(table), None, Some(GroupBy(gby, None)), None)
    })
    body += NOP(1)

    body += InsertInto("result", {
      val gby = {
        (1 to GR).map(i => Field(gbyKeyName(i), Some(outeralias))) ++
          (1 to GS).map(i => Field(gbyKeyName(i), Some(joinalias)))
      }.toList
      val cols = gby :+ Agg(Mul(Field("aggR", Some(outeralias)), Field("aggS", Some(joinalias))), OpSum)
      val joinC = {
        val list = (1 to E).map { i => Cmp(Field(eqKeyName(i), Some(joinalias)), Field(eqKeyName(i), Some(outeralias)), EqualTo) } ++
          (1 to D).map { case i => Cmp(Field(ineqKeyName(i), Some(joinalias)), Field(ineqKeyName(i), Some(outeralias)), EqualTo) }
        Some(if (list.isEmpty) TrueCond else list.tail.foldLeft[Cond](list.head)((acc, cur) => And(acc, cur)))
      }
      val table = TableJoin(TableAlias(TableNamed(outername), outeralias), TableAlias(TableNamed("RSJoin"), joinalias), JoinInner, joinC)
      Select(false, cols, List(table), None, Some(GroupBy(gby, None)), None)
    })
    val vars = List(
      VariableDecl("StartTime", TypeTimestamp, None),
      VariableDecl("EndTime", TypeTimestamp, None),
      VariableDecl("Delta", TypeDouble, None)
    )
    ProcedureDef("querySmart", Nil, vars, List(
      MeasureTime(body.toList),
      Return(Cast(Variable("Delta"), TypeInt))
    ))

  }

  def generateRange(innername: String, outername: String, innergby: Seq[SourceExpr], outergby: Seq[SourceExpr], innerval: SourceExpr, outerval: SourceExpr, innereqkeys: Seq[SourceExpr], outereqkeys: Seq[SourceExpr], innerineqkeys: Seq[SourceExpr], outerineqkeys: Seq[SourceExpr], ineqtheta: Seq[ComparatorOp[Double]]) = {

    val inneralias = "S"
    val outeralias = "R"
    val joinalias = "X"
    val D = innerineqkeys.size
    val E = innereqkeys.size
    val GS = innergby.size
    val GR = outergby.size
    val body = collection.mutable.ListBuffer[Statement]()

    body += ViewDef("aggviewR", {
      val gby = {
        outergby.zipWithIndex.map { case (e, i) => Alias(e(None), gbyKeyName(i+1)) } ++
          outereqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), eqKeyName(i+1)) } ++
          outerineqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), ineqKeyName(i+1)) }
      }.toList
      val gbyNoAlias = {
        outergby.zipWithIndex.map { case (e, i) => e(None) } ++
          outereqkeys.zipWithIndex.map { case (e, i) => e(None) } ++
          outerineqkeys.zipWithIndex.map { case (e, i) => e(None) }
      }.toList
      val cols = gby :+ Alias(Agg(outerval(None), OpSum), "aggR")
      Select(false, cols, List(TableNamed(outername)), None, Some(GroupBy(gbyNoAlias, None)), None)
    })
    body += NOP(1)

    body += ViewDef("aggviewS", {
      val gby = {
        innergby.zipWithIndex.map { case (e, i) => Alias(e(None), gbyKeyName(i+1)) } ++
          innereqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), eqKeyName(i+1)) } ++
          innerineqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), ineqKeyName(i+1)) }
      }.toList
      val gbyNoAlias = {
        innergby.zipWithIndex.map { case (e, i) => e(None) } ++
          innereqkeys.zipWithIndex.map { case (e, i) => e(None) } ++
          innerineqkeys.zipWithIndex.map { case (e, i) => e(None) }
      }.toList
      val cols = gby :+ Alias(Agg(innerval(None), OpSum), "aggS")
      Select(false, cols, List(TableNamed(innername)), None, Some(GroupBy(gbyNoAlias, None)), None)
    })

    body += NOP(1)
    body += ViewDef("keyviewR", {
      val keys = {
        outereqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), eqKeyName(i+1)) } ++
          outerineqkeys.zipWithIndex.map { case (e, i) => Alias(e(None), ineqKeyName(i+1)) }
      }.toList
      Select(true, keys, List(TableNamed(outername)), None, None, None)
    })
    body += NOP(1)
    body += ProcedureCall("construct_rt_bidsS", Nil)
    body += NOP(1)
    body += TempTableDefQuery("RSJoin", {
      val gby = {
        (1 to E).map(i => Field(eqKeyName(i), Some(outeralias))) ++
          (1 to D).map(i => Field(ineqKeyName(i), Some(outeralias))) ++
          (1 to GS).map(i => Field(gbyKeyName(i), Some("(f)")))
      }.toList
      val cols = gby :+ Alias(Field("aggS", Some("(f)")), "aggS")
      val table = List(TableAlias(TableNamed(outername), outeralias), FunCallWithOffset("lookup_rt_bidsS", List(Field("*", Some(outeralias))), "func"))
      Select(false, cols, table, None, Some(GroupBy(gby, None)), None)
    })
    body += NOP(1)
    body += InsertInto("result", {
      val gby = {
        (1 to GR).map(i => Field(gbyKeyName(i), Some(outeralias))) ++
          (1 to GS).map(i => Field(gbyKeyName(i), Some(joinalias)))
      }.toList
      val cols = gby :+ Agg(Mul(Field("aggR", Some(outeralias)), Field("aggS", Some(joinalias))), OpSum)
      val joinC = {
        val list = (1 to E).map { i => Cmp(Field(eqKeyName(i), Some(joinalias)), Field(eqKeyName(i), Some(outeralias)), EqualTo) } ++
          (1 to D).map { case i => Cmp(Field(ineqKeyName(i), Some(joinalias)), Field(ineqKeyName(i), Some(outeralias)), EqualTo) }
        Some(if (list.isEmpty) TrueCond else list.tail.foldLeft[Cond](list.head)((acc, cur) => And(acc, cur)))
      }
      val table = TableJoin(TableAlias(TableNamed(outername), outeralias), TableAlias(TableNamed("RSJoin"), joinalias), JoinInner, joinC)
      Select(false, cols, List(table), None, None, None)
    })
    val vars = List(
      VariableDecl("StartTime", TypeTimestamp, None),
      VariableDecl("EndTime", TypeTimestamp, None),
      VariableDecl("Delta", TypeDouble, None)
    )


    val defs = {
      def toMap[T](s: Seq[T]): Map[Int, T] = s.zipWithIndex.map { case (e, i) => (i+1, e) }.toMap

      val gbyMap: Map[Int, SourceExpr] = (1 to GS).map(i => i -> ((s: Option[String]) => Field(gbyKeyName(i), s))).toMap
      val innerEqMap: Map[Int, SourceExpr] = (1 to E).map(i => i -> ((s: Option[String]) => Field(eqKeyName(i), s))).toMap
      val outerEqMap = innerEqMap
      val innerIneqMap: Map[Int, SourceExpr] = (1 to D).map(i => i -> ((s: Option[String]) => Field(ineqKeyName(i), s))).toMap
      val outerIneqMap = innerIneqMap
      val thetamap = toMap(ineqtheta)
      assert(innerEqMap.size == E)
      assert(outerEqMap.size == E)
      assert(innerIneqMap.size == D)
      assert(outerIneqMap.size == D)
      assert(thetamap.size == D)
      Generator.generateRange(gbyMap, innerEqMap, outerEqMap, innerIneqMap, thetamap, outerIneqMap, s => Field("aggS", s), OpSum, "aggviewR", "aggviewS", "bidsS", false, true)
    }

    defs + "\n\n" +
      ProcedureDef("queryRange", Nil, vars, List(
        MeasureTime(body.toList),
        Return(Cast(Variable("Delta"), TypeInt))
      ))

  }
}