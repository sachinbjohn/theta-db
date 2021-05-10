package queries.dbt

import ddbt.lib._
import akka.actor.Actor


object Bids6 {
  import Helper._

  class TDD(val _1:Double, val _2:Double) extends Product {
    def canEqual(that: Any) = true
    def productArity = 2
    def productElement(i: Int): Any = List[Any](_1, _2)(i)
    override def equals(o: Any) = { o match { case x: TDD => (_1 == x._1 && _2 == x._2) case x: Product => if (this.productArity == x.productArity) (0 to (productArity - 1)).forall(i => x.productElement(i) == this.productElement(i)) else false case _ => false } }
    override def toString() = "<"+List[Any](_1,_2).mkString(",")+">"
    override def hashCode() = {
      var h: Int = 2
      h = h * 41 + _1.toInt
      h = h * 41 + _2.toInt
      h
    }
  }
  
  class TDLLDD(val _1:Double, val _2:Long, val _3:Long, val _4:Double, val _5:Double) extends Product {
    def canEqual(that: Any) = true
    def productArity = 5
    def productElement(i: Int): Any = List[Any](_1, _2, _3, _4, _5)(i)
    override def equals(o: Any) = { o match { case x: TDLLDD => (_1 == x._1 && _2 == x._2 && _3 == x._3 && _4 == x._4 && _5 == x._5) case x: Product => if (this.productArity == x.productArity) (0 to (productArity - 1)).forall(i => x.productElement(i) == this.productElement(i)) else false case _ => false } }
    override def toString() = "<"+List[Any](_1,_2,_3,_4,_5).mkString(",")+">"
    override def hashCode() = {
      var h: Int = 5
      h = h * 41 + _1.toInt
      h = h * 41 + _2.toInt
      h = h * 41 + _3.toInt
      h = h * 41 + _4.toInt
      h = h * 41 + _5.toInt
      h
    }
  }

  def execute(args: Array[String], f: List[Any] => Unit) = 
    bench(args, (dataset: String, parallelMode: Int, timeout: Long, batchSize: Int) => run[Bids6](
      Seq(
        (new java.io.FileInputStream("examples/data/finance.csv"),new Adaptor.OrderBook(brokers=10,deterministic=true,bids="BIDS"),Split())
      ), 
      parallelMode, timeout, batchSize), f)

  def main(args: Array[String]) {

    val argMap = parseArgs(args)
    
    execute(args, (res: List[Any]) => {
      if (!argMap.contains("noOutput")) {
        println("<snap>")
        println("<__SQL_SUM_AGGREGATE_1>\n" + M3Map.toStr(res(0))+"\n" + "</__SQL_SUM_AGGREGATE_1>\n")
        println("</snap>")
      }
    })
  }  
}
class Bids6Base {
  import Bids6._
  import ddbt.lib.Functions._
  var tstart = Double.NegativeInfinity
  var tend = Double.PositiveInfinity
  var __SQL_SUM_AGGREGATE_1: Long = 0L
  val __SQL_SUM_AGGREGATE_1BIDS1_DELTA = M3Map.make[TDD, Long]();
  val __SQL_SUM_AGGREGATE_1BIDS1 = M3Map.make[TDD, Long]();
  var __SQL_SUM_AGGREGATE_1BIDS3_DELTA: Long = 0L
  val DELTA_BIDS = M3Map.make[TDLLDD, Long]();

  def onBatchUpdateBIDS(DELTA_BIDS:M3Map[TDLLDD, Long]) {
    var agg1: Long = 0L
    DELTA_BIDS.foreach { (k1, v1) =>
      val b1_t = k1._1;
      val b1_id = k1._2;
      val b1_broker_id = k1._3;
      val b1_volume = k1._4;
      val b1_price = k1._5;
      DELTA_BIDS.foreach { (k2, v2) =>
        val b2_t = k2._1;
        val b2_id = k2._2;
        val b2_broker_id = k2._3;
        val b2_volume = k2._4;
        val b2_price = k2._5;
        (if (tend > b1_t && b1_t > tstart && b1_price > b2_price && b2_t > b1_t && tend > b2_t && b2_t > tstart) agg1 += (v1 * v2) else ())
      }
    }
    __SQL_SUM_AGGREGATE_1BIDS3_DELTA  =  agg1
    __SQL_SUM_AGGREGATE_1BIDS1_DELTA.clear()
    DELTA_BIDS.foreach { (k3, v3) =>
      val b1_t = k3._1;
      val b1_id = k3._2;
      val b1_broker_id = k3._3;
      val b1_volume = k3._4;
      val b1_price = k3._5;
      (if (tend > b1_t && b1_t > tstart) __SQL_SUM_AGGREGATE_1BIDS1_DELTA.add(new TDD(b1_t, b1_price), v3) else ());
    }
    var agg2: Long = 0L
    __SQL_SUM_AGGREGATE_1BIDS1_DELTA.foreach { (k4, v4) =>
      val b1_t = k4._1;
      val b1_price = k4._2;
      __SQL_SUM_AGGREGATE_1BIDS1.foreach { (k5, v5) =>
        val b2_t = k5._1;
        val b2_price = k5._2;
        (if (b1_price > b2_price && b2_t > b1_t) agg2 += (v4 * v5) else ())
      }
    }
    var agg3: Long = 0L
    __SQL_SUM_AGGREGATE_1BIDS1_DELTA.foreach { (k6, v6) =>
      val b2_t = k6._1;
      val b2_price = k6._2;
      __SQL_SUM_AGGREGATE_1BIDS1.foreach { (k7, v7) =>
        val b1_t = k7._1;
        val b1_price = k7._2;
        (if (b1_price > b2_price && b2_t > b1_t) agg3 += (v6 * v7) else ())
      }
    }
    __SQL_SUM_AGGREGATE_1  +=  (agg2 + (agg3 + __SQL_SUM_AGGREGATE_1BIDS3_DELTA))
    __SQL_SUM_AGGREGATE_1BIDS1_DELTA.foreach { (k8, v8) =>
      val b2_t = k8._1;
      val b2_price = k8._2;
      __SQL_SUM_AGGREGATE_1BIDS1.add(new TDD(b2_t, b2_price), v8)
    }
  }
  
  def onSystemReady() { }

  
}

class Bids6 extends Bids6Base with Actor {
  import ddbt.lib.Messages._
  import ddbt.lib.Functions._
  import Bids6._
  
  var t0 = 0L; var t1 = 0L; var tN = 0L; var tS = 0L

  

  

  def receive_skip: Receive = { 
    case EndOfStream | GetSnapshot(_) => 
       sender ! (StreamStat(t1 - t0, tN, tS), null)
    case _ => tS += 1L
  }

  def receive = {
    case BatchUpdateEvent(streamData) =>
      val batchSize = streamData.map(_._2.length).sum
      // Timeout check
      if (t1 > 0) {
        val t = System.nanoTime
        if (t > t1) { t1 = t; tS = batchSize; context.become(receive_skip) }
      }
      tN += batchSize
    
      streamData.foreach { 
        case ("BIDS", dataList) => 
          DELTA_BIDS.clear
          dataList.foreach { case List(v0:Double,v1:Long,v2:Long,v3:Double,v4:Double,vv:TupleOp) =>
            DELTA_BIDS.add(new TDLLDD(v0, v1, v2, v3, v4),vv)
          }
          onBatchUpdateBIDS(DELTA_BIDS)
        case (s, _) => sys.error("Unknown stream event name " + s)
      }
    case StreamInit(timeout) => 
      
      onSystemReady();
      t0 = System.nanoTime;
      if (timeout > 0) t1 = t0 + timeout * 1000000L
    case EndOfStream | GetSnapshot(_) => 
      t1 = System.nanoTime;
       sender ! (StreamStat(t1 - t0, tN, tS), List(__SQL_SUM_AGGREGATE_1))
  }
}