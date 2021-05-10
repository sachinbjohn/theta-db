package queries.dbt

import ddbt.lib._
import akka.actor.Actor


object Bids5 {
  import Helper._

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

  def execute(args: Array[String], f: List[Any] => Unit) = 
    bench(args, (dataset: String, parallelMode: Int, timeout: Long, batchSize: Int) => run[Bids5](
      Seq(
        (new java.io.FileInputStream("examples/data/finance.csv"),new Adaptor.OrderBook(brokers=10,deterministic=true,bids="BIDS"),Split())
      ), 
      parallelMode, timeout, batchSize), f)

  def main(args: Array[String]) {

    val argMap = parseArgs(args)
    
    execute(args, (res: List[Any]) => {
      if (!argMap.contains("noOutput")) {
        println("<snap>")
        println("<COUNT>\n" + M3Map.toStr(res(0), List("B1_T", "B1_ID", "B1_BROKER_ID", "B1_VOLUME", "B1_PRICE"))+"\n" + "</COUNT>\n")
        println("</snap>")
      }
    })
  }  
}
class Bids5Base {
  import Bids5._
  import ddbt.lib.Functions._

  val COUNT = M3Map.make[TDLLDD, Long]();
  val COUNTBIDS1 = M3Map.make[TDLLDD, Long]();
  val COUNTBIDS1BIDS1_DELTA = M3Map.make[TDLLDD, Long]();
  val COUNTBIDS1_L2_1 = M3Map.make[TDD, Long]();
  val COUNTBIDS1_L2_1BIDS1_DELTA = M3Map.make[TDD, Long]();
  val COUNTBIDS1_L2_1_L2_1 = M3Map.make[Double, Long]();
  val COUNTBIDS1_L2_1_L2_1BIDS1_DELTA = M3Map.make[Double, Long]();
  val DELTA_BIDS = M3Map.make[TDLLDD, Long]();
  
  def onBatchUpdateBIDS(DELTA_BIDS:M3Map[TDLLDD, Long]) {
    COUNTBIDS1BIDS1_DELTA.clear()
    DELTA_BIDS.foreach { (k1, v1) =>
      val b1_t = k1._1;
      val b1_id = k1._2;
      val b1_broker_id = k1._3;
      val b1_volume = k1._4;
      val b1_price = k1._5;
      COUNTBIDS1BIDS1_DELTA.add(new TDLLDD(b1_t, b1_id, b1_broker_id, b1_volume, b1_price), v1)
    }
    COUNTBIDS1_L2_1BIDS1_DELTA.clear()
    DELTA_BIDS.foreach { (k2, v2) =>
      val b2_t = k2._1;
      val b2_id = k2._2;
      val b2_broker_id = k2._3;
      val b2_volume = k2._4;
      val b2_price = k2._5;
      COUNTBIDS1_L2_1BIDS1_DELTA.add(new TDD(b2_t, b2_price), v2)
    }
    COUNTBIDS1_L2_1_L2_1BIDS1_DELTA.clear()
    DELTA_BIDS.foreach { (k3, v3) =>
      val b3_t = k3._1;
      val b3_id = k3._2;
      val b3_broker_id = k3._3;
      val b3_volume = k3._4;
      val b3_price = k3._5;
      COUNTBIDS1_L2_1_L2_1BIDS1_DELTA.add(b3_t, v3)
    }
    COUNTBIDS1BIDS1_DELTA.foreach { (k4, v4) =>
      val b1_t = k4._1;
      val b1_id = k4._2;
      val b1_broker_id = k4._3;
      val b1_volume = k4._4;
      val b1_price = k4._5;
      COUNTBIDS1.add(new TDLLDD(b1_t, b1_id, b1_broker_id, b1_volume, b1_price), v4)
    }
    COUNTBIDS1_L2_1BIDS1_DELTA.foreach { (k5, v5) =>
      val b2_t = k5._1;
      val b2_price = k5._2;
      COUNTBIDS1_L2_1.add(new TDD(b2_t, b2_price), v5)
    }
    COUNTBIDS1_L2_1_L2_1BIDS1_DELTA.foreach { (b3_t, v6) =>
      COUNTBIDS1_L2_1_L2_1.add(b3_t, v6)
    }
    COUNT.clear()
    COUNTBIDS1.foreach { (k7, v7) =>
      val b1_t = k7._1;
      val b1_id = k7._2;
      val b1_broker_id = k7._3;
      val b1_volume = k7._4;
      val b1_price = k7._5;
      var agg1: Long = 0L
      var agg2: Long = 0L
      val l2 = 0;
      COUNTBIDS1_L2_1.foreach { (k8, v8) =>
        val b2_t = k8._1;
        val b2_price = k8._2;
        var agg3: Long = 0L
        COUNTBIDS1_L2_1_L2_1.foreach { (b3_t, v9) =>
          (if (b3_t > b2_t && b1_t > b3_t) agg3 += v9 else ())
        }
        (if ((1.1 * b2_price) > b1_price && l2 == agg3) agg2 += v8 else ())
      }
      val l1 = agg2;
      (if (l1 == 0) agg1 += 1L else ())
      COUNT.add(new TDLLDD(b1_t, b1_id, b1_broker_id, b1_volume, b1_price), (v7 * agg1))
    }
  }
  
  def onSystemReady() { }

  
}

class Bids5 extends Bids5Base with Actor {
  import ddbt.lib.Messages._
  import ddbt.lib.Functions._
  import Bids5._
  
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
       sender ! (StreamStat(t1 - t0, tN, tS), List({ val COUNT_node_mres = new scala.collection.mutable.HashMap[(Double, Long, Long, Double, Double), Long](); COUNT.foreach { case (e, v) => COUNT_node_mres += ((e._1, e._2, e._3, e._4, e._5) -> v) }; COUNT_node_mres.toMap }))
  }
}