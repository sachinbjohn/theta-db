package query.dbt

import ddbt.lib._
import akka.actor.Actor


object VWAP1 {
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

  def execute(args: Array[String], f: List[Any] => Unit) = 
    bench(args, (dataset: String, parallelMode: Int, timeout: Long, batchSize: Int) => run[VWAP1](
      Seq(
        (new java.io.FileInputStream("examples/data/finance.csv"),new Adaptor.OrderBook(brokers=10,deterministic=true,bids="BIDS"),Split())
      ), 
      parallelMode, timeout, batchSize), f)

  def main(args: Array[String]) {

    val argMap = parseArgs(args)
    
    execute(args, (res: List[Any]) => {
      if (!argMap.contains("noOutput")) {
        println("<snap>")
        println("<VWAP>\n" + M3Map.toStr(res(0))+"\n" + "</VWAP>\n")
        println("</snap>")
      }
    })
  }  
}
class VWAP1Base {
  import VWAP1._
  import ddbt.lib.Functions._

  var VWAP: Double = 0.0
  val VWAPBIDS1 = M3Map.make[Double, Double]();
  val VWAPBIDS1BIDS1_DELTA = M3Map.make[Double, Double]();
  var VWAPBIDS1_L1_1: Double = 0.0
  var VWAPBIDS1_L1_1BIDS1_DELTA: Double = 0.0
  val VWAPBIDS1_L2_1 = M3Map.make[Double, Double]();
  val VWAPBIDS1_L2_1BIDS1_DELTA = M3Map.make[Double, Double]();
  val DELTA_BIDS = M3Map.make[TDLLDD, Long]();
  
  def onBatchUpdateBIDS(DELTA_BIDS:M3Map[TDLLDD, Long]) {
    VWAPBIDS1BIDS1_DELTA.clear()
    DELTA_BIDS.foreach { (k1, v1) =>
      val b1_t = k1._1;
      val b1_id = k1._2;
      val b1_broker_id = k1._3;
      val b1_volume = k1._4;
      val b1_price = k1._5;
      VWAPBIDS1BIDS1_DELTA.add(b1_price, (v1 * (b1_price * b1_volume)))
    }
    var agg1: Double = 0.0
    DELTA_BIDS.foreach { (k2, v2) =>
      val b3_t = k2._1;
      val b3_id = k2._2;
      val b3_broker_id = k2._3;
      val b3_volume = k2._4;
      val b3_price = k2._5;
      agg1 += (v2 * b3_volume)
    }
    VWAPBIDS1_L1_1BIDS1_DELTA  =  agg1
    VWAPBIDS1_L2_1BIDS1_DELTA.clear()
    DELTA_BIDS.foreach { (k3, v3) =>
      val b2_t = k3._1;
      val b2_id = k3._2;
      val b2_broker_id = k3._3;
      val b2_volume = k3._4;
      val b2_price = k3._5;
      VWAPBIDS1_L2_1BIDS1_DELTA.add(b2_price, (v3 * b2_volume))
    }
    VWAPBIDS1BIDS1_DELTA.foreach { (b1_price, v4) =>
      VWAPBIDS1.add(b1_price, v4)
    }
    VWAPBIDS1_L1_1  +=  VWAPBIDS1_L1_1BIDS1_DELTA
    VWAPBIDS1_L2_1BIDS1_DELTA.foreach { (b2_price, v5) =>
      VWAPBIDS1_L2_1.add(b2_price, v5)
    }
    var agg2: Double = 0.0
    val l1 = (VWAPBIDS1_L1_1 * 0.25);
    VWAPBIDS1.foreach { (b1_price, v6) =>
      var agg3: Double = 0.0
      VWAPBIDS1_L2_1.foreach { (b2_price, v7) =>
        (if (b1_price > b2_price) agg3 += v7 else ())
      }
      val l2 = agg3;
      (if (l2 > l1) agg2 += v6 else ())
    }
    VWAP  =  agg2
  }
  
  def onSystemReady() {
    VWAP  =  0.0
    VWAPBIDS1_L1_1  =  0.0
    VWAPBIDS1_L1_1BIDS1_DELTA  =  0.0
  }

  
}

class VWAP1 extends VWAP1Base with Actor {
  import ddbt.lib.Messages._
  import ddbt.lib.Functions._
  import VWAP1._
  
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
       sender ! (StreamStat(t1 - t0, tN, tS), List(VWAP))
  }
}