package queries.dbt

import ddbt.lib._
import akka.actor.Actor


object Bids3 {
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
    bench(args, (dataset: String, parallelMode: Int, timeout: Long, batchSize: Int) => run[Bids3](
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
class Bids3Base {
  import Bids3._
  import ddbt.lib.Functions._

  val COUNT = M3Map.make[TDLLDD, Long]();
  val COUNTBIDS1 = M3Map.make[TDLLDD, Long]();
  val COUNTBIDS1BIDS1_DELTA = M3Map.make[TDLLDD, Long]();
  val COUNTBIDS1_L1_1 = M3Map.make[Double, Double]();
  val COUNTBIDS1_L1_1BIDS1_DELTA = M3Map.make[Double, Double]();
  val COUNTBIDS1_L1_2 = M3Map.make[Double, Double]();
  val COUNTBIDS1_L1_2BIDS1_DELTA = M3Map.make[Double, Double]();
  val COUNTBIDS1_L2_1 = M3Map.make[Double, Long]();
  val COUNTBIDS1_L2_1BIDS1_DELTA = M3Map.make[Double, Long]();
  val COUNTBIDS1_L2_2 = M3Map.make[Double, Double]();
  val COUNTBIDS1_L2_2BIDS1_DELTA = M3Map.make[Double, Double]();
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
    COUNTBIDS1_L1_1BIDS1_DELTA.clear()
    DELTA_BIDS.foreach { (k2, v2) =>
      val b4_t = k2._1;
      val b4_id = k2._2;
      val b4_broker_id = k2._3;
      val b4_volume = k2._4;
      val b4_price = k2._5;
      COUNTBIDS1_L1_1BIDS1_DELTA.add(b4_t, (v2 * b4_price))
    }
    COUNTBIDS1_L1_2BIDS1_DELTA.clear()
    DELTA_BIDS.foreach { (k3, v3) =>
      val b5_t = k3._1;
      val b5_id = k3._2;
      val b5_broker_id = k3._3;
      val b5_volume = k3._4;
      val b5_price = k3._5;
      COUNTBIDS1_L1_2BIDS1_DELTA.add(b5_t, (v3 * b5_t))
    }
    COUNTBIDS1_L2_1BIDS1_DELTA.clear()
    DELTA_BIDS.foreach { (k4, v4) =>
      val b2_t = k4._1;
      val b2_id = k4._2;
      val b2_broker_id = k4._3;
      val b2_volume = k4._4;
      val b2_price = k4._5;
      COUNTBIDS1_L2_1BIDS1_DELTA.add(b2_t, v4)
    }
    COUNTBIDS1_L2_2BIDS1_DELTA.clear()
    DELTA_BIDS.foreach { (k5, v5) =>
      val b3_t = k5._1;
      val b3_id = k5._2;
      val b3_broker_id = k5._3;
      val b3_volume = k5._4;
      val b3_price = k5._5;
      COUNTBIDS1_L2_2BIDS1_DELTA.add(b3_t, (v5 * (b3_price * b3_t)))
    }
    COUNTBIDS1BIDS1_DELTA.foreach { (k6, v6) =>
      val b1_t = k6._1;
      val b1_id = k6._2;
      val b1_broker_id = k6._3;
      val b1_volume = k6._4;
      val b1_price = k6._5;
      COUNTBIDS1.add(new TDLLDD(b1_t, b1_id, b1_broker_id, b1_volume, b1_price), v6)
    }
    COUNTBIDS1_L1_1BIDS1_DELTA.foreach { (b4_t, v7) =>
      COUNTBIDS1_L1_1.add(b4_t, v7)
    }
    COUNTBIDS1_L1_2BIDS1_DELTA.foreach { (b5_t, v8) =>
      COUNTBIDS1_L1_2.add(b5_t, v8)
    }
    COUNTBIDS1_L2_1BIDS1_DELTA.foreach { (b2_t, v9) =>
      COUNTBIDS1_L2_1.add(b2_t, v9)
    }
    COUNTBIDS1_L2_2BIDS1_DELTA.foreach { (b3_t, v10) =>
      COUNTBIDS1_L2_2.add(b3_t, v10)
    }
    COUNT.clear()
    COUNTBIDS1.foreach { (k11, v11) =>
      val b1_t = k11._1;
      val b1_id = k11._2;
      val b1_broker_id = k11._3;
      val b1_volume = k11._4;
      val b1_price = k11._5;
      var agg1: Long = 0L
      var agg2: Long = 0L
      COUNTBIDS1_L2_1.foreach { (b2_t, v12) =>
        (if (b1_t > b2_t) agg2 += v12 else ())
      }
      var agg3: Double = 0.0
      COUNTBIDS1_L2_2.foreach { (b3_t, v13) =>
        (if (b1_t > b3_t) agg3 += v13 else ())
      }
      val l1 = (agg2 * agg3);
      var agg4: Double = 0.0
      COUNTBIDS1_L1_1.foreach { (b4_t, v14) =>
        (if (b1_t > b4_t) agg4 += v14 else ())
      }
      var agg5: Double = 0.0
      COUNTBIDS1_L1_2.foreach { (b5_t, v15) =>
        (if (b1_t > b5_t) agg5 += v15 else ())
      }
      val l2 = (agg4 * agg5);
      (if (l1 > l2) agg1 += 1L else ())
      COUNT.add(new TDLLDD(b1_t, b1_id, b1_broker_id, b1_volume, b1_price), (v11 * agg1))
    }
  }
  
  def onSystemReady() { }

  
}

class Bids3 extends Bids3Base with Actor {
  import ddbt.lib.Messages._
  import ddbt.lib.Functions._
  import Bids3._
  
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