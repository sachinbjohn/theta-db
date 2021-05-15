#include <sys/time.h>
#include <cstring>
#include <vector>
#include <tuple>
#include "types.hpp"
#include "hash.hpp"
#include "multi_map.hpp"
#include "standard_functions.hpp"
#include "event.hpp"
#include "source.hpp"





using namespace std;

namespace dbtoaster {

  using namespace standard_functions;
  using namespace hashing;
  using namespace serialization;

  /* Definitions of maps used for storing materialized views. */
  struct __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry {
    DoubleType B1_T; DoubleType B1_PRICE; long __av; __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry* nxt; __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry* prv;
  
    explicit __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry() : nxt(nullptr), prv(nullptr) { }
    explicit __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry(const DoubleType c0, const DoubleType c1, const long c2) { B1_T = c0; B1_PRICE = c1; __av = c2;  }
    __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry(const __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry& other) : B1_T(other.B1_T), B1_PRICE(other.B1_PRICE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry& modify(const DoubleType c0, const DoubleType c1) { B1_T = c0; B1_PRICE = c1;  return *this; }
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_T, STRING(B1_T));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_PRICE, STRING(B1_PRICE));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, __av, STRING(__av));
    }
  };
  
  struct __SQL_SUM_AGGREGATE_1BIDS1_DELTA_mapkey01_idxfn {
    FORCE_INLINE static size_t hash(const __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B1_T);
      hash_combine(h, e.B1_PRICE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry& x, const __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry& y) {
      return x.B1_T == y.B1_T && x.B1_PRICE == y.B1_PRICE;
    }
  };
  
  typedef MultiHashMap<__SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry, long, 
    PrimaryHashIndex<__SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry, __SQL_SUM_AGGREGATE_1BIDS1_DELTA_mapkey01_idxfn>
  > __SQL_SUM_AGGREGATE_1BIDS1_DELTA_map;
  
  
  struct __SQL_SUM_AGGREGATE_1BIDS1_entry {
    DoubleType B2_T; DoubleType B2_PRICE; long __av; __SQL_SUM_AGGREGATE_1BIDS1_entry* nxt; __SQL_SUM_AGGREGATE_1BIDS1_entry* prv;
  
    explicit __SQL_SUM_AGGREGATE_1BIDS1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit __SQL_SUM_AGGREGATE_1BIDS1_entry(const DoubleType c0, const DoubleType c1, const long c2) { B2_T = c0; B2_PRICE = c1; __av = c2;  }
    __SQL_SUM_AGGREGATE_1BIDS1_entry(const __SQL_SUM_AGGREGATE_1BIDS1_entry& other) : B2_T(other.B2_T), B2_PRICE(other.B2_PRICE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE __SQL_SUM_AGGREGATE_1BIDS1_entry& modify(const DoubleType c0, const DoubleType c1) { B2_T = c0; B2_PRICE = c1;  return *this; }
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B2_T, STRING(B2_T));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B2_PRICE, STRING(B2_PRICE));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, __av, STRING(__av));
    }
  };
  
  struct __SQL_SUM_AGGREGATE_1BIDS1_mapkey01_idxfn {
    FORCE_INLINE static size_t hash(const __SQL_SUM_AGGREGATE_1BIDS1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B2_T);
      hash_combine(h, e.B2_PRICE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const __SQL_SUM_AGGREGATE_1BIDS1_entry& x, const __SQL_SUM_AGGREGATE_1BIDS1_entry& y) {
      return x.B2_T == y.B2_T && x.B2_PRICE == y.B2_PRICE;
    }
  };
  
  typedef MultiHashMap<__SQL_SUM_AGGREGATE_1BIDS1_entry, long, 
    PrimaryHashIndex<__SQL_SUM_AGGREGATE_1BIDS1_entry, __SQL_SUM_AGGREGATE_1BIDS1_mapkey01_idxfn>
  > __SQL_SUM_AGGREGATE_1BIDS1_map;
  
  
  struct DELTA_BIDS_entry {
    DoubleType bids_t; long bids_id; long bids_broker_id; DoubleType bids_volume; DoubleType bids_price; long __av; DELTA_BIDS_entry* nxt; DELTA_BIDS_entry* prv;
  
    explicit DELTA_BIDS_entry() : nxt(nullptr), prv(nullptr) { }
    explicit DELTA_BIDS_entry(const DoubleType c0, const long c1, const long c2, const DoubleType c3, const DoubleType c4, const long c5) { bids_t = c0; bids_id = c1; bids_broker_id = c2; bids_volume = c3; bids_price = c4; __av = c5;  }
    DELTA_BIDS_entry(const DELTA_BIDS_entry& other) : bids_t(other.bids_t), bids_id(other.bids_id), bids_broker_id(other.bids_broker_id), bids_volume(other.bids_volume), bids_price(other.bids_price), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE DELTA_BIDS_entry& modify(const DoubleType c0, const long c1, const long c2, const DoubleType c3, const DoubleType c4) { bids_t = c0; bids_id = c1; bids_broker_id = c2; bids_volume = c3; bids_price = c4;  return *this; }
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, bids_t, STRING(bids_t));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, bids_id, STRING(bids_id));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, bids_broker_id, STRING(bids_broker_id));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, bids_volume, STRING(bids_volume));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, bids_price, STRING(bids_price));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, __av, STRING(__av));
    }
  };
  
  struct DELTA_BIDS_mapkey0tstart_idxfn {
    FORCE_INLINE static size_t hash(const DELTA_BIDS_entry& e) {
      size_t h = 0;
      hash_combine(h, e.bids_t);
      hash_combine(h, e.bids_id);
      hash_combine(h, e.bids_broker_id);
      hash_combine(h, e.bids_volume);
      hash_combine(h, e.bids_price);
      return h;
    }
    
    FORCE_INLINE static bool equals(const DELTA_BIDS_entry& x, const DELTA_BIDS_entry& y) {
      return x.bids_t == y.bids_t && x.bids_id == y.bids_id && x.bids_broker_id == y.bids_broker_id && x.bids_volume == y.bids_volume && x.bids_price == y.bids_price;
    }
  };
  
  typedef MultiHashMap<DELTA_BIDS_entry, long, 
    PrimaryHashIndex<DELTA_BIDS_entry, DELTA_BIDS_mapkey0tstart_idxfn>
  > DELTA_BIDS_map;

  

  /* Defines top-level materialized views */
  struct tlq_t {
    struct timeval t0, t;
    unsigned long long tT, tN, tS;
  
    tlq_t(): tN(0), tS(0) , __SQL_SUM_AGGREGATE_1(0L) { 
      gettimeofday(&t0, NULL); 
    }
  
    /* Serialization code */
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << "\n";
      const long ___SQL_SUM_AGGREGATE_1 = get___SQL_SUM_AGGREGATE_1();
      dbtoaster::serialization::serialize(ar, ___SQL_SUM_AGGREGATE_1, STRING(__SQL_SUM_AGGREGATE_1), "\t");
    }
  
    /* Functions returning / computing the results of top level queries */
    long get___SQL_SUM_AGGREGATE_1() const {
      return __SQL_SUM_AGGREGATE_1;
    }
  
  protected:
    /* Data structures used for storing / computing top-level queries */
    long __SQL_SUM_AGGREGATE_1;
    
  };

  /* Contains materialized views and processing (IVM) logic */
  struct data_t : tlq_t {
  
    data_t(): tlq_t(), __SQL_SUM_AGGREGATE_1BIDS3_DELTA(0L) {
      
      
    }
  
    
  
    
  
    static constexpr bool kOrderedDataset = false;
    static constexpr bool kBatchModeActive = true;
    
    struct BidsAdaptor {
      using MessageType = Message<std::tuple<DoubleType, long, long, DoubleType, DoubleType>>;
    
      static constexpr Relation relation() {
        return Relation(1, "BIDS", RelationType::kStream);
      }
    
      static constexpr CStringMap<4> params() {
        return CStringMap<4>{{ { { "book", "bids" }, { "brokers", "10" }, { "deterministic", "yes" }, { "deletions", "false" } } }};
      }
    };
  
    /* Trigger functions for table relations */
    
    
    /* Trigger functions for stream relations */
    void on_batch_update_BIDS(const vector<BatchMessage<BidsAdaptor::MessageType, int>::KVpair>& batch) {
      
      tN += batch.size();
      __SQL_SUM_AGGREGATE_1BIDS3_DELTA = 0L;
      { //foreach
        for (auto& e1 : batch) {  
          const DoubleType b1_t = get<0>(e1.first.content);
          const long b1_id = get<1>(e1.first.content);
          const long b1_broker_id = get<2>(e1.first.content);
          const DoubleType b1_volume = get<3>(e1.first.content);
          const DoubleType b1_price = get<4>(e1.first.content);
          const long v1 = e1.second;
          { //foreach
            for (auto& e2 : batch) {  
              const DoubleType b2_t = get<0>(e2.first.content);
              const long b2_id = get<1>(e2.first.content);
              const long b2_broker_id = get<2>(e2.first.content);
              const DoubleType b2_volume = get<3>(e2.first.content);
              const DoubleType b2_price = get<4>(e2.first.content);
              const long v2 = e2.second;
              if ((tend > b1_t) && (b1_t > tstart) && (b1_price > b2_price) && (b2_t > b1_t) && (tend > b2_t) && b2_t > tstart) {
                __SQL_SUM_AGGREGATE_1BIDS3_DELTA += (v1 * v2);
              
              
              
              
              
              }
            }
          }
        }
      }
      
      __SQL_SUM_AGGREGATE_1BIDS1_DELTA.clear();
      { //foreach
        for (auto& e3 : batch) {  
          const DoubleType b1_t = get<0>(e3.first.content);
          const long b1_id = get<1>(e3.first.content);
          const long b1_broker_id = get<2>(e3.first.content);
          const DoubleType b1_volume = get<3>(e3.first.content);
          const DoubleType b1_price = get<4>(e3.first.content);
          const long v3 = e3.second;
          if ((tend > b1_t) && b1_t > tstart) {
            __SQL_SUM_AGGREGATE_1BIDS1_DELTA.addOrDelOnZero(se2.modify(b1_t, b1_price), v3);
          
          }
        }
      }
      
      long agg1 = 0L;
      { //foreach
        __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry* e4 = __SQL_SUM_AGGREGATE_1BIDS1_DELTA.head;
        while (e4) {
          const DoubleType b1_t = e4->B1_T;
          const DoubleType b1_price = e4->B1_PRICE;
          const long v4 = e4->__av;
          { //foreach
            __SQL_SUM_AGGREGATE_1BIDS1_entry* e5 = __SQL_SUM_AGGREGATE_1BIDS1.head;
            while (e5) {
              const DoubleType b2_t = e5->B2_T;
              const DoubleType b2_price = e5->B2_PRICE;
              const long v5 = e5->__av;
              (/*if */(b1_price > b2_price && b2_t > b1_t) ? agg1 += (v4 * v5) : 0L);
              e5 = e5->nxt;
            }
          }
          e4 = e4->nxt;
        }
      }
      long agg2 = 0L;
      { //foreach
        __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry* e6 = __SQL_SUM_AGGREGATE_1BIDS1_DELTA.head;
        while (e6) {
          const DoubleType b2_t = e6->B1_T;
          const DoubleType b2_price = e6->B1_PRICE;
          const long v6 = e6->__av;
          { //foreach
            __SQL_SUM_AGGREGATE_1BIDS1_entry* e7 = __SQL_SUM_AGGREGATE_1BIDS1.head;
            while (e7) {
              const DoubleType b1_t = e7->B2_T;
              const DoubleType b1_price = e7->B2_PRICE;
              const long v7 = e7->__av;
              (/*if */(b1_price > b2_price && b2_t > b1_t) ? agg2 += (v6 * v7) : 0L);
              e7 = e7->nxt;
            }
          }
          e6 = e6->nxt;
        }
      }
      __SQL_SUM_AGGREGATE_1 += (agg1 + (agg2 + __SQL_SUM_AGGREGATE_1BIDS3_DELTA));
      
      { //foreach
        __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry* e8 = __SQL_SUM_AGGREGATE_1BIDS1_DELTA.head;
        while (e8) {
          const DoubleType b2_t = e8->B1_T;
          const DoubleType b2_price = e8->B1_PRICE;
          const long v8 = e8->__av;
          __SQL_SUM_AGGREGATE_1BIDS1.addOrDelOnZero(se4.modify(b2_t, b2_price), v8);
          e8 = e8->nxt;
        }
      }
    }
    
    
    void on_system_ready_event() {
      
    }
  
    template <class Visitor>
    void registerMaps(Visitor& visitor) {
      visitor.template addMap<long>("__SQL_SUM_AGGREGATE_1", __SQL_SUM_AGGREGATE_1, true);
      visitor.template addMap<__SQL_SUM_AGGREGATE_1BIDS1_DELTA_map>("__SQL_SUM_AGGREGATE_1BIDS1_DELTA", __SQL_SUM_AGGREGATE_1BIDS1_DELTA, false);
      visitor.template addMap<__SQL_SUM_AGGREGATE_1BIDS1_map>("__SQL_SUM_AGGREGATE_1BIDS1", __SQL_SUM_AGGREGATE_1BIDS1, false);
      visitor.template addMap<long>("__SQL_SUM_AGGREGATE_1BIDS3_DELTA", __SQL_SUM_AGGREGATE_1BIDS3_DELTA, false);
      visitor.template addMap<DELTA_BIDS_map>("DELTA_BIDS", DELTA_BIDS, false);
    }
  
    template <class Visitor>
    static void registerSources(Visitor& visitor) {
      visitor.template addSource(
        "ORDERBOOK",
        FileSource("examples/data/finance.csv"),
        BidsAdaptor{}
      );
    }
  
    void process_stream_event(const Event& e) {
      if (e.id == Event::getId(BidsAdaptor::relation().id, EventType::kBatchUpdate)) {
        auto* msg = static_cast<BatchMessage<BidsAdaptor::MessageType, int>*>(e.message.get());
        on_batch_update_BIDS(msg->content);
      }
    }
  
    void process_table_event(const Event& e) {
      
    }
    double tstart, tend;
  private:
    
      /* Preallocated map entries (to avoid recreation of temporary objects) */
      __SQL_SUM_AGGREGATE_1BIDS1_DELTA_entry se2;
      __SQL_SUM_AGGREGATE_1BIDS1_entry se4;
    
      
    
      /* Data structures used for storing materialized views */
      __SQL_SUM_AGGREGATE_1BIDS1_DELTA_map __SQL_SUM_AGGREGATE_1BIDS1_DELTA;
      __SQL_SUM_AGGREGATE_1BIDS1_map __SQL_SUM_AGGREGATE_1BIDS1;
      long __SQL_SUM_AGGREGATE_1BIDS3_DELTA;
      DELTA_BIDS_map DELTA_BIDS;
    
      
    
  };
  

  

}