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
  struct VWAP_entry {
    DoubleType B1_T; DoubleType __av; VWAP_entry* nxt; VWAP_entry* prv;
  
    explicit VWAP_entry() : nxt(nullptr), prv(nullptr) { }
    explicit VWAP_entry(const DoubleType c0, const DoubleType c1) { B1_T = c0; __av = c1;  }
    VWAP_entry(const VWAP_entry& other) : B1_T(other.B1_T), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE VWAP_entry& modify(const DoubleType c0) { B1_T = c0;  return *this; }
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_T, STRING(B1_T));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, __av, STRING(__av));
    }
  };
  
  struct VWAP_mapkey0_idxfn {
    FORCE_INLINE static size_t hash(const VWAP_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B1_T);
      return h;
    }
    
    FORCE_INLINE static bool equals(const VWAP_entry& x, const VWAP_entry& y) {
      return x.B1_T == y.B1_T;
    }
  };
  
  typedef MultiHashMap<VWAP_entry, DoubleType, 
    PrimaryHashIndex<VWAP_entry, VWAP_mapkey0_idxfn>
  > VWAP_map;
  
  
  struct VWAPBIDS1_entry {
    DoubleType B1_T; DoubleType B1_PRICE; DoubleType __av; VWAPBIDS1_entry* nxt; VWAPBIDS1_entry* prv;
  
    explicit VWAPBIDS1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit VWAPBIDS1_entry(const DoubleType c0, const DoubleType c1, const DoubleType c2) { B1_T = c0; B1_PRICE = c1; __av = c2;  }
    VWAPBIDS1_entry(const VWAPBIDS1_entry& other) : B1_T(other.B1_T), B1_PRICE(other.B1_PRICE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE VWAPBIDS1_entry& modify(const DoubleType c0, const DoubleType c1) { B1_T = c0; B1_PRICE = c1;  return *this; }
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
  
  struct VWAPBIDS1_mapkey01_idxfn {
    FORCE_INLINE static size_t hash(const VWAPBIDS1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B1_T);
      hash_combine(h, e.B1_PRICE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const VWAPBIDS1_entry& x, const VWAPBIDS1_entry& y) {
      return x.B1_T == y.B1_T && x.B1_PRICE == y.B1_PRICE;
    }
  };
  
  typedef MultiHashMap<VWAPBIDS1_entry, DoubleType, 
    PrimaryHashIndex<VWAPBIDS1_entry, VWAPBIDS1_mapkey01_idxfn>
  > VWAPBIDS1_map;
  
  
  struct VWAPBIDS1BIDS1_DELTA_entry {
    DoubleType B1_T; DoubleType B1_PRICE; DoubleType __av; VWAPBIDS1BIDS1_DELTA_entry* nxt; VWAPBIDS1BIDS1_DELTA_entry* prv;
  
    explicit VWAPBIDS1BIDS1_DELTA_entry() : nxt(nullptr), prv(nullptr) { }
    explicit VWAPBIDS1BIDS1_DELTA_entry(const DoubleType c0, const DoubleType c1, const DoubleType c2) { B1_T = c0; B1_PRICE = c1; __av = c2;  }
    VWAPBIDS1BIDS1_DELTA_entry(const VWAPBIDS1BIDS1_DELTA_entry& other) : B1_T(other.B1_T), B1_PRICE(other.B1_PRICE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE VWAPBIDS1BIDS1_DELTA_entry& modify(const DoubleType c0, const DoubleType c1) { B1_T = c0; B1_PRICE = c1;  return *this; }
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
  
  struct VWAPBIDS1BIDS1_DELTA_mapkey01_idxfn {
    FORCE_INLINE static size_t hash(const VWAPBIDS1BIDS1_DELTA_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B1_T);
      hash_combine(h, e.B1_PRICE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const VWAPBIDS1BIDS1_DELTA_entry& x, const VWAPBIDS1BIDS1_DELTA_entry& y) {
      return x.B1_T == y.B1_T && x.B1_PRICE == y.B1_PRICE;
    }
  };
  
  typedef MultiHashMap<VWAPBIDS1BIDS1_DELTA_entry, DoubleType, 
    PrimaryHashIndex<VWAPBIDS1BIDS1_DELTA_entry, VWAPBIDS1BIDS1_DELTA_mapkey01_idxfn>
  > VWAPBIDS1BIDS1_DELTA_map;
  
  
  struct VWAPBIDS1_L1_1_entry {
    DoubleType B2_T; DoubleType B2_PRICE; DoubleType __av; VWAPBIDS1_L1_1_entry* nxt; VWAPBIDS1_L1_1_entry* prv;
  
    explicit VWAPBIDS1_L1_1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit VWAPBIDS1_L1_1_entry(const DoubleType c0, const DoubleType c1, const DoubleType c2) { B2_T = c0; B2_PRICE = c1; __av = c2;  }
    VWAPBIDS1_L1_1_entry(const VWAPBIDS1_L1_1_entry& other) : B2_T(other.B2_T), B2_PRICE(other.B2_PRICE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE VWAPBIDS1_L1_1_entry& modify(const DoubleType c0, const DoubleType c1) { B2_T = c0; B2_PRICE = c1;  return *this; }
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
  
  struct VWAPBIDS1_L1_1_mapkey01_idxfn {
    FORCE_INLINE static size_t hash(const VWAPBIDS1_L1_1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B2_T);
      hash_combine(h, e.B2_PRICE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const VWAPBIDS1_L1_1_entry& x, const VWAPBIDS1_L1_1_entry& y) {
      return x.B2_T == y.B2_T && x.B2_PRICE == y.B2_PRICE;
    }
  };
  
  typedef MultiHashMap<VWAPBIDS1_L1_1_entry, DoubleType, 
    PrimaryHashIndex<VWAPBIDS1_L1_1_entry, VWAPBIDS1_L1_1_mapkey01_idxfn>
  > VWAPBIDS1_L1_1_map;
  
  
  struct VWAPBIDS1_L1_1BIDS1_DELTA_entry {
    DoubleType B2_T; DoubleType B2_PRICE; DoubleType __av; VWAPBIDS1_L1_1BIDS1_DELTA_entry* nxt; VWAPBIDS1_L1_1BIDS1_DELTA_entry* prv;
  
    explicit VWAPBIDS1_L1_1BIDS1_DELTA_entry() : nxt(nullptr), prv(nullptr) { }
    explicit VWAPBIDS1_L1_1BIDS1_DELTA_entry(const DoubleType c0, const DoubleType c1, const DoubleType c2) { B2_T = c0; B2_PRICE = c1; __av = c2;  }
    VWAPBIDS1_L1_1BIDS1_DELTA_entry(const VWAPBIDS1_L1_1BIDS1_DELTA_entry& other) : B2_T(other.B2_T), B2_PRICE(other.B2_PRICE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE VWAPBIDS1_L1_1BIDS1_DELTA_entry& modify(const DoubleType c0, const DoubleType c1) { B2_T = c0; B2_PRICE = c1;  return *this; }
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
  
  struct VWAPBIDS1_L1_1BIDS1_DELTA_mapkey01_idxfn {
    FORCE_INLINE static size_t hash(const VWAPBIDS1_L1_1BIDS1_DELTA_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B2_T);
      hash_combine(h, e.B2_PRICE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const VWAPBIDS1_L1_1BIDS1_DELTA_entry& x, const VWAPBIDS1_L1_1BIDS1_DELTA_entry& y) {
      return x.B2_T == y.B2_T && x.B2_PRICE == y.B2_PRICE;
    }
  };
  
  typedef MultiHashMap<VWAPBIDS1_L1_1BIDS1_DELTA_entry, DoubleType, 
    PrimaryHashIndex<VWAPBIDS1_L1_1BIDS1_DELTA_entry, VWAPBIDS1_L1_1BIDS1_DELTA_mapkey01_idxfn>
  > VWAPBIDS1_L1_1BIDS1_DELTA_map;
  
  
  struct VWAPBIDS1_L2_1_entry {
    DoubleType B3_T; DoubleType __av; VWAPBIDS1_L2_1_entry* nxt; VWAPBIDS1_L2_1_entry* prv;
  
    explicit VWAPBIDS1_L2_1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit VWAPBIDS1_L2_1_entry(const DoubleType c0, const DoubleType c1) { B3_T = c0; __av = c1;  }
    VWAPBIDS1_L2_1_entry(const VWAPBIDS1_L2_1_entry& other) : B3_T(other.B3_T), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE VWAPBIDS1_L2_1_entry& modify(const DoubleType c0) { B3_T = c0;  return *this; }
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B3_T, STRING(B3_T));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, __av, STRING(__av));
    }
  };
  
  struct VWAPBIDS1_L2_1_mapkey0_idxfn {
    FORCE_INLINE static size_t hash(const VWAPBIDS1_L2_1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B3_T);
      return h;
    }
    
    FORCE_INLINE static bool equals(const VWAPBIDS1_L2_1_entry& x, const VWAPBIDS1_L2_1_entry& y) {
      return x.B3_T == y.B3_T;
    }
  };
  
  typedef MultiHashMap<VWAPBIDS1_L2_1_entry, DoubleType, 
    PrimaryHashIndex<VWAPBIDS1_L2_1_entry, VWAPBIDS1_L2_1_mapkey0_idxfn>
  > VWAPBIDS1_L2_1_map;
  
  
  struct VWAPBIDS1_L2_1BIDS1_DELTA_entry {
    DoubleType B3_T; DoubleType __av; VWAPBIDS1_L2_1BIDS1_DELTA_entry* nxt; VWAPBIDS1_L2_1BIDS1_DELTA_entry* prv;
  
    explicit VWAPBIDS1_L2_1BIDS1_DELTA_entry() : nxt(nullptr), prv(nullptr) { }
    explicit VWAPBIDS1_L2_1BIDS1_DELTA_entry(const DoubleType c0, const DoubleType c1) { B3_T = c0; __av = c1;  }
    VWAPBIDS1_L2_1BIDS1_DELTA_entry(const VWAPBIDS1_L2_1BIDS1_DELTA_entry& other) : B3_T(other.B3_T), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE VWAPBIDS1_L2_1BIDS1_DELTA_entry& modify(const DoubleType c0) { B3_T = c0;  return *this; }
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B3_T, STRING(B3_T));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, __av, STRING(__av));
    }
  };
  
  struct VWAPBIDS1_L2_1BIDS1_DELTA_mapkey0_idxfn {
    FORCE_INLINE static size_t hash(const VWAPBIDS1_L2_1BIDS1_DELTA_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B3_T);
      return h;
    }
    
    FORCE_INLINE static bool equals(const VWAPBIDS1_L2_1BIDS1_DELTA_entry& x, const VWAPBIDS1_L2_1BIDS1_DELTA_entry& y) {
      return x.B3_T == y.B3_T;
    }
  };
  
  typedef MultiHashMap<VWAPBIDS1_L2_1BIDS1_DELTA_entry, DoubleType, 
    PrimaryHashIndex<VWAPBIDS1_L2_1BIDS1_DELTA_entry, VWAPBIDS1_L2_1BIDS1_DELTA_mapkey0_idxfn>
  > VWAPBIDS1_L2_1BIDS1_DELTA_map;
  
  
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
  
  struct DELTA_BIDS_mapkey01234_idxfn {
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
    PrimaryHashIndex<DELTA_BIDS_entry, DELTA_BIDS_mapkey01234_idxfn>
  > DELTA_BIDS_map;

  

  /* Defines top-level materialized views */
  struct tlq_t {
    struct timeval t0, t;
    unsigned long long tT, tN, tS;
  
    tlq_t(): tN(0), tS(0)  { 
      gettimeofday(&t0, NULL); 
    }
  
    /* Serialization code */
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << "\n";
      const VWAP_map& _VWAP = get_VWAP();
      dbtoaster::serialization::serialize(ar, _VWAP, STRING(VWAP), "\t");
    }
  
    /* Functions returning / computing the results of top level queries */
    const VWAP_map& get_VWAP() const {
      return VWAP;
    }
  
  protected:
    /* Data structures used for storing / computing top-level queries */
    VWAP_map VWAP;
    
  };

  /* Contains materialized views and processing (IVM) logic */
  struct data_t : tlq_t {
  
    data_t(): tlq_t() {
      
      
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
      VWAPBIDS1BIDS1_DELTA.clear();
      { //foreach
        for (auto& e1 : batch) {  
          const DoubleType b1_t = get<0>(e1.first.content);
          const long b1_id = get<1>(e1.first.content);
          const long b1_broker_id = get<2>(e1.first.content);
          const DoubleType b1_volume = get<3>(e1.first.content);
          const DoubleType b1_price = get<4>(e1.first.content);
          const long v1 = e1.second;
          VWAPBIDS1BIDS1_DELTA.addOrDelOnZero(se1.modify(b1_t, b1_price), (v1 * (b1_price * b1_volume)));
        }
      }
      
      VWAPBIDS1_L1_1BIDS1_DELTA.clear();
      { //foreach
        for (auto& e2 : batch) {  
          const DoubleType b2_t = get<0>(e2.first.content);
          const long b2_id = get<1>(e2.first.content);
          const long b2_broker_id = get<2>(e2.first.content);
          const DoubleType b2_volume = get<3>(e2.first.content);
          const DoubleType b2_price = get<4>(e2.first.content);
          const long v2 = e2.second;
          VWAPBIDS1_L1_1BIDS1_DELTA.addOrDelOnZero(se2.modify(b2_t, b2_price), (v2 * b2_volume));
        }
      }
      
      VWAPBIDS1_L2_1BIDS1_DELTA.clear();
      { //foreach
        for (auto& e3 : batch) {  
          const DoubleType b3_t = get<0>(e3.first.content);
          const long b3_id = get<1>(e3.first.content);
          const long b3_broker_id = get<2>(e3.first.content);
          const DoubleType b3_volume = get<3>(e3.first.content);
          const DoubleType b3_price = get<4>(e3.first.content);
          const long v3 = e3.second;
          VWAPBIDS1_L2_1BIDS1_DELTA.addOrDelOnZero(se3.modify(b3_t), (v3 * b3_volume));
        }
      }
      
      { //foreach
        VWAPBIDS1BIDS1_DELTA_entry* e4 = VWAPBIDS1BIDS1_DELTA.head;
        while (e4) {
          const DoubleType b1_t = e4->B1_T;
          const DoubleType b1_price = e4->B1_PRICE;
          const DoubleType v4 = e4->__av;
          VWAPBIDS1.addOrDelOnZero(se4.modify(b1_t, b1_price), v4);
          e4 = e4->nxt;
        }
      }
      
      { //foreach
        VWAPBIDS1_L1_1BIDS1_DELTA_entry* e5 = VWAPBIDS1_L1_1BIDS1_DELTA.head;
        while (e5) {
          const DoubleType b2_t = e5->B2_T;
          const DoubleType b2_price = e5->B2_PRICE;
          const DoubleType v5 = e5->__av;
          VWAPBIDS1_L1_1.addOrDelOnZero(se5.modify(b2_t, b2_price), v5);
          e5 = e5->nxt;
        }
      }
      
      { //foreach
        VWAPBIDS1_L2_1BIDS1_DELTA_entry* e6 = VWAPBIDS1_L2_1BIDS1_DELTA.head;
        while (e6) {
          const DoubleType b3_t = e6->B3_T;
          const DoubleType v6 = e6->__av;
          VWAPBIDS1_L2_1.addOrDelOnZero(se6.modify(b3_t), v6);
          e6 = e6->nxt;
        }
      }
      
      VWAP.clear();
      { //foreach
        VWAPBIDS1_entry* e7 = VWAPBIDS1.head;
        while (e7) {
          const DoubleType b1_t = e7->B1_T;
          const DoubleType b1_price = e7->B1_PRICE;
          const DoubleType v7 = e7->__av;
          DoubleType agg1 = 0.0;
          { //foreach
            VWAPBIDS1_L1_1_entry* e8 = VWAPBIDS1_L1_1.head;
            while (e8) {
              const DoubleType b2_t = e8->B2_T;
              const DoubleType b2_price = e8->B2_PRICE;
              const DoubleType v8 = e8->__av;
              if (b1_t >= b2_t) {
                (/*if */(b1_price > b2_price) ? agg1 += v8 : 0.0);
              }
              e8 = e8->nxt;
            }
          }
          DoubleType l1 = agg1;
          DoubleType agg2 = 0.0;
          { //foreach
            VWAPBIDS1_L2_1_entry* e9 = VWAPBIDS1_L2_1.head;
            while (e9) {
              const DoubleType b3_t = e9->B3_T;
              const DoubleType v9 = e9->__av;
              (/*if */(b1_t >= b3_t) ? agg2 += v9 : 0.0);
              e9 = e9->nxt;
            }
          }
          DoubleType l2 = (agg2 * 0.25);
          if (l1 > l2) {
            VWAP.addOrDelOnZero(se7.modify(b1_t), v7);
          }
          e7 = e7->nxt;
        }
      }
    }
    
    
    void on_system_ready_event() {
      
    }
  
    template <class Visitor>
    void registerMaps(Visitor& visitor) {
      visitor.template addMap<VWAP_map>("VWAP", VWAP, true);
      visitor.template addMap<VWAPBIDS1_map>("VWAPBIDS1", VWAPBIDS1, false);
      visitor.template addMap<VWAPBIDS1BIDS1_DELTA_map>("VWAPBIDS1BIDS1_DELTA", VWAPBIDS1BIDS1_DELTA, false);
      visitor.template addMap<VWAPBIDS1_L1_1_map>("VWAPBIDS1_L1_1", VWAPBIDS1_L1_1, false);
      visitor.template addMap<VWAPBIDS1_L1_1BIDS1_DELTA_map>("VWAPBIDS1_L1_1BIDS1_DELTA", VWAPBIDS1_L1_1BIDS1_DELTA, false);
      visitor.template addMap<VWAPBIDS1_L2_1_map>("VWAPBIDS1_L2_1", VWAPBIDS1_L2_1, false);
      visitor.template addMap<VWAPBIDS1_L2_1BIDS1_DELTA_map>("VWAPBIDS1_L2_1BIDS1_DELTA", VWAPBIDS1_L2_1BIDS1_DELTA, false);
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
  
  private:
    
      /* Preallocated map entries (to avoid recreation of temporary objects) */
      VWAPBIDS1BIDS1_DELTA_entry se1;
      VWAPBIDS1_L1_1BIDS1_DELTA_entry se2;
      VWAPBIDS1_L2_1BIDS1_DELTA_entry se3;
      VWAPBIDS1_entry se4;
      VWAPBIDS1_L1_1_entry se5;
      VWAPBIDS1_L2_1_entry se6;
      VWAP_entry se7;
    
      
    
      /* Data structures used for storing materialized views */
      VWAPBIDS1_map VWAPBIDS1;
      VWAPBIDS1BIDS1_DELTA_map VWAPBIDS1BIDS1_DELTA;
      VWAPBIDS1_L1_1_map VWAPBIDS1_L1_1;
      VWAPBIDS1_L1_1BIDS1_DELTA_map VWAPBIDS1_L1_1BIDS1_DELTA;
      VWAPBIDS1_L2_1_map VWAPBIDS1_L2_1;
      VWAPBIDS1_L2_1BIDS1_DELTA_map VWAPBIDS1_L2_1BIDS1_DELTA;
      DELTA_BIDS_map DELTA_BIDS;
    
      
    
  };
  

  

}