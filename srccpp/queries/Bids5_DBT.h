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
  struct COUNT_entry {
    DoubleType B1_T; long B1_ID; long B1_BROKER_ID; DoubleType B1_VOLUME; DoubleType B1_PRICE; long __av; COUNT_entry* nxt; COUNT_entry* prv;
  
    explicit COUNT_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNT_entry(const DoubleType c0, const long c1, const long c2, const DoubleType c3, const DoubleType c4, const long c5) { B1_T = c0; B1_ID = c1; B1_BROKER_ID = c2; B1_VOLUME = c3; B1_PRICE = c4; __av = c5;  }
    COUNT_entry(const COUNT_entry& other) : B1_T(other.B1_T), B1_ID(other.B1_ID), B1_BROKER_ID(other.B1_BROKER_ID), B1_VOLUME(other.B1_VOLUME), B1_PRICE(other.B1_PRICE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE COUNT_entry& modify(const DoubleType c0, const long c1, const long c2, const DoubleType c3, const DoubleType c4) { B1_T = c0; B1_ID = c1; B1_BROKER_ID = c2; B1_VOLUME = c3; B1_PRICE = c4;  return *this; }
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_T, STRING(B1_T));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_ID, STRING(B1_ID));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_BROKER_ID, STRING(B1_BROKER_ID));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_VOLUME, STRING(B1_VOLUME));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_PRICE, STRING(B1_PRICE));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, __av, STRING(__av));
    }
  };
  
  struct COUNT_mapkey01234_idxfn {
    FORCE_INLINE static size_t hash(const COUNT_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B1_T);
      hash_combine(h, e.B1_ID);
      hash_combine(h, e.B1_BROKER_ID);
      hash_combine(h, e.B1_VOLUME);
      hash_combine(h, e.B1_PRICE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNT_entry& x, const COUNT_entry& y) {
      return x.B1_T == y.B1_T && x.B1_ID == y.B1_ID && x.B1_BROKER_ID == y.B1_BROKER_ID && x.B1_VOLUME == y.B1_VOLUME && x.B1_PRICE == y.B1_PRICE;
    }
  };
  
  typedef MultiHashMap<COUNT_entry, long, 
    PrimaryHashIndex<COUNT_entry, COUNT_mapkey01234_idxfn>
  > COUNT_map;
  
  
  struct COUNTBIDS1_entry {
    DoubleType B1_T; long B1_ID; long B1_BROKER_ID; DoubleType B1_VOLUME; DoubleType B1_PRICE; long __av; COUNTBIDS1_entry* nxt; COUNTBIDS1_entry* prv;
  
    explicit COUNTBIDS1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNTBIDS1_entry(const DoubleType c0, const long c1, const long c2, const DoubleType c3, const DoubleType c4, const long c5) { B1_T = c0; B1_ID = c1; B1_BROKER_ID = c2; B1_VOLUME = c3; B1_PRICE = c4; __av = c5;  }
    COUNTBIDS1_entry(const COUNTBIDS1_entry& other) : B1_T(other.B1_T), B1_ID(other.B1_ID), B1_BROKER_ID(other.B1_BROKER_ID), B1_VOLUME(other.B1_VOLUME), B1_PRICE(other.B1_PRICE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE COUNTBIDS1_entry& modify(const DoubleType c0, const long c1, const long c2, const DoubleType c3, const DoubleType c4) { B1_T = c0; B1_ID = c1; B1_BROKER_ID = c2; B1_VOLUME = c3; B1_PRICE = c4;  return *this; }
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_T, STRING(B1_T));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_ID, STRING(B1_ID));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_BROKER_ID, STRING(B1_BROKER_ID));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_VOLUME, STRING(B1_VOLUME));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_PRICE, STRING(B1_PRICE));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, __av, STRING(__av));
    }
  };
  
  struct COUNTBIDS1_mapkey01234_idxfn {
    FORCE_INLINE static size_t hash(const COUNTBIDS1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B1_T);
      hash_combine(h, e.B1_ID);
      hash_combine(h, e.B1_BROKER_ID);
      hash_combine(h, e.B1_VOLUME);
      hash_combine(h, e.B1_PRICE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNTBIDS1_entry& x, const COUNTBIDS1_entry& y) {
      return x.B1_T == y.B1_T && x.B1_ID == y.B1_ID && x.B1_BROKER_ID == y.B1_BROKER_ID && x.B1_VOLUME == y.B1_VOLUME && x.B1_PRICE == y.B1_PRICE;
    }
  };
  
  typedef MultiHashMap<COUNTBIDS1_entry, long, 
    PrimaryHashIndex<COUNTBIDS1_entry, COUNTBIDS1_mapkey01234_idxfn>
  > COUNTBIDS1_map;
  
  
  struct COUNTBIDS1BIDS1_DELTA_entry {
    DoubleType B1_T; long B1_ID; long B1_BROKER_ID; DoubleType B1_VOLUME; DoubleType B1_PRICE; long __av; COUNTBIDS1BIDS1_DELTA_entry* nxt; COUNTBIDS1BIDS1_DELTA_entry* prv;
  
    explicit COUNTBIDS1BIDS1_DELTA_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNTBIDS1BIDS1_DELTA_entry(const DoubleType c0, const long c1, const long c2, const DoubleType c3, const DoubleType c4, const long c5) { B1_T = c0; B1_ID = c1; B1_BROKER_ID = c2; B1_VOLUME = c3; B1_PRICE = c4; __av = c5;  }
    COUNTBIDS1BIDS1_DELTA_entry(const COUNTBIDS1BIDS1_DELTA_entry& other) : B1_T(other.B1_T), B1_ID(other.B1_ID), B1_BROKER_ID(other.B1_BROKER_ID), B1_VOLUME(other.B1_VOLUME), B1_PRICE(other.B1_PRICE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE COUNTBIDS1BIDS1_DELTA_entry& modify(const DoubleType c0, const long c1, const long c2, const DoubleType c3, const DoubleType c4) { B1_T = c0; B1_ID = c1; B1_BROKER_ID = c2; B1_VOLUME = c3; B1_PRICE = c4;  return *this; }
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_T, STRING(B1_T));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_ID, STRING(B1_ID));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_BROKER_ID, STRING(B1_BROKER_ID));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_VOLUME, STRING(B1_VOLUME));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B1_PRICE, STRING(B1_PRICE));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, __av, STRING(__av));
    }
  };
  
  struct COUNTBIDS1BIDS1_DELTA_mapkey01234_idxfn {
    FORCE_INLINE static size_t hash(const COUNTBIDS1BIDS1_DELTA_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B1_T);
      hash_combine(h, e.B1_ID);
      hash_combine(h, e.B1_BROKER_ID);
      hash_combine(h, e.B1_VOLUME);
      hash_combine(h, e.B1_PRICE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNTBIDS1BIDS1_DELTA_entry& x, const COUNTBIDS1BIDS1_DELTA_entry& y) {
      return x.B1_T == y.B1_T && x.B1_ID == y.B1_ID && x.B1_BROKER_ID == y.B1_BROKER_ID && x.B1_VOLUME == y.B1_VOLUME && x.B1_PRICE == y.B1_PRICE;
    }
  };
  
  typedef MultiHashMap<COUNTBIDS1BIDS1_DELTA_entry, long, 
    PrimaryHashIndex<COUNTBIDS1BIDS1_DELTA_entry, COUNTBIDS1BIDS1_DELTA_mapkey01234_idxfn>
  > COUNTBIDS1BIDS1_DELTA_map;
  
  
  struct COUNTBIDS1_L2_1_entry {
    DoubleType B2_T; DoubleType B2_PRICE; long __av; COUNTBIDS1_L2_1_entry* nxt; COUNTBIDS1_L2_1_entry* prv;
  
    explicit COUNTBIDS1_L2_1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNTBIDS1_L2_1_entry(const DoubleType c0, const DoubleType c1, const long c2) { B2_T = c0; B2_PRICE = c1; __av = c2;  }
    COUNTBIDS1_L2_1_entry(const COUNTBIDS1_L2_1_entry& other) : B2_T(other.B2_T), B2_PRICE(other.B2_PRICE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE COUNTBIDS1_L2_1_entry& modify(const DoubleType c0, const DoubleType c1) { B2_T = c0; B2_PRICE = c1;  return *this; }
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
  
  struct COUNTBIDS1_L2_1_mapkey01_idxfn {
    FORCE_INLINE static size_t hash(const COUNTBIDS1_L2_1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B2_T);
      hash_combine(h, e.B2_PRICE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNTBIDS1_L2_1_entry& x, const COUNTBIDS1_L2_1_entry& y) {
      return x.B2_T == y.B2_T && x.B2_PRICE == y.B2_PRICE;
    }
  };
  
  typedef MultiHashMap<COUNTBIDS1_L2_1_entry, long, 
    PrimaryHashIndex<COUNTBIDS1_L2_1_entry, COUNTBIDS1_L2_1_mapkey01_idxfn>
  > COUNTBIDS1_L2_1_map;
  
  
  struct COUNTBIDS1_L2_1BIDS1_DELTA_entry {
    DoubleType B2_T; DoubleType B2_PRICE; long __av; COUNTBIDS1_L2_1BIDS1_DELTA_entry* nxt; COUNTBIDS1_L2_1BIDS1_DELTA_entry* prv;
  
    explicit COUNTBIDS1_L2_1BIDS1_DELTA_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNTBIDS1_L2_1BIDS1_DELTA_entry(const DoubleType c0, const DoubleType c1, const long c2) { B2_T = c0; B2_PRICE = c1; __av = c2;  }
    COUNTBIDS1_L2_1BIDS1_DELTA_entry(const COUNTBIDS1_L2_1BIDS1_DELTA_entry& other) : B2_T(other.B2_T), B2_PRICE(other.B2_PRICE), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE COUNTBIDS1_L2_1BIDS1_DELTA_entry& modify(const DoubleType c0, const DoubleType c1) { B2_T = c0; B2_PRICE = c1;  return *this; }
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
  
  struct COUNTBIDS1_L2_1BIDS1_DELTA_mapkey01_idxfn {
    FORCE_INLINE static size_t hash(const COUNTBIDS1_L2_1BIDS1_DELTA_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B2_T);
      hash_combine(h, e.B2_PRICE);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNTBIDS1_L2_1BIDS1_DELTA_entry& x, const COUNTBIDS1_L2_1BIDS1_DELTA_entry& y) {
      return x.B2_T == y.B2_T && x.B2_PRICE == y.B2_PRICE;
    }
  };
  
  typedef MultiHashMap<COUNTBIDS1_L2_1BIDS1_DELTA_entry, long, 
    PrimaryHashIndex<COUNTBIDS1_L2_1BIDS1_DELTA_entry, COUNTBIDS1_L2_1BIDS1_DELTA_mapkey01_idxfn>
  > COUNTBIDS1_L2_1BIDS1_DELTA_map;
  
  
  struct COUNTBIDS1_L2_1_L2_1_entry {
    DoubleType B3_T; long __av; COUNTBIDS1_L2_1_L2_1_entry* nxt; COUNTBIDS1_L2_1_L2_1_entry* prv;
  
    explicit COUNTBIDS1_L2_1_L2_1_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNTBIDS1_L2_1_L2_1_entry(const DoubleType c0, const long c1) { B3_T = c0; __av = c1;  }
    COUNTBIDS1_L2_1_L2_1_entry(const COUNTBIDS1_L2_1_L2_1_entry& other) : B3_T(other.B3_T), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE COUNTBIDS1_L2_1_L2_1_entry& modify(const DoubleType c0) { B3_T = c0;  return *this; }
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B3_T, STRING(B3_T));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, __av, STRING(__av));
    }
  };
  
  struct COUNTBIDS1_L2_1_L2_1_mapkey0_idxfn {
    FORCE_INLINE static size_t hash(const COUNTBIDS1_L2_1_L2_1_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B3_T);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNTBIDS1_L2_1_L2_1_entry& x, const COUNTBIDS1_L2_1_L2_1_entry& y) {
      return x.B3_T == y.B3_T;
    }
  };
  
  typedef MultiHashMap<COUNTBIDS1_L2_1_L2_1_entry, long, 
    PrimaryHashIndex<COUNTBIDS1_L2_1_L2_1_entry, COUNTBIDS1_L2_1_L2_1_mapkey0_idxfn>
  > COUNTBIDS1_L2_1_L2_1_map;
  
  
  struct COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry {
    DoubleType B3_T; long __av; COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry* nxt; COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry* prv;
  
    explicit COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry() : nxt(nullptr), prv(nullptr) { }
    explicit COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry(const DoubleType c0, const long c1) { B3_T = c0; __av = c1;  }
    COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry(const COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry& other) : B3_T(other.B3_T), __av(other.__av), nxt(nullptr), prv(nullptr) { }
  
    
    FORCE_INLINE COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry& modify(const DoubleType c0) { B3_T = c0;  return *this; }
    template<class Archive>
    void serialize(Archive& ar) const {
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, B3_T, STRING(B3_T));
      ar << dbtoaster::serialization::kElemSeparator;
      dbtoaster::serialization::serialize(ar, __av, STRING(__av));
    }
  };
  
  struct COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_mapkey0_idxfn {
    FORCE_INLINE static size_t hash(const COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry& e) {
      size_t h = 0;
      hash_combine(h, e.B3_T);
      return h;
    }
    
    FORCE_INLINE static bool equals(const COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry& x, const COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry& y) {
      return x.B3_T == y.B3_T;
    }
  };
  
  typedef MultiHashMap<COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry, long, 
    PrimaryHashIndex<COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry, COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_mapkey0_idxfn>
  > COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_map;
  
  
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
      const COUNT_map& _COUNT = get_COUNT();
      dbtoaster::serialization::serialize(ar, _COUNT, STRING(COUNT), "\t");
    }
  
    /* Functions returning / computing the results of top level queries */
    const COUNT_map& get_COUNT() const {
      return COUNT;
    }
  
  protected:
    /* Data structures used for storing / computing top-level queries */
    COUNT_map COUNT;
    
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
      COUNTBIDS1BIDS1_DELTA.clear();
      { //foreach
        for (auto& e1 : batch) {  
          const DoubleType b1_t = get<0>(e1.first.content);
          const long b1_id = get<1>(e1.first.content);
          const long b1_broker_id = get<2>(e1.first.content);
          const DoubleType b1_volume = get<3>(e1.first.content);
          const DoubleType b1_price = get<4>(e1.first.content);
          const long v1 = e1.second;
          COUNTBIDS1BIDS1_DELTA.addOrDelOnZero(se1.modify(b1_t, b1_id, b1_broker_id, b1_volume, b1_price), v1);
        }
      }
      
      COUNTBIDS1_L2_1BIDS1_DELTA.clear();
      { //foreach
        for (auto& e2 : batch) {  
          const DoubleType b2_t = get<0>(e2.first.content);
          const long b2_id = get<1>(e2.first.content);
          const long b2_broker_id = get<2>(e2.first.content);
          const DoubleType b2_volume = get<3>(e2.first.content);
          const DoubleType b2_price = get<4>(e2.first.content);
          const long v2 = e2.second;
          COUNTBIDS1_L2_1BIDS1_DELTA.addOrDelOnZero(se2.modify(b2_t, b2_price), v2);
        }
      }
      
      COUNTBIDS1_L2_1_L2_1BIDS1_DELTA.clear();
      { //foreach
        for (auto& e3 : batch) {  
          const DoubleType b3_t = get<0>(e3.first.content);
          const long b3_id = get<1>(e3.first.content);
          const long b3_broker_id = get<2>(e3.first.content);
          const DoubleType b3_volume = get<3>(e3.first.content);
          const DoubleType b3_price = get<4>(e3.first.content);
          const long v3 = e3.second;
          COUNTBIDS1_L2_1_L2_1BIDS1_DELTA.addOrDelOnZero(se3.modify(b3_t), v3);
        }
      }
      
      { //foreach
        COUNTBIDS1BIDS1_DELTA_entry* e4 = COUNTBIDS1BIDS1_DELTA.head;
        while (e4) {
          const DoubleType b1_t = e4->B1_T;
          const long b1_id = e4->B1_ID;
          const long b1_broker_id = e4->B1_BROKER_ID;
          const DoubleType b1_volume = e4->B1_VOLUME;
          const DoubleType b1_price = e4->B1_PRICE;
          const long v4 = e4->__av;
          COUNTBIDS1.addOrDelOnZero(se4.modify(b1_t, b1_id, b1_broker_id, b1_volume, b1_price), v4);
          e4 = e4->nxt;
        }
      }
      
      { //foreach
        COUNTBIDS1_L2_1BIDS1_DELTA_entry* e5 = COUNTBIDS1_L2_1BIDS1_DELTA.head;
        while (e5) {
          const DoubleType b2_t = e5->B2_T;
          const DoubleType b2_price = e5->B2_PRICE;
          const long v5 = e5->__av;
          COUNTBIDS1_L2_1.addOrDelOnZero(se5.modify(b2_t, b2_price), v5);
          e5 = e5->nxt;
        }
      }
      
      { //foreach
        COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry* e6 = COUNTBIDS1_L2_1_L2_1BIDS1_DELTA.head;
        while (e6) {
          const DoubleType b3_t = e6->B3_T;
          const long v6 = e6->__av;
          COUNTBIDS1_L2_1_L2_1.addOrDelOnZero(se6.modify(b3_t), v6);
          e6 = e6->nxt;
        }
      }
      
      COUNT.clear();
      { //foreach
        COUNTBIDS1_entry* e7 = COUNTBIDS1.head;
        while (e7) {
          const DoubleType b1_t = e7->B1_T;
          const long b1_id = e7->B1_ID;
          const long b1_broker_id = e7->B1_BROKER_ID;
          const DoubleType b1_volume = e7->B1_VOLUME;
          const DoubleType b1_price = e7->B1_PRICE;
          const long v7 = e7->__av;
          int agg1 = 0;
          long agg2 = 0L;
          int l2 = 0;
          { //foreach
            COUNTBIDS1_L2_1_entry* e8 = COUNTBIDS1_L2_1.head;
            while (e8) {
              const DoubleType b2_t = e8->B2_T;
              const DoubleType b2_price = e8->B2_PRICE;
              const long v8 = e8->__av;
              if ((b1_t > b2_t) && (1.1 * b2_price) > b1_price) {
                long agg3 = 0L;
                { //foreach
                  COUNTBIDS1_L2_1_L2_1_entry* e9 = COUNTBIDS1_L2_1_L2_1.head;
                  while (e9) {
                    const DoubleType b3_t = e9->B3_T;
                    const long v9 = e9->__av;
                    if (b3_t > b2_t) {
                      (/*if */(b1_t > b3_t) ? agg3 += v9 : 0L);
                    }
                    e9 = e9->nxt;
                  }
                }
                (/*if */(l2 == agg3) ? agg2 += v8 : 0L);
              
              }
              e8 = e8->nxt;
            }
          }
          long l1 = agg2;
          (/*if */(l1 == 0) ? agg1 += 1 : 0);
          COUNT.addOrDelOnZero(se7.modify(b1_t, b1_id, b1_broker_id, b1_volume, b1_price), (v7 * agg1));
          e7 = e7->nxt;
        }
      }
    }
    
    
    void on_system_ready_event() {
      
    }
  
    template <class Visitor>
    void registerMaps(Visitor& visitor) {
      visitor.template addMap<COUNT_map>("COUNT", COUNT, true);
      visitor.template addMap<COUNTBIDS1_map>("COUNTBIDS1", COUNTBIDS1, false);
      visitor.template addMap<COUNTBIDS1BIDS1_DELTA_map>("COUNTBIDS1BIDS1_DELTA", COUNTBIDS1BIDS1_DELTA, false);
      visitor.template addMap<COUNTBIDS1_L2_1_map>("COUNTBIDS1_L2_1", COUNTBIDS1_L2_1, false);
      visitor.template addMap<COUNTBIDS1_L2_1BIDS1_DELTA_map>("COUNTBIDS1_L2_1BIDS1_DELTA", COUNTBIDS1_L2_1BIDS1_DELTA, false);
      visitor.template addMap<COUNTBIDS1_L2_1_L2_1_map>("COUNTBIDS1_L2_1_L2_1", COUNTBIDS1_L2_1_L2_1, false);
      visitor.template addMap<COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_map>("COUNTBIDS1_L2_1_L2_1BIDS1_DELTA", COUNTBIDS1_L2_1_L2_1BIDS1_DELTA, false);
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
      COUNTBIDS1BIDS1_DELTA_entry se1;
      COUNTBIDS1_L2_1BIDS1_DELTA_entry se2;
      COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_entry se3;
      COUNTBIDS1_entry se4;
      COUNTBIDS1_L2_1_entry se5;
      COUNTBIDS1_L2_1_L2_1_entry se6;
      COUNT_entry se7;
    
      
    
      /* Data structures used for storing materialized views */
      COUNTBIDS1_map COUNTBIDS1;
      COUNTBIDS1BIDS1_DELTA_map COUNTBIDS1BIDS1_DELTA;
      COUNTBIDS1_L2_1_map COUNTBIDS1_L2_1;
      COUNTBIDS1_L2_1BIDS1_DELTA_map COUNTBIDS1_L2_1BIDS1_DELTA;
      COUNTBIDS1_L2_1_L2_1_map COUNTBIDS1_L2_1_L2_1;
      COUNTBIDS1_L2_1_L2_1BIDS1_DELTA_map COUNTBIDS1_L2_1_L2_1BIDS1_DELTA;
      DELTA_BIDS_map DELTA_BIDS;
    
      
    
  };
  

  

}