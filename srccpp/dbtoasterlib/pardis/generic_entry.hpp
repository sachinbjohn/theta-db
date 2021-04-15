#ifndef DBTOASTER_PARDIS_GENTRY_HPP
#define DBTOASTER_PARDIS_GENTRY_HPP
#include <ostream>
#include <unordered_map>
#include <cmath>
#include <type_traits>
#include "macro.hpp"
#include "types.hpp"
#include "serialization.hpp"

using namespace std;

namespace dbtoaster {

namespace pardis {

typedef unsigned int uint;      // necessary on macOS with gcc 6.4.0
typedef std::string PString;

template <class GE, typename T>
void processSampleEntry(GE* e, const int& col, const T& arg) {
  e->update(col, arg);
  e->backPtrs = new void*[col + 1];
  for (int i = 0; i < col + 1; i++)
    e->backPtrs[i] = nullptr;
}

template <class GE, typename T, typename... Args>
void processSampleEntry(GE* e, const int& col, const T& arg, const Args&... args) {
  e->update(col, arg);
  processSampleEntry(e, args...);
}

template <class GE, typename T>
void processFullEntry(GE* e, int col, const T& arg) {
  e->update(col, arg);
  e->backPtrs = new void*[col + 1];
  for (int i = 0; i < col + 1; i++)
    e->backPtrs[i] = nullptr;
}

template <class GE, typename T, typename... Args>
void processFullEntry(GE* e, int col, const T& arg, const Args&... args) {
  e->update(col, arg);
  processFullEntry(e, col + 1, args...);
}

enum AnyType : char {
  UNDEFINED, INT, LONG, DOUBLE, STRING, CHAR, DATE
};

union AnyUnion {
  int i;
  long l;
  double d;
  PString s;
  char c;
  DateType date;

  AnyUnion() {
    memset(this, 0, sizeof (AnyUnion));
  }

  AnyUnion(const AnyUnion& a) {
    memcpy(this, &a, sizeof (AnyUnion));
  }

  ~AnyUnion() { }

  bool operator==(const AnyUnion& right) const {
    return memcmp(this, &right, sizeof (AnyUnion)) == 0;
  }
};

struct Any {
  AnyUnion data;
  AnyType type;

  friend std::ostream& operator<<(std::ostream& os, const Any& obj) {
    switch (obj.type) {
      case INT: os << obj.data.i;
        break;
      case LONG: os << obj.data.l;
        break;
      case DOUBLE: os << obj.data.d;
        break;
      case STRING: os << obj.data.s;
        break;
      case CHAR: os << obj.data.c;
        break;
      case DATE: os << obj.data.date.getYear() * 10000 + 
                       obj.data.date.getMonth() * 100 + 
                       obj.data.date.getDay();
        break;
      default: os << "????";
    }
    return os;
  }

  Any() : data(), type(UNDEFINED) { }

  Any(const Any& that) : data(that.data), type(that.type) { }

  FORCE_INLINE bool operator==(const Any& that) const {
    if (type == UNDEFINED || that.type == UNDEFINED) throw std::logic_error("UNDEFINED Type in Any ");
    if (type != that.type) return false;
    switch (type) {
      case INT: return data.i == that.data.i;
      case LONG: return data.l == that.data.l;
      case DOUBLE: return fabs(data.d - that.data.d) < 0.01;
      case STRING: return data.s == that.data.s;
      case CHAR: return data.c == that.data.c;
      case DATE: return data.date == that.data.date;
      default: throw std::logic_error("Unknown type");
    }
  }

  FORCE_INLINE bool operator!=(const Any& that) const {
    if (type == UNDEFINED || that.type == UNDEFINED) throw std::logic_error("UNDEFINED Type in Any ");
    if (type != that.type) return true;
    switch (type) {
      case INT: return data.i != that.data.i;
      case LONG: return data.l != that.data.l;
      case DOUBLE: return data.d != that.data.d;
      case STRING: return !(data.s == that.data.s);
      case CHAR: return data.c != that.data.c;
      case DATE: return data.date != that.data.date;
      default: throw std::logic_error("Unknown type");
    }
  }

  FORCE_INLINE bool operator<(const Any& that) const {
    if (type == UNDEFINED || that.type == UNDEFINED) throw std::logic_error("UNDEFINED Type in Any ");
    if (type != that.type) throw std::logic_error("Cannot compare different types in Any");
    switch (type) {
      case INT: return data.i < that.data.i;
      case LONG: return data.l < that.data.l;
      case DOUBLE: return data.d < that.data.d;
      case STRING: return data.s < that.data.s;
      case CHAR: return data.c < that.data.c;
      case DATE: return data.date < that.data.date;
      default: throw std::logic_error("Unknown type");
    }
  }
};

class GenericEntry {

  GenericEntry(const std::unordered_map<int, Any> & m) : map(m) {
    nxt = prv = nullptr;
    int s = m.size() + 1;
    backPtrs = new void*[s];
    for (int i = 0; i < s; i++) {
      backPtrs[i] = nullptr;
    }
    isSampleEntry = false;
  }
    
  friend class GenericOps;

 public:
  std::unordered_map<int, Any> map;
  bool isSampleEntry;
  void** backPtrs;
  GenericEntry *nxt;
  GenericEntry *prv;

  template<class Archive>
  void serialize(Archive& ar) const {
    for (uint i = 1; i <= map.size(); ++i) {
      ar << dbtoaster::serialization::kElemSeparator;
      std::string name = "_" + std::to_string(i);
      const Any& a = map.at(i);
      switch (a.type) {
        case INT: dbtoaster::serialization::serialize(ar, a.data.i, name.c_str());
          break;
        case LONG: dbtoaster::serialization::serialize(ar, a.data.l, name.c_str());
          break;
        case DOUBLE: dbtoaster::serialization::serialize(ar, a.data.d, name.c_str());
          break;
        case STRING: dbtoaster::serialization::serialize(ar, a.data.s, name.c_str());
          break;
        case CHAR: dbtoaster::serialization::serialize(ar, a.data.c, name.c_str());
          break;
        case DATE: dbtoaster::serialization::serialize(ar, a.data.date, name.c_str());
          break;
        default: throw std::logic_error("Cannot serialize AnyType");
      }
    }
  }

  template <typename... Args>
  GenericEntry(true_type isSampleEntry, const Args&... args) : map() {
    this->isSampleEntry = true;
    processSampleEntry(this, args...);
  }

  template <typename... Args>
  GenericEntry(false_type isSampleEntry, const Args&... args) : map() {
    this->isSampleEntry = false;
    processFullEntry(this, 1, args...);
  }

  GenericEntry(int maxIdx = 10) {
    nxt = prv = nullptr;
    backPtrs = new void*[maxIdx];
    for (int i = 0; i < maxIdx; i++) {
      backPtrs[i] = nullptr;
    }
    isSampleEntry = false;
  }

  FORCE_INLINE void update(int i, int v) {
    map[i].type = INT;
    map[i].data.i = v;
  }

  FORCE_INLINE void update(int i, long v) {
    map[i].type = LONG;
    map[i].data.l = v;
  }

  FORCE_INLINE void update(int i, double v) {
    map[i].type = DOUBLE;
    map[i].data.d = v;
  }

  FORCE_INLINE void update(int i, const PString& v) {
    map[i].type = STRING;
    map[i].data.s = v;
  }

  FORCE_INLINE void update(int i, char v) {
    map[i].type = CHAR;
    map[i].data.c = v;
  }

  FORCE_INLINE void update(int i, DateType v) {
    map[i].type = DATE;
    map[i].data.date = v;
  }

  FORCE_INLINE void increase(int i, int v) {
    map[i].data.i += v;
  }

  FORCE_INLINE void increase(int i, long v) {
    map[i].data.l += v;
  }

  FORCE_INLINE void increase(int i, double v) {
    map[i].data.d += v;
  }

  FORCE_INLINE void decrease(int i, int v) {
    map[i].data.i -= v;
  }

  FORCE_INLINE void decrease(int i, long v) {
    map[i].data.l -= v;
  }    

  FORCE_INLINE void decrease(int i, double v) {
    map[i].data.d -= v;
  }

  FORCE_INLINE const Any& get(int i) const {
    return map.at(i);
  }

  FORCE_INLINE int getInt(int i) const {
    return map.at(i).data.i;
  }

  FORCE_INLINE long getLong(int i) const {
    return map.at(i).data.l;
  }

  FORCE_INLINE double getDouble(int i) const {
    return map.at(i).data.d;
  }

  FORCE_INLINE const PString& getString(int i) const {
    return map.at(i).data.s;
  }

  FORCE_INLINE const char getChar(int i) const {
    return map.at(i).data.c;
  }

  FORCE_INLINE const DateType getDate(int i) const {
    return map.at(i).data.date;
  }

  FORCE_INLINE GenericEntry* copy() const {
    //ONLY SHALLOW COPY for PString.
    GenericEntry* ptr = (GenericEntry*) malloc(sizeof (GenericEntry));
    new(ptr) GenericEntry(map);
    return ptr;
  }

  FORCE_INLINE bool operator==(const GenericEntry& right) const {
    return map == right.map;
  }

  friend std::ostream& operator<<(std::ostream& os, const GenericEntry& obj) {
    for (auto it : obj.map) {
        os << it.first << "->" << it.second << ", ";
    }
    return os;
  }

};

}

}
#endif /* DBTOASTER_PARDIS_GENTRY_HPP */