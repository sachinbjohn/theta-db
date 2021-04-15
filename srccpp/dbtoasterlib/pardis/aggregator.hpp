#ifndef DBTOASTER_PARDIS_AGGREGATOR_HPP
#define DBTOASTER_PARDIS_AGGREGATOR_HPP

#include <functional>
#include <vector>
#include <algorithm>
#include <cassert>

namespace dbtoaster {

namespace pardis {

typedef void TransactionReturnStatus;
#define SUCCESS 

template <typename E, typename R>
struct MinAggregator {
  E** minEntry;
  R minRes;
  std::function<R(E*) > func;
  bool first;

  MinAggregator(const std::function<R(E*) >& f, E**res)
      : minEntry(res), func(f), first(true) { }

  TransactionReturnStatus operator()(E* e) {
    if (first) {
      first = false;
      minRes = func(e);
      *minEntry = e;
    }
    else {
      R val = func(e);
      if (minRes > val) {
        minRes = val;
        *minEntry = e;
      }
    }
    return SUCCESS;
  }

  E* result() { return *minEntry; }
};

template <typename E, typename R>
struct MaxAggregator {
  E** maxEntry;
  R maxRes;
  std::function<R(E*) > func;
  bool first;

  MaxAggregator(const std::function<R(E*) >& f, E**res)
      : maxEntry(res), func(f), first(true) { }

  TransactionReturnStatus operator()(E* e) {
    if (first) {
      first = false;
      maxRes = func(e);
      *maxEntry = e;
    } else {
      R val = func(e);
      if (maxRes < val) {
        maxRes = val;
        *maxEntry = e;
      }
    }
    return SUCCESS;
  }

  E* result() { return *maxEntry; }
};

template<typename E, typename R>
struct MedianAggregator {
  std::function<R(E*) > func;
  std::vector<E*>& results;

  MedianAggregator(const std::function<R(E*)>& f, std::vector<E*>& res)
      : func(f), results(res) { }

  TransactionReturnStatus operator()(E* e) {
    results.push_back(e);
    return SUCCESS;
  }

  E* result() {
    if (results.empty()) return nullptr;

    std::sort(results.begin(), results.end(), [&](E* e1, E * e2) {
      assert(e1);
      assert(e2);
      const R& v1 = func(e1);
      const R& v2 = func(e2);
      return v1 < v2;
    });
    int s = results.size();
    int i = s / 2;
    if (s % 2 == 0) i--;
    return results[i];
  }
};

}

}
#endif /* DBTOASTER_PARDIS_AGGREGATOR_HPP */