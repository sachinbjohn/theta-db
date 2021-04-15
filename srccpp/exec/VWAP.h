#ifndef VWAPPARAMS_H
#define VWAPPARAMS_H
#include "ds/Table.h"
#include <string>
struct VWAPExecutable {
    const string algo;
    const string query;
    VWAPExecutable(const string& q, const string & a):algo(a), query(q){}
    virtual long long evaluate(const Table &bids) = 0;
};

struct ParamsVWAP{
    int n;
    int p;
    int t;
    int r;
    VWAPExecutable* qalgo;
};
#endif //VWAPPARAMS_H
