#ifndef VWAPPARAMS_H
#define VWAPPARAMS_H

#include "ds/Table.h"
#include <string>
#include <ostream>

struct BidsExecutable {
    const string algo;
    const string query;
    bool enable;
    BidsExecutable(const string &q, const string &a) : algo(a), query(q), enable(true){}

    virtual long long evaluate(const Table &bids) = 0;
};

struct ParamsVWAP {
    int logn;
    int logr;
    int logp;
    int logt;
    BidsExecutable *qalgo;

    ParamsVWAP(int logn, int logr, int logp, int logt, BidsExecutable *qalgo) : logn(logn), logr(logr), logp(logp),
                                                                                logt(logt), qalgo(qalgo) {}
    friend void operator>>(const string& line, ParamsVWAP& vwap) {

    }
    friend ostream &operator<<(ostream &os, const ParamsVWAP &vwap) {
        os << vwap.qalgo->query << "," << vwap.qalgo->algo << "," << vwap.logn << "," << vwap.logr << "," << vwap.logp
           << "," << vwap.logt;
        return os;
    }
};

#endif //VWAPPARAMS_H
