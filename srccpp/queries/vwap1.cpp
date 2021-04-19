
#include "ds/Table.h"
#include "utils/ComparatorOp.h"
#include "utils/Aggregator.h"
#include "ds/RangeTree.h"
#include "ds/Cube.h"
#include <chrono>
#include <unordered_map>
#include "datagen/BidsGenerator.h"
#include "exec/VWAP.h"
#include "VWAP1_DBT.h"

using namespace std;

struct VWAP1 : VWAPExecutable {
    double result;
    static const int priceCol = 0;
    static const int timeCol = 1;
    static const int volCol = 2;
    static const int volCol2 = 1;
    static const int aggCol = 3;
    static const int aggCol2 = 2;
    static const vector<COp> ops;
    static const AggPlus agg;

    VWAP1(const string &algo) : VWAPExecutable("Q1", algo) {}

    static void keyFunction(const Row &r, Key &k) {
        k[0] = r[priceCol];
    }

//    static double valueFunction(const Row& r) {
//        return r[volCol];
//    }
    static double valueFunction2(const Row &r) {
        return r[volCol2];
    }

    static const SortingFunc ord;


};

const vector<COp> VWAP1::ops = {LessThan::getInstance()};
const SortingFunc VWAP1::ord = sorting(keyFunction, ops);
const AggPlus VWAP1::agg;

struct VWAP1Naive : VWAP1 {
    VWAP1Naive() : VWAP1("Naive CPP") {}

    long long int evaluate(const Table &bids) override {

        auto start = chrono::steady_clock::now();
        double nC2 = 0;
        result = 0;
        for (const auto &b : bids.rows) {
            nC2 += 0.25 * b[volCol];
        }
        for (const auto &b1: bids.rows) {
            double sum = 0;
            for (const auto &b2: bids.rows) {
                if (b2[priceCol] < b1[priceCol])
                    sum += b2[volCol];
            }
            if (nC2 < sum)
                result += b1[priceCol] * b1[volCol];
        }

        auto end = chrono::steady_clock::now();
        return chrono::duration_cast<chrono::nanoseconds>(end - start).count();
    }
};

struct VWAP1DBT : VWAP1 {

    VWAP1DBT() : VWAP1("DBT CPP") {}

    dbtoaster::data_t obj;

    long long int evaluate(const Table &bids) override {
//       dbtoaster::data_t::BidsAdaptor::MessageType::
        vector<dbtoaster::BatchMessage<dbtoaster::data_t::BidsAdaptor::MessageType, int>::KVpair> batch(
                bids.rows.size());
        int i = 0;
        for (const auto &r : bids.rows) {
            batch[i].first.content = make_tuple(r[timeCol], 0, 0, r[volCol], r[priceCol]);
            batch[i].second = 1;
            i++;
        }
        auto start = chrono::steady_clock::now();
        obj.on_system_ready_event();
        obj.on_batch_update_BIDS(batch);
        result = obj.get_VWAP();
        auto end = chrono::steady_clock::now();
        return chrono::duration_cast<chrono::nanoseconds>(end - start).count();
    }
};

struct VWAP1Range : VWAP1 {
    VWAP1Range() : VWAP1("Range CPP") {}

    long long int evaluate(const Table &bids) override {
        auto start = chrono::steady_clock::now();
        result = 0;
        double nC2 = 0;
        unordered_map<double, double> nC1;

        for (const auto &b : bids.rows) {
            int price = b[priceCol];
            int volume = b[volCol];
            nC2 += 0.25 * volume;
            nC1[price] += volume;  //Not correct for max agg;
        }

        Table preAgg;
        for (const auto &kv : nC1) {
            preAgg.rows.emplace_back(Row({kv.first, kv.second}));
        }

        RangeTree rtB2(agg, 1);
        rtB2.buildFrom(preAgg, keyFunction, 1, valueFunction2);
        Table join;
        rtB2.join(preAgg, join, keyFunction, ops);


        for (const auto &r: join.rows) {
            if (nC2 < r[aggCol2])
                result += r[priceCol] * r[volCol2];
        }
        auto end = chrono::steady_clock::now();
        long long ns = chrono::duration_cast<chrono::nanoseconds>(end - start).count();
        return ns;
    }
};

struct VWAP1Merge : VWAP1 {
    VWAP1Merge() : VWAP1("Merge CPP") {}

    long long int evaluate(const Table &bids) override {
        auto start = chrono::steady_clock::now();
        result = 0;
        double nC2 = 0;
        unordered_map<double, double> nC1;

        for (const auto &b : bids.rows) {
            int price = b[priceCol];
            int volume = b[volCol];
            nC2 += 0.25 * volume;
            nC1[price] += volume;  //Not correct for max agg;
        }

        Table preAgg;
        for (const auto &kv : nC1) {
            preAgg.rows.emplace_back(Row({kv.first, kv.second}));
        }

        sort(preAgg.rows.begin(), preAgg.rows.end(), ord);
        vector<Domain> domains(1);
        preAgg.fillDomain(domains[0], 0, false);

        Cube cubeB2(domains, agg);
        cubeB2.fillData(preAgg, keyFunction, valueFunction2);
        cubeB2.accumulate(ops);

        Table join;
        cubeB2.join(preAgg, join, keyFunction, ops);


        for (const auto &r: join.rows) {
            if (nC2 < r[aggCol2])
                result += r[priceCol] * r[volCol2];
        }
        auto end = chrono::steady_clock::now();
        long long ns = chrono::duration_cast<chrono::nanoseconds>(end - start).count();
        return ns;
    }
};


int main() {

    int all = 10;
    int logn = all;
    int logp = all;
    int logt = 10;
    int logr = all;
    int numRuns = 1;

    Table bids;

    loadFromFile(bids, logn, logr, logp, logt);

    vector<VWAP1 *> tests;
//    tests.emplace_back(new VWAP1Naive);
//    tests.emplace_back(new VWAP1DBT);
    tests.emplace_back(new VWAP1Range);
    tests.emplace_back(new VWAP1Merge);

    for (const auto &t : tests) {
        long long execTime = t->evaluate(bids);
        printf("%s,%s,%d,%d,%d,%d,%lld\n", t->query.c_str(), t->algo.c_str(), logn, logr, logp, logt,
               execTime / 1000000);
        cout << "Result = " << (long long) t->result << endl;
    }

}