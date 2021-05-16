#include "ds/Table.h"
#include "utils/ComparatorOp.h"
#include "utils/Aggregator.h"
#include "ds/RangeTree.h"
#include "ds/Cube.h"
#include <chrono>
#include <unordered_map>
#include "datagen/BidsGenerator.h"
#include "exec/VWAP.h"
#include "VWAP2_DBT.h"
#include <map>

using namespace std;
using namespace chrono;

struct VWAP2 : BidsExecutable {
    VWAP2(const string &a) : BidsExecutable("Q2", a) {}

    unordered_map<double, double> result;
    static const int priceCol = 0;
    static const int timeCol = 1;
    static const int volCol = 2;
    static const int aggB3Col = 3;
    static const int aggB2Col = 4;

    static void keyFunction2(const Row &r, Key &k) {
        k[0] = r[timeCol];
        k[1] = r[priceCol];
    }

    static void keyFunction3(const Row &r, Key &k) {
        k[0] = r[timeCol];
    }

    static double valueFn2(const Row &r) {
        return r[volCol];
    }

    static double valueFn3(const Row &r) {
        return r[volCol] * 0.25;
    }

    static const SortingFunc ord;
    static const vector<COp> ops2;
    static const vector<COp> ops3;
    static const AggPlus agg;
};

Key k1(2);
Key k2(2);

const AggPlus VWAP2::agg;
const vector<COp> VWAP2::ops2 = {LessThanEqual::getInstance(), LessThan::getInstance()};
const vector<COp> VWAP2::ops3 = {LessThanEqual::getInstance()};
const SortingFunc  VWAP2::ord = sorting(&keyFunction2, &ops2, &k1, &k2);

struct VWAP2Naive : VWAP2 {
    VWAP2Naive() : VWAP2("Naive,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        auto start = chrono::steady_clock::now();
        for (const auto &b1 : bids.rows) {
            double sum = 0;
            double nC2 = 0;
            for (const auto &b3: bids.rows) {
                if (b3[timeCol] <= b1[timeCol]) {
                    nC2 += 0.25 * b3[volCol];
                }

            }
            for (const auto &b2: bids.rows) {
                if (b2[priceCol] < b1[priceCol] && b2[timeCol] <= b1[timeCol])
                    sum += b2[volCol];
            }
            if (nC2 < sum) {
                double v = b1[priceCol] * b1[volCol];
                double t = b1[timeCol];
                result[t] += v; //not correct for max;
            }
        }
        auto end = chrono::steady_clock::now();
        return duration_cast<nanoseconds>(end - start).count();

    }
};

struct VWAP2DBT : VWAP2 {

    VWAP2DBT() : VWAP2("DBT,CPP") {}



    long long int evaluate(const Table &bids) override {
        dbtoaster::data_t obj;
//       dbtoaster::data_t::BidsAdaptor::MessageType::
        vector<dbtoaster::BatchMessage<dbtoaster::data_t::BidsAdaptor::MessageType, int>::KVpair> batch(
                bids.rows.size());
        int i = 0;
        for (const auto &r : bids.rows) {
            batch[i].first.content = make_tuple(r[timeCol], 0, 0, r[volCol], r[priceCol]);
            batch[i].second = 1;
            i++;
        }
        result.clear();
        auto start = chrono::steady_clock::now();
        obj.on_system_ready_event();
        obj.on_batch_update_BIDS(batch);
        auto head = obj.get_VWAP().head;
        while (head) {
            result.emplace(head->B1_T, head->__av);
            head = head->nxt;
        }
        auto end = chrono::steady_clock::now();
        return chrono::duration_cast<chrono::nanoseconds>(end - start).count();
    }
};

struct VWAP2Range : VWAP2 {
    VWAP2Range() : VWAP2("Range,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        auto start = steady_clock::now();
        map<pair<double, double>, double> nC1;
        for (const auto &b: bids.rows) {
            double price = b[priceCol];
            double volume = b[volCol];
            double time = b[timeCol];
            nC1[make_pair(time, price)] += volume;
        }

        Table preAgg;
        for (const auto &kv: nC1) {
            preAgg.rows.emplace_back(Row({kv.first.second, kv.first.first, kv.second}));
        }

        RangeTree rtB3(agg, 1);
        rtB3.buildFrom(preAgg, keyFunction3, 1, valueFn3);

        RangeTree rtB2(agg, 2);
        rtB2.buildFrom(preAgg, keyFunction2, 2, valueFn2);

        Table B1B3;
        rtB3.join(preAgg, B1B3, keyFunction3, ops3);

        Table B1B3B2;
        rtB2.join(B1B3, B1B3B2, keyFunction2, ops2);

        for (const auto &r : B1B3B2.rows) {
            double t = r[timeCol];
            double v = r[priceCol] * r[volCol];
            if (r[aggB3Col] < r[aggB2Col])
                result[t] += v;
        }
        auto end = steady_clock::now();
        return duration_cast<nanoseconds>(end - start).count();
    }
};

struct VWAP2Merge : VWAP2 {
    VWAP2Merge() : VWAP2("Merge,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        auto start = steady_clock::now();
        map<pair<double, double>, double> nC1;
        for (const auto &b: bids.rows) {
            double price = b[priceCol];
            double volume = b[volCol];
            double time = b[timeCol];
            nC1[make_pair(time, price)] += volume;
        }

        Table preAgg;
        for (const auto &kv: nC1) {
            preAgg.rows.emplace_back(Row({kv.first.second, kv.first.first, kv.second}));
        }
        sort(preAgg.rows.begin(), preAgg.rows.end(), ord);

        vector<Domain> domain2(2);
        vector<Domain> domain3(1);
        preAgg.fillDomain(domain3[0], timeCol);
        preAgg.fillDomain(domain2[0], timeCol);
        preAgg.fillDomain(domain2[1], priceCol);

        Cube cubeB3(domain3, agg);
        cubeB3.fillData(preAgg, keyFunction3, valueFn3);
        cubeB3.accumulate(ops3);

        Cube cubeB2(domain2, agg);
        cubeB2.fillData(preAgg, keyFunction2, valueFn2);
        cubeB2.accumulate(ops2);

        Table B1B3, B1B3B2;
        cubeB3.join(preAgg, B1B3, keyFunction3, ops3);
        cubeB2.join(B1B3, B1B3B2, keyFunction2, ops2);

        for (const auto &r : B1B3B2.rows) {
            double t = r[timeCol];
            double v = r[priceCol] * r[volCol];
            if (r[aggB3Col] < r[aggB2Col])
                result[t] += v;
        }
        auto end = steady_clock::now();
        return duration_cast<nanoseconds>(end - start).count();
    }
};

int main(int argc, char **argv) {

    vector<VWAP2 *> tests;
    tests.emplace_back(new VWAP2Naive);
    tests.emplace_back(new VWAP2DBT);
    tests.emplace_back(new VWAP2Range);
    tests.emplace_back(new VWAP2Merge);
    long long maxTimeInMS = 1000 * 60 * 5;
    int testFlag = 0xFF;
    if (argc >= 3) {
        testFlag = stoi(argv[1]);
        maxTimeInMS = stoi(argv[2]) * 60 * 1000;  //arg in minutes
    }
    bool enable = true;
    for (int all = 10; all <= 28 && enable; all += 1) {

        int logn = all;
        int logp = all;
        int logt = 10;
        int logr = all;
        int numRuns = 1;

        Table bids;
        loadFromFile(bids, logn, logr, logp, logt);


        for (int i = 0; i < tests.size(); i++) {
            if (testFlag & (1 << i)) {
                long long execTime = tests[i]->evaluate(bids);
                printf("%s,%s,%d,%d,%d,%d,%lld", tests[i]->query.c_str(), tests[i]->algo.c_str(), logn, logr, logp,
                       logt,
                       execTime / 1000000);
                cout << endl;
                if (execTime / 1000000 > maxTimeInMS)
                    enable = false;
                /*for(const auto& r: t->result){
                    if(r.second != 0)
                        cout << "(" << r.first << "," << r.second << "), ";
                }
                cout << endl;*/
            }
        }
    }

}