#include "ds/Table.h"
#include "utils/ComparatorOp.h"
#include "utils/Aggregator.h"
#include "ds/RangeTree.h"
#include "ds/Cube.h"
#include <chrono>
#include <unordered_map>
#include "datagen/BidsGenerator.h"
#include "exec/VWAP.h"
#include <cassert>

using namespace std;
using namespace chrono;

struct MB2 : BidsExecutable {
    vector<Row> result;

    MB2(const string &a) : BidsExecutable("MB2", a) {}

    static const int priceCol = 0;
    static const int timeCol = 1;
    static const int volCol = 2;
    static const int aggCol = 3;

    static const int aggbidsTimeCol = 0;
    static const int aggbidsaggCol = 1;

    static void keyFn(const Row &r, Key &k) {
        k[0] = r[aggbidsTimeCol];
    }

    static double valueFn(const Row &r) {
        return 1.0;
    }

    static const SortingFunc ord;
    static const vector<COp> ops;
    static const AggMax agg;
};

Key k1(1);
Key k2(1);

const AggSum MB2::agg;
const vector<COp> MB2::ops = {LessThanEqual::getInstance()};
const SortingFunc  MB2::ord = sorting(&keyFn, &ops, &k1, &k2);

struct MB2Naive : MB2 {


    MB2Naive() : MB2("Naive,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        auto start = chrono::steady_clock::now();
        for (const auto &b1 : bids.rows) {
            double sum = 0;
            for (const auto &b2: bids.rows) {
                if (b2[timeCol] < b1[timeCol]) {
                    sum += 1.0;
                }
            }
            Row newrow = b1;
            newrow.push_back(sum);
            result.emplace_back(b1);
        }
        auto end = chrono::steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

struct MB2Smart : MB2 {


    MB2Smart() : MB2("Smart,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        unordered_map<double, double> aggbids;
        unordered_map<tuple<double, double, double>, double> distbids;
        auto start = chrono::steady_clock::now();
        for (const auto &b1 : bids.rows) {
            double time = b1[timeCol];
            double price = b1[priceCol];
            double volume = b1[volCol];
            aggbids[time] += 1;
            distbids[make_tuple(price, time, volume)] += 1;
        }
        unordered_map<double, double> cumaggbids;
        for (const auto &b1: aggbids) {
            double sum = 0;
            for (const auto &b2: aggbids) {
                if (b2.first < b1.first)
                    sum += b2.second;
            }
            cumaggbids[b1.first] = sum;
        }

        for (const auto &b1: distbids) {
            double price = get<0>(b1.first);
            double time = get<1>(b1.first);
            double volume = get<2>(b1.first);
            auto newrow = Row({price, time, volume});
            double agg = cumaggbids[time] * b1.second;
            newrow.emplace_back(agg);
            result.emplace_back(newrow);
        }
        auto end = chrono::steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};


struct MB2Range : MB2 {
    MB2Range() : MB2("Range,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        unordered_map<double, double> aggbids;
        unordered_map<tuple<double, double, double>, double> distbids;
        auto start = chrono::steady_clock::now();
        for (const auto &b1 : bids.rows) {
            double time = b1[timeCol];
            double price = b1[priceCol];
            double volume = b1[volCol];
            aggbids[time] += 1;
            distbids[make_tuple(price, time, volume)] += 1;
        }
        Table aggbidsTable;
        for(const auto &kv : aggbids){
            aggbidsTable.rows.emplace_back({kv.first, kv.second})
        }
        RangeTree rt(agg, 1);
        rt.buildFrom(aggbidsTable, keyFn, 1, valueFn);

        Table join;
        rt.join(distbids, join, keyFn, ops);

        for (const auto &r: join.rows) {
            verify.emplace_back(r);
            if (r[aggCol] == r[priceCol])
                result.emplace_back(Row({r[priceCol], r[timeCol], r[volCol]}));
        }
        auto end = steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

struct MB2Merge : MB2 {
    MB2Merge() : MB2("Merge,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        verify.clear();
        auto start = steady_clock::now();
        Table sortedBids;
        sortedBids.rows = bids.rows;
        sort(sortedBids.rows.begin(), sortedBids.rows.end(), ord);
        vector<Domain> domain(1);
        sortedBids.fillDomain(domain[0], timeCol);
        Cube cube(domain, agg);
        cube.fillData(sortedBids, keyFn, valueFn);
        cube.accumulate(ops);

        Table join;
        cube.join(sortedBids, join, keyFn, ops);
        for (const auto &r: join.rows) {
            verify.emplace_back(r);
            if (r[priceCol] == r[aggCol])
                result.emplace_back(Row({r[priceCol], r[timeCol], r[volCol]}));
        }
        auto end = steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

int main(int argc, char **argv) {
    vector<MB2 *> tests;
    tests.emplace_back(new MB2Naive);
    tests.emplace_back(new MB2DBT);
    tests.emplace_back(new MB2Range);
    tests.emplace_back(new MB2Merge);
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
        int logt = all;
        int logr = all;
        int numRuns = 1;

        Table bids;
        loadFromFile(bids, logn, logr, logp, logt);


        for (int i = 0; i < tests.size(); i++) {
            if (testFlag & (1 << i)) {
                long long execTime = tests[i]->evaluate(bids);
                printf("%s,%s,%d,%d,%d,%d,%lld", tests[i]->query.c_str(), tests[i]->algo.c_str(), logn, logr, logp,
                       logt,
                       execTime);
                cout << endl;
                if (execTime > maxTimeInMS)
                    enable = false;
                sort(tests[i]->result.begin(), tests[i]->result.end());
                sort(tests[i]->verify.begin(), tests[i]->verify.end());
                /*
                cout << "\nResult" << endl;
                for (const auto &r: tests[i]->result) {
                    cout << r << " ";
                }
                cout << endl;

                cout << "\n Verify" << endl;
                for (const auto &r: tests[i]->verify) {
                    cout << r << " ";
                }
                 */
            }
        }
        if ((testFlag & 3) == 3) {
            assert(tests[0]->result == tests[1]->result);
        }
        if ((testFlag & 5) == 5) {
            auto pair02Ver = mismatch(tests[0]->verify.begin(), tests[0]->verify.end(), tests[2]->verify.begin());
            if (pair02Ver.first != tests[0]->verify.end() && pair02Ver.second != tests[2]->verify.end()) {
                cout << "RangeError" << endl;
                cout << *pair02Ver.first << " " << *pair02Ver.second << endl;
            }

            assert(tests[0]->result == tests[2]->result);
        }
        if ((testFlag & 9) == 9) {
            auto pair03Ver = mismatch(tests[0]->verify.begin(), tests[0]->verify.end(), tests[3]->verify.begin());
            if (pair03Ver.first != tests[0]->verify.end() && pair03Ver.second != tests[3]->verify.end()) {
                cout << "MergeError" << endl;
                cout << *pair03Ver.first << " " << *pair03Ver.second << endl;
            }

            assert(tests[0]->result == tests[3]->result);
        }

    }
}