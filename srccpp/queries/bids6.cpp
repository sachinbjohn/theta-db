#include "ds/Table.h"
#include "utils/ComparatorOp.h"
#include "utils/Aggregator.h"
#include "ds/RangeTree.h"
#include "ds/Cube.h"
#include <chrono>
#include <unordered_map>
#include "datagen/BidsGenerator.h"
#include "exec/VWAP.h"
#include "queries/Bids6_DBT.h"
#include <cassert>

using namespace std;
using namespace chrono;

struct Bids6 : BidsExecutable {
    double result;
    vector<Row> verify;
    static constexpr double tstart = -INFINITY ;
    static constexpr double tend = INFINITY;

    Bids6(const string &a) : BidsExecutable("Bids6", a) {}

    static const int priceCol = 0;
    static const int timeCol = 1;
    static const int volCol = 2;
    static const int aggCol = 3;


    static void keyFn(const Row &r, Key &k) {
        k[0] = r[timeCol];
        k[1] = r[priceCol];
    }

    static double valueFn(const Row &r) {
        return 1.0;
    }

    static const SortingFunc ord;
    static const vector<COp> ops;
    static const AggPlus agg;
};

Key k1(2);
Key k2(2);

const AggPlus Bids6::agg;
const vector<COp> Bids6::ops = {GreaterThan::getInstance(), LessThan::getInstance()};
const SortingFunc  Bids6::ord = sorting(&keyFn, &ops, &k1, &k2);

struct Bids6Naive : Bids6 {


    Bids6Naive() : Bids6("Naive,CPP") {}

    long long int evaluate(const Table &bids) override {
        result = 0;
        verify.clear();
        auto start = chrono::steady_clock::now();
        for (const auto &b1 : bids.rows) {
            double sum1 = 0;
            if (b1[timeCol] > tstart && b1[timeCol] < tend) {
                for (const auto &b2: bids.rows) {
                    if (b2[timeCol] > tstart && b2[timeCol] < tend) {
                        if (b2[timeCol] > b1[timeCol] && b2[priceCol] < b1[priceCol]) {
                            sum1 += 1;
                        }
                    }
                }
                Row newrow = b1;
                newrow.push_back(sum1);
                verify.emplace_back(newrow);
            }

            result += sum1;
        }
        auto end = chrono::steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

struct Bids6DBT : Bids6 {
    Bids6DBT() : Bids6("DBT,CPP") {}

    dbtoaster::data_t obj;

    long long int evaluate(const Table &bids) override {
        result = 0;
        verify.clear();
        vector<dbtoaster::BatchMessage<dbtoaster::data_t::BidsAdaptor::MessageType, int>::KVpair> batch(
                bids.rows.size());
        int i = 0;
        for (const auto &r : bids.rows) {
            batch[i].first.content = make_tuple(r[timeCol], 0, 0, r[volCol], r[priceCol]);
            batch[i].second = 1;
            i++;
        }
        obj.tstart = tstart;
        obj.tend = tend;
        auto start = chrono::steady_clock::now();
        obj.on_system_ready_event();
        obj.on_batch_update_BIDS(batch);
        auto end = chrono::steady_clock::now();
        result = obj.get___SQL_SUM_AGGREGATE_1();
        return duration_cast<milliseconds>(end - start).count();
    }
};

struct Bids6Range : Bids6 {
    Bids6Range() : Bids6("Range,CPP") {}

    long long int evaluate(const Table &bids) override {
        result = 0;
        verify.clear();
        auto start = steady_clock::now();

        RangeTree rt(agg, 2);
        rt.buildFrom(bids, keyFn, 2, valueFn);

        Table join;
        rt.join(bids, join, keyFn, ops);

        for (const auto &r: join.rows) {
            verify.emplace_back(r);
            result += r[aggCol];
        }
        auto end = steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

struct Bids6Merge : Bids6 {
    Bids6Merge() : Bids6("Merge,CPP") {}

    long long int evaluate(const Table &bids) override {
        result = 0;
        verify.clear();
        auto start = steady_clock::now();
        Table sortedBids;
        sortedBids.rows = bids.rows;
        sort(sortedBids.rows.begin(), sortedBids.rows.end(), ord);
        vector<Domain> domain(2);
        sortedBids.fillDomain(domain[0], timeCol, true);
        sortedBids.fillDomain(domain[1], priceCol, false);
        Cube cube(domain, agg);
        cube.fillData(sortedBids, keyFn, valueFn);
        cube.accumulate(ops);


        Table join;
        cube.join(sortedBids, join, keyFn, ops);


        for (const auto &r: join.rows) {
            verify.emplace_back(r);
            result += r[aggCol];
        }
        auto end = steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

int main(int argc, char **argv) {
    vector<Bids6 *> tests;
    tests.emplace_back(new Bids6Naive);
    tests.emplace_back(new Bids6DBT);
    tests.emplace_back(new Bids6Range);
    tests.emplace_back(new Bids6Merge);
    long long maxTimeInMS = 1000 * 60 * 5;
    int testFlag = 0xFF;
    if (argc >= 3) {
        testFlag = stoi(argv[1]);
        maxTimeInMS = stoi(argv[2]) * 60 * 1000;  //arg in minutes
    }
    bool enable = true;
    for (int all = 10; all <= 10 && enable; all += 1) {

        int logn = all;
        int logp = all-5;
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
                       execTime);
                cout << endl;
                if (execTime > maxTimeInMS)
                    enable = false;
                sort(tests[i]->verify.begin(), tests[i]->verify.end());
                /*
                cout << "\nResult" << endl;
                for (const auto &r: tests[i]->result) {
                    cout << r << " ";
                }
                cout << "\n Verify" << endl;
                for (const auto &r: tests[i]->verify) {
                    cout << r << " ";
                }
                 */
            }
        }
        auto pair02Ver = mismatch(tests[0]->verify.begin(), tests[0]->verify.end(), tests[2]->verify.begin());
        if (pair02Ver.first != tests[0]->verify.end() && pair02Ver.second != tests[2]->verify.end()) {
            cout << "RangeError" << endl;
            cout << *pair02Ver.first << " " << *pair02Ver.second << endl;
        }
        auto pair03Ver = mismatch(tests[0]->verify.begin(), tests[0]->verify.end(), tests[3]->verify.begin());
        if (pair03Ver.first != tests[0]->verify.end() && pair03Ver.second != tests[3]->verify.end()) {
            cout << "MergeError" << endl;
            cout << *pair03Ver.first << " " << *pair03Ver.second << endl;
        }
        assert(tests[0]->result == tests[1]->result);
        assert(tests[0]->result == tests[2]->result);
        assert(tests[0]->result == tests[3]->result);
    }
}