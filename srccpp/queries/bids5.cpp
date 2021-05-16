#include "ds/Table.h"
#include "utils/ComparatorOp.h"
#include "utils/Aggregator.h"
#include "ds/RangeTree.h"
#include "ds/Cube.h"
#include <chrono>
#include <unordered_map>
#include "datagen/BidsGenerator.h"
#include "exec/VWAP.h"
#include "queries/Bids5_DBT.h"
#include <cassert>

using namespace std;
using namespace chrono;

struct Bids5 : BidsExecutable {
    vector<Row> result;
    vector<Row> verify;

    Bids5(const string &a) : BidsExecutable("Bids5", a) {}

    static const int priceCol = 0;
    static const int timeCol = 1;
    static const int volCol = 2;
    static const int agg3Col = 3;
    static const int agg2Col = 4;

    static void keyFn2Outer(const Row &r, Key &k) {
        k[0] = r[agg3Col];
    }

    static void keyFn(const Row &r, Key &k) {
        k[0] = r[timeCol];
    }

    static double valueFn2(const Row &r) {
        return r[priceCol];
    }

    static double valueFn3(const Row &r) {
        return r[timeCol];
    }

    static const SortingFunc ord;
    static const vector<COp> ops3;
    static const vector<COp> ops2;
    static const AggMax agg;
};

Key k1(1);
Key k2(1);

const AggMax Bids5::agg;
const vector<COp> Bids5::ops3 = {LessThan::getInstance()};
const vector<COp> Bids5::ops2 = {::EqualTo::getInstance()};
const SortingFunc  Bids5::ord = sorting(&keyFn, &ops3, &k1, &k2);

struct Bids5Naive : Bids5 {


    Bids5Naive() : Bids5("Naive,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        verify.clear();
        auto start = chrono::steady_clock::now();
        for (const auto &b1 : bids.rows) {
            double maxTime = -INFINITY;
            double maxPrice = -INFINITY;
            for (const auto &b2: bids.rows) {
                for (const auto &b3: bids.rows) {
                    if (b3[timeCol] < b1[timeCol] && b3[timeCol] > maxTime)
                        maxTime = b3[timeCol];
                }
                if (b2[timeCol] == maxTime && b2[priceCol] > maxPrice)
                    maxPrice = b2[priceCol];
            }

            Row newrow = b1;
            newrow.push_back(maxTime);
            newrow.push_back(maxPrice);
            verify.emplace_back(newrow);
            if (b1[priceCol] >= 1.1 * maxPrice)
                result.emplace_back(b1);
        }
        auto end = chrono::steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

struct Bids5DBT : Bids5 {
    Bids5DBT() : Bids5("DBT,CPP") {}



    long long int evaluate(const Table &bids) override {
        result.clear();
        verify.clear();
        dbtoaster::data_t obj;
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
        auto end = chrono::steady_clock::now();
        auto head = obj.get_COUNT().head;
        while (head) {
            result.emplace_back(Row({head->B1_PRICE, head->B1_T, head->B1_VOLUME}));
            head = head->nxt;
        }
        return duration_cast<milliseconds>(end - start).count();
    }
};

struct Bids5Range : Bids5 {
    Bids5Range() : Bids5("Range,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        verify.clear();
        auto start = steady_clock::now();

        RangeTree rt2(agg, 1);
        rt2.buildFrom(bids, keyFn, 1, valueFn2);
        RangeTree rt3(agg, 1);
        rt3.buildFrom(bids, keyFn, 1, valueFn3);

        Table b132, b13;
        rt3.join(bids, b13, keyFn, ops3);
        rt2.join(b13, b132, keyFn2Outer, ops2);

        for (const auto &r: b132.rows) {
            verify.emplace_back(r);
            if (r[priceCol] >= 1.1 * r[agg2Col])
                result.emplace_back(Row({r[priceCol], r[timeCol], r[volCol]}));
        }
        auto end = steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

struct Bids5Merge : Bids5 {
    Bids5Merge() : Bids5("Merge,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        verify.clear();
        auto start = steady_clock::now();
        Table sortedBids;
        sortedBids.rows = bids.rows;
        sort(sortedBids.rows.begin(), sortedBids.rows.end(), ord);
        vector<Domain> domain(1);
        sortedBids.fillDomain(domain[0], timeCol);
        Cube cube2(domain, agg), cube3(domain, agg);
        cube2.fillData(sortedBids, keyFn, valueFn2);
        cube2.accumulate(ops2);
        cube3.fillData(sortedBids, keyFn, valueFn3);
        cube3.accumulate(ops3);

        vector<bool> domFlags = {true};
        SortingFunc ord2 = sortingOther(&domain, &domFlags, &keyFn2Outer, &ops2, &k1, &k2);
        Table b132, b13;
        cube3.join(sortedBids, b13, keyFn, ops3);
        sort(b13.rows.begin(), b13.rows.end(), ord2);
        cube2.join(b13, b132, keyFn2Outer, ops2);

        for (const auto &r: b132.rows) {
            verify.emplace_back(r);
            if (r[priceCol] >= 1.1 * r[agg2Col])
                result.emplace_back(Row({r[priceCol], r[timeCol], r[volCol]}));
        }
        auto end = steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

int main(int argc, char **argv) {
    vector<Bids5 *> tests;
    tests.emplace_back(new Bids5Naive);
    tests.emplace_back(new Bids5DBT);
    tests.emplace_back(new Bids5Range);
    tests.emplace_back(new Bids5Merge);
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