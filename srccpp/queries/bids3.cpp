#include "ds/Table.h"
#include "utils/ComparatorOp.h"
#include "utils/Aggregator.h"
#include "ds/RangeTree.h"
#include "ds/Cube.h"
#include <chrono>
#include <unordered_map>
#include "datagen/BidsGenerator.h"
#include "exec/VWAP.h"
#include "queries/Bids3_DBT.h"
#include <cassert>

using namespace std;
using namespace chrono;

struct Bids3 : BidsExecutable {
    vector<Row> result;
    vector<Row> verify;

    Bids3(const string &a) : BidsExecutable("Bids3", a) {}

    static const int priceCol = 0;
    static const int timeCol = 1;
    static const int volCol = 2;
    static const int agg2Col = 3;
    static const int agg3Col = 4;
    static const int agg4Col = 5;
    static const int agg5Col = 6;

    static void keyFn(const Row &r, Key &k) {
        k[0] = r[timeCol];
    }

    static double valueFn2(const Row &r) {
        return 1.0;
    }

    static double valueFn3(const Row &r) {
        return r[priceCol] * r[timeCol];
    }

    static double valueFn4(const Row &r) {
        return r[priceCol];
    }

    static double valueFn5(const Row &r) {
        return r[timeCol];
    }

    static const SortingFunc ord;
    static const vector<COp> ops;
    static const AggPlus agg;
};

Key k1(1);
Key k2(1);

const AggPlus Bids3::agg;
const vector<COp> Bids3::ops = {LessThan::getInstance()};
const SortingFunc  Bids3::ord = sorting(&keyFn, &ops, &k1, &k2);

struct Bids3Naive : Bids3 {


    Bids3Naive() : Bids3("Naive,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        verify.clear();
        auto start = chrono::steady_clock::now();
        for (const auto &b1 : bids.rows) {
            double sum2 = 0;
            double sum3 = 0;
            double sum4 = 0;
            double sum5 = 0;
            for (const auto &b2: bids.rows) {
                if (b2[timeCol] < b1[timeCol]) {
                    sum2 += 1;
                    sum3 += b2[priceCol] * b2[timeCol];
                    sum4 += b2[priceCol];
                    sum5 += b2[timeCol];
                }
            }
            Row newrow = b1;
            newrow.push_back(sum2);
            newrow.push_back(sum3);
            newrow.push_back(sum4);
            newrow.push_back(sum5);
            verify.emplace_back(newrow);
            if (sum2 * sum3 > sum4 * sum5)
                result.emplace_back(b1);
        }
        auto end = chrono::steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

struct Bids3DBT : Bids3 {
    Bids3DBT() : Bids3("DBT,CPP") {}

    dbtoaster::data_t obj;

    long long int evaluate(const Table &bids) override {
        result.clear();
        verify.clear();
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

struct Bids3Range : Bids3 {
    Bids3Range() : Bids3("Range,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        verify.clear();
        auto start = steady_clock::now();

        RangeTree rt2(agg, 1);
        rt2.buildFrom(bids, keyFn, 1, valueFn2);
        RangeTree rt3(agg, 1);
        rt3.buildFrom(bids, keyFn, 1, valueFn3);
        RangeTree rt4(agg, 1);
        rt4.buildFrom(bids, keyFn, 1, valueFn4);
        RangeTree rt5(agg, 1);
        rt5.buildFrom(bids, keyFn, 1, valueFn5);

        Table b12, b123, b1234, b12345;
        rt2.join(bids, b12, keyFn, ops);
        rt3.join(b12, b123, keyFn, ops);
        rt4.join(b123, b1234, keyFn, ops);
        rt5.join(b1234, b12345, keyFn, ops);

        for (const auto &r: b12345.rows) {
            verify.emplace_back(r);
            if (r[agg2Col] * r[agg3Col] > r[agg4Col] * r[agg5Col])
                result.emplace_back(Row({r[priceCol], r[timeCol], r[volCol]}));
        }
        auto end = steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

struct Bids3Merge : Bids3 {
    Bids3Merge() : Bids3("Merge,CPP") {}

    long long int evaluate(const Table &bids) override {
        result.clear();
        verify.clear();
        auto start = steady_clock::now();
        Table sortedBids;
        sortedBids.rows = bids.rows;
        sort(sortedBids.rows.begin(), sortedBids.rows.end(), ord);
        vector<Domain> domain(1);
        sortedBids.fillDomain(domain[0], timeCol);
        Cube cube2(domain, agg), cube3(domain, agg), cube4(domain, agg), cube5(domain, agg);
        cube2.fillData(sortedBids, keyFn, valueFn2);
        cube2.accumulate(ops);
        cube3.fillData(sortedBids, keyFn, valueFn3);
        cube3.accumulate(ops);
        cube4.fillData(sortedBids, keyFn, valueFn4);
        cube4.accumulate(ops);
        cube5.fillData(sortedBids, keyFn, valueFn5);
        cube5.accumulate(ops);

        Table b12, b123, b1234, b12345;
        cube2.join(sortedBids, b12, keyFn, ops);
        cube3.join(b12, b123, keyFn, ops);
        cube4.join(b123, b1234, keyFn, ops);
        cube5.join(b1234, b12345, keyFn, ops);

        for (const auto &r: b12345.rows) {
            verify.emplace_back(r);
            if (r[agg2Col] * r[agg3Col] > r[agg4Col] * r[agg5Col])
                result.emplace_back(Row({r[priceCol], r[timeCol], r[volCol]}));
        }
        auto end = steady_clock::now();
        return duration_cast<milliseconds>(end - start).count();
    }
};

int main(int argc, char **argv) {
    vector<Bids3 *> tests;
    tests.emplace_back(new Bids3Naive);
    tests.emplace_back(new Bids3DBT);
    tests.emplace_back(new Bids3Range);
    tests.emplace_back(new Bids3Merge);
    long long maxTimeInMS = 1000 * 60 * 5;
    int testFlag = 0xFF;
    if (argc >= 3) {
        testFlag = stoi(argv[1]);
        maxTimeInMS = stoi(argv[2]) * 60 * 1000;  //arg in minutes
    }
    bool enable = true;
    for (int all = 10; all <= 10 && enable; all += 1) {

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