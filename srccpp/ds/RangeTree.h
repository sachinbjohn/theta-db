
#ifndef RANGETREE_H
#define RANGETREE_H

#include <ostream>
#include "utils/Aggregator.h"
#include "ds/Table.h"
#include <map>
#include "utils/groupBy.h"

struct RangeTree;

struct Node {
    double keyUpper;
    double keyLower;
    double value;

    Node *leftChild;
    Node *rightChild;
    Node *parent;
    RangeTree *prevDim;
    RangeTree *nextDim;

    Node() : leftChild(nullptr), rightChild(nullptr), parent(nullptr), prevDim(nullptr), nextDim(nullptr) {}

    void setLeft(Node *n) {
        if (!n)
            n->parent = this;
        leftChild = n;
    }

    void setRight(Node *n) {
        if (!n)
            n->parent = this;
        rightChild = n;
    }

    friend std::ostream &operator<<(std::ostream &os, const Node &node);
};

struct Range {
    double l;
    double r;
    bool lc;
    bool rc;

    void make(COp op, double x) {
        if (op == LessThan::getInstance()) {
            l = -INFINITY;
            r = x;
            lc = false;
            rc = false;
        } else if (op == LessThanEqual::getInstance()) {
            l = -INFINITY;
            r = x;
            lc = false;
            rc = true;
        } else if (op == EqualTo::getInstance()) {
            l = r = x;
            lc = rc = true;
        } else if (op == GreaterThanEqual::getInstance()) {
            l = x;
            r = INFINITY;
            lc = true;
            rc = false;
        } else {
            l = x;
            r = INFINITY;
            lc = false;
            rc = false;
        }
    }
};

struct RangeTree {
    Node *root;
    const Aggregator* agg;
    const int D;

    RangeTree(const Aggregator &a, int dim) : root(nullptr), agg(&a), D(dim) {}

    double rangeQuery(const vector<Range> &ranges, int i = 0) {
        return rangeQueryRec(ranges, i, root);
    }

    double rangeQueryRec(const vector<Range> &ranges, int i, Node *n) {
        bool lc = ranges[i].lc;
        bool rc = ranges[i].rc;
        double l = ranges[i].l;
        double r = ranges[i].r;
        if (!n)
            return agg -> zero;

        bool c11 = lc ? l > n->keyUpper : l >= n->keyUpper;
        bool c12 = rc ? r < n->keyLower : r <= n->keyLower;
        bool c1 = c11 || c12;

        if (c1)
            return agg -> zero;

        bool c21 = lc ? l <= n->keyLower : l < n->keyLower;
        bool c22 = rc ? n->keyUpper <= r : n->keyUpper < r;
        bool c2 = c21 && c22;

        if (c2)
            return (D == 1) ? n->value : n->nextDim->rangeQuery(ranges, i + 1);

        bool c3 = rc ? r < n->rightChild->keyLower : r <= n->rightChild->keyLower;
        if (c3)
            return rangeQueryRec(ranges, i, n->leftChild);

        bool c4 = lc ? l > n->leftChild->keyUpper : l >= n->leftChild->keyUpper;
        if (c4)
            return rangeQueryRec(ranges, i, n->rightChild);

        double lval = rangeQueryRec(ranges, i, n->leftChild);
        double rval = rangeQueryRec(ranges, i, n->rightChild);
        return agg -> apply(lval, rval);
    }

    void join(const Table &input, Table &output, KeyFunc keyFunc, const vector<COp> &ops) {
        vector<Range> ranges(D);
        Key key(D);
        for (const auto &r: input.rows) {
            keyFunc(r, key);
            for (int i = 0; i < D; i++)
                ranges[i].make(ops[i], key[i]);
            double v = rangeQuery(ranges);
            Row newrow = r;
            newrow.push_back(v);
            output.rows.emplace_back(move(newrow));

        }
    }

    void buildFrom(const Table &t, KeyFunc keyFunc, int totalDim, ValueFunc valueFunc) {
        vector<Key> keys;
        keys.reserve(t.rows.size());
        for(int i = 0; i < t.rows.size(); i++) {
            keys.emplace_back( Key(D, 0));
        }

        map<Key *, double> kvrow;
        vector<pair<Key *, double>> kvrowvect;
        auto keyit = keys.begin();
        for (const auto &r : t.rows) {
            keyFunc(r, *keyit);
            kvrow[&(*keyit)] = agg->apply( kvrow[&(*keyit)] , valueFunc(r));
            keyit++;
        }
        kvrowvect.reserve(kvrow.size());
        for (const auto &kv : kvrow)
            kvrowvect.emplace_back(kv.first, kv.second);
        buildLayer(kvrowvect, 0, totalDim);
    }

    void buildLayer(const vector<pair<Key *, double>> &rows, int currentDim, int totalDim) {

        auto _keys = groupBy(rows.begin(), rows.end(),
                             [=](const pair<Key *, double> &kv) { return kv.first->at(currentDim); });

        root = buildDim(_keys, currentDim, totalDim);
    }

    Node *buildDim(const map<double, vector<pair<Key *, double>>> &keys, int currentDim, int totalDim) {
        int keySize = keys.size();
        Node *n = new Node();
        if (keySize == 1) {
            auto it = keys.begin();

            n->keyLower = it->first;
            n->keyUpper = n->keyLower;
            if (currentDim == totalDim - 1) {
                double value = agg -> zero;
                for (auto v : it->second)
                    value = agg -> apply(value, v.second);
                n->value = value;
            } else {
                n->nextDim = new RangeTree(*agg, totalDim - currentDim - 1);
                n->nextDim->buildLayer(it->second, currentDim + 1, totalDim);
            }
        } else {
            map<double, vector<pair<Key *, double>>> left, right;
            int i = 0;
            auto it = keys.begin();
            int mid = (keySize - 1) / 2;

            while (i <= mid) {
                left.emplace(make_pair(it->first, it->second));
                it++;
                i++;
            }
            while (i < keySize) {
                right.emplace(make_pair(it->first, it->second));
                it++;
                i++;
            }
            n->setLeft(buildDim(left, currentDim, totalDim));
            n->setRight(buildDim(right, currentDim, totalDim));
            if (currentDim == totalDim - 1)
                n->value = agg -> apply(n->leftChild->value, n->rightChild->value);
            else {
                n->nextDim = new RangeTree(*agg, totalDim - currentDim - 1);
                vector<pair<Key *, double>> allkeys;
                for (auto const &kv: keys)
                    allkeys.insert(allkeys.end(), kv.second.begin(), kv.second.end());
                n->nextDim->buildLayer(allkeys, currentDim + 1, totalDim);
            }
            n->keyLower = n->leftChild->keyLower;
            n->keyUpper = n->rightChild->keyUpper;
        }
        return n;
    }
};


#endif //VWAP_RANGETREE_H
