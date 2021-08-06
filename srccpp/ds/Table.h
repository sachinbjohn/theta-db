//
// Created by Sachin Basil John on 13.04.21.
//

#ifndef TABLE_H
#define TABLE_H

#include <vector>
#include <iostream>
#include "utils/ComparatorOp.h"
#include <functional>
#include <algorithm>
#include <iterator>

using namespace std;

struct Domain {
    vector<double> arr;

    const double &operator[](int i) {
        return arr[i];
    }

    double findPredEq(double v, ComparatorOp *op) const {
        if (op == EqualTo::getInstance())
            return v;

        int l = 0;
        int r = arr.size() - 1;
        int mid = 0;
        while (l <= r) {
            mid = (l + r) / 2;
            if (arr[mid] == v)
                return v;
            else if (op->apply(v, arr[mid]))
                r = mid - 1;
            else
                l = mid + 1;
        }
        int index = op->apply(v, arr[mid]) ? mid - 1 : mid;
        return index == -1 ? op->first : arr[index];
    }
};

typedef vector<double> Row;
typedef vector<double> Key;

struct Table {

    vector<Row> rows;
    typedef iterator<input_iterator_tag, Row> iterator;
    void fillDomain(Domain &d, int column, bool descending = false) {
        d.arr.reserve(rows.size());
        for (const Row &r: rows) {
            d.arr.push_back(r[column]);
        }
        if (descending) {
            sort(d.arr.rbegin(), d.arr.rend());
        } else
            sort(d.arr.begin(), d.arr.end());
        d.arr.erase(unique(d.arr.begin(), d.arr.end()), d.arr.end());
    }
};

typedef void (*KeyFunc)(const Row &, Key &);

typedef double (*ValueFunc)(const Row &);

typedef function<bool(const Row &, const Row &)> SortingFunc;

SortingFunc sorting(KeyFunc keyFunc, const vector<COp> *ops, Key *k1, Key *k2);

SortingFunc
sortingOther(const vector<Domain> *domains, const vector<bool> *domFlags, KeyFunc keyFunc, const vector<COp> *ops,
             Key *k1, Key *k2);

ostream &operator<<(ostream &os, const vector<double> &vs);

ostream &operator<<(ostream &os, const vector<int> &vs);


#endif
