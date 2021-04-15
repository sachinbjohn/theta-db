#include "ds/Table.h"

SortingFunc sorting(const KeyFunc keyFunc, const vector<COp> &ops) {
    const int D = ops.size();
    return [=](const Row &r1, const Row &r2) -> bool {
        Key k1(D);
        Key k2(D);
        keyFunc(r1, k1);
        keyFunc(r2, k2);

        bool a = true;
        bool b = false;

        for (int i = 0; i < D; ++i) {
            int x = k1[i];
            int y = k2[i];
            b = b || (a && ops[i]->sorting->apply(x, y));
            a = a && (x == y);
        }

//        cout << k1 << " < " << k2 << " = " << b << endl;
        return b;
    };
}

SortingFunc sortingOther(const vector<Domain> &domains, const KeyFunc keyFunc, const vector<COp> &ops) {
    const int D = ops.size();
    return [=](const Row &r1, const Row &r2) -> bool {
        Key k1(D);
        Key k2(D);
        keyFunc(r1, k1);
        keyFunc(r2, k2);

        bool a = true;
        bool b = false;

        for (int i = 0; i < D; ++i) {
            int x = domains[i].findPredEq(k1[i], ops[i]);
            int y = domains[i].findPredEq(k2[i], ops[i]);
            b = b || (a && ops[i]->sorting->apply(x, y));
            a = a && (x == y);
        }

//    cout << k1 << " < " << k2 << " = " << b << endl;
        return b;
    };
}

ostream &operator<<(ostream &os, const vector<double> &vs) {
    os << "[";
    for (double v: vs) {
        os << " " << v;
    }
    os << "]";
    return os;
}

ostream &operator<<(ostream &os, const vector<int> &vs) {
    os << "[";
    for (int v: vs) {
        os << " " << v;
    }
    os << "]";
    return os;
}