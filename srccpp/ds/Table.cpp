#include "ds/Table.h"

SortingFunc sorting(KeyFunc keyFunc, const vector<COp> *ops, Key* k1, Key* k2) {

    return [=](const Row &r1, const Row &r2) -> bool {
        int D = ops->size();
        keyFunc(r1, *k1);
        keyFunc(r2, *k2);

        bool a = true;
        bool b = false;

        for (int i = 0; i < D; ++i) {
            int x = (*k1)[i];
            int y = (*k2)[i];
            b = b || (a && (*ops)[i]->sorting->apply(x, y));
            a = a && (x == y);
        }

//        cout << k1 << " < " << k2 << " = " << b << endl;
        return b;
    };
}

SortingFunc
sortingOther(const vector<Domain> *domains, const vector<bool> *domFlags, KeyFunc keyFunc, const vector<COp> *ops,
             Key *k1, Key *k2) {
    return [=](const Row &r1, const Row &r2) -> bool {
        const int D = ops->size();
        keyFunc(r1, *k1);
        keyFunc(r2, *k2);

        bool a = true;
        bool b = false;

        for (int i = 0; i < D; ++i) {
            int x = (*domFlags)[i] ? (*domains)[i].findPredEq((*k1)[i], (*ops)[i]) : (*k1)[i];
            int y = (*domFlags)[i] ? (*domains)[i].findPredEq((*k2)[i], (*ops)[i]) : (*k2)[i];
            b = b || (a && (*ops)[i]->sorting->apply(x, y));
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