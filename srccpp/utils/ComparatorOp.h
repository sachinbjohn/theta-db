
#ifndef COMPARATOROP_H
#define COMPARATOROP_H
#include <cmath>
struct ComparatorOp {
    ComparatorOp *withEq;
    ComparatorOp *sorting;
    double first;
    double last;
    virtual bool apply(double a, double b) = 0;
    ComparatorOp(double f, double l): first(f), last(l){}
};

struct LessThan : ComparatorOp {
    static LessThan* instance;
    static ComparatorOp *getInstance();
    bool apply(double a, double b) override {
        return a < b;
    }
    LessThan(): ComparatorOp(-INFINITY, INFINITY){}
};

struct LessThanEqual : ComparatorOp {
    static LessThanEqual* instance;
    static ComparatorOp *getInstance();
    bool apply(double a, double b) override {
        return a <= b;
    }
    LessThanEqual(): ComparatorOp(-INFINITY, INFINITY){}
};

struct GreaterThan : ComparatorOp {
    static GreaterThan* instance;
    static ComparatorOp* getInstance();
    bool apply(double a, double b) override {
        return a > b;
    }
    GreaterThan(): ComparatorOp(INFINITY, -INFINITY){}
};

struct GreaterThanEqual : ComparatorOp {
    static GreaterThanEqual* instance;
    static ComparatorOp* getInstance();
    bool apply(double a, double b) override {
        return a >= b;
    }
    GreaterThanEqual(): ComparatorOp(INFINITY, -INFINITY){}
};

struct EqualTo : ComparatorOp {
    static EqualTo* instance;
    static ComparatorOp* getInstance();
    bool apply(double a, double b) override {
        return a == b;
    }
    EqualTo(): ComparatorOp(-INFINITY, INFINITY){}
};

typedef ComparatorOp* COp;
#endif //COMPARATOROP_H
