#ifndef AGGREGATOR_H
#define AGGREGATOR_H

#include <cmath>

struct Aggregator {
    double zero = 0;
    virtual double apply(double x, double y) = 0;
    Aggregator(double z) : zero(z) {}
};

struct AggPlus : Aggregator {
    AggPlus() : Aggregator(0) {}
    double apply(double x, double y) override {
        x + y;
    }
};

struct AggMax: Aggregator {
    AggMax: Aggregator(-INFINITY){}

    double apply(double x, double y) override {
        return v1 < v2 ? v2 : v1;
    }
}

struct AggMin: Aggregator {
    AggMin: Aggregator(INFINITY) {}

    double apply(double x, double y) override {
        return v1 < v2 ? v1 : v2;
    }
};
#endif
