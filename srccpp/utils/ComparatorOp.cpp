#include "ComparatorOp.h"

LessThan* LessThan::instance = nullptr;
GreaterThan* GreaterThan::instance  = nullptr;
LessThanEqual* LessThanEqual::instance  = nullptr;
GreaterThanEqual* GreaterThanEqual::instance  = nullptr;
EqualTo* EqualTo::instance;

ComparatorOp *LessThan::getInstance() {
    if (!instance) {
        instance = new LessThan;
        instance->withEq = LessThanEqual::getInstance();
        instance->sorting = instance;
    }
    return instance;
}

ComparatorOp *LessThanEqual::getInstance() {
    if (!instance) {
        instance = new LessThanEqual;
        instance->withEq = instance;
        instance->sorting = LessThan::getInstance();
    }
    return instance;
}


ComparatorOp *GreaterThanEqual::getInstance() {
    if (!instance) {
        instance = new GreaterThanEqual;
        instance->withEq = instance;
        instance->sorting = GreaterThan::getInstance();
    }
    return instance;
}


ComparatorOp *GreaterThan::getInstance() {
    if (!instance) {
        instance = new GreaterThan;
        instance->withEq = GreaterThanEqual::getInstance();
        instance->sorting = instance;
    }
    return instance;
}


ComparatorOp *EqualTo::getInstance() {
    if (!instance) {
        instance = new EqualTo;
        instance->withEq = instance;
        instance->sorting = LessThan::getInstance();
    }
    return instance;
}