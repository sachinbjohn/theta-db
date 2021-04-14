
#include "ds/RangeTree.h"
#include <iostream>

std::ostream &operator<<(std::ostream &os, const Node &node) {
    os << "[" << node.keyUpper << ":" << node.keyLower << "] = " << node.value << "  @  " << &node;
    return os;
}
