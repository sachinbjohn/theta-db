
#ifndef BIDSGENERATOR_H
#define BIDSGENERATOR_H

#include <cstdlib>
#include <algorithm>
#include "ds/Table.h"
using namespace std;
void writeToFile(int logn, int logr, int logp, int logt);
void loadFromFile(Table &table, int logn, int logr, int logp, int logt);
#endif //VWAP_BIDSGENERATOR_H
