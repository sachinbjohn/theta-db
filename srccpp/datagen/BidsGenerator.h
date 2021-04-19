
#ifndef BIDSGENERATOR_H
#define BIDSGENERATOR_H

#include <cstdlib>
#include <algorithm>
#include "ds/Table.h"
using namespace std;
void writeToFile(int logn, int logp, int logt, int logr);
void loadFromFile(Table &t, int logn, int logp, int logt, int logr);
#endif //VWAP_BIDSGENERATOR_H
