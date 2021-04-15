
#ifndef BIDSGENERATOR_H
#define BIDSGENERATOR_H

#include <cstdlib>
#include <algorithm>
#include "ds/Table.h"
using namespace std;
void generate(Table &t, int total, int price, int time, int pricetime);
void loadFromFile(Table &t, int total, int price, int time, int pricetime);
#endif //VWAP_BIDSGENERATOR_H
