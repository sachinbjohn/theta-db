#include <iostream>
#include "utils/ComparatorOp.h"
#include "ds/Table.h"
#include "ds/Cube.h"
#include <algorithm>

using namespace std;

int main() {
    std::cout << "Hello, World!" << std::endl;
    ComparatorOp *lessThan = LessThan::getInstance();
    cout << lessThan->apply(4, 4) << endl;

    vector<Domain> domains(3);
    domains[0].arr = {10, 11, 12, 13, 14};
    domains[1].arr = {20, 21, 22};
    domains[2].arr = {30, 31, 32, 33, 34, 35, 36};
    Cube c(domains);
    cout << "Total = "<<c.totalSize << endl;
    vector<int> dims(2);
    for(int i = 0; i < c.totalSize; i++){
        c.OneToD(i, dims);
        int j = c.DtoOne(dims);
        assert(i == j);
    }

    Table relT;
    relT.rows.emplace_back(Row({3, 10, 10}));
    relT.rows.emplace_back(Row({2, 20, 15}));
    relT.rows.emplace_back(Row({4, 20, 12}));
    relT.rows.emplace_back(Row({7, 30, 34}));
    relT.rows.emplace_back(Row({2, 40, 9}));
    relT.rows.emplace_back(Row({4, 50, 7}));
    relT.rows.emplace_back(Row({7, 60, 5}));
    relT.rows.emplace_back(Row({2, 60, 34}));
    relT.rows.emplace_back(Row({3, 70, 8}));
    relT.rows.emplace_back(Row({4, 70, 55}));
    relT.rows.emplace_back(Row({7, 70, 1}));

    ComparatorOp *const leq = LessThanEqual::getInstance();
    ComparatorOp *const gt = GreaterThan::getInstance();
    auto keyFunc = [](const Row &r, Key &k) {
        k[0] = r[0];
        k[1] = r[1];
    };

    auto valueFunc = [](const Row &r) {
        return r[2];
    };
    vector<COp> ops = {leq, gt};
    cout << "BEFORE " << endl;
    for (auto const &r: relT.rows)
        cout << r << endl;

    auto sortFn = sorting(keyFunc, ops);
    sort(relT.rows.begin(), relT.rows.end(), sortFn);

    cout << "AFTER " << endl;
    for (auto const &r: relT.rows)
        cout << r << endl;

    vector<Domain> domainsR(2);
    relT.fillDomain(domainsR[0], 0, false);
    relT.fillDomain(domainsR[1], 1, true);

    cout << "DOMAIN 1" << endl;
    for (const double &d: domainsR[0].arr)
        cout << d << endl;

    cout << "DOMAIN 2" << endl;
    for (const double &d: domainsR[1].arr)
        cout << d << endl;

    auto sortFn2 = sortingOther(domainsR, keyFunc, ops);

    Cube c3(domainsR);
    cout << c3.totalSize << endl;
    c3.fillData(relT, keyFunc, valueFunc);
    c3.accumulate(ops);

    Table relS;
    relS.rows.emplace_back(Row({3, 30}));
    relS.rows.emplace_back(Row({5, 20}));
    relS.rows.emplace_back(Row({7, 35}));
    relS.rows.emplace_back(Row({6, 45}));

    sort(relS.rows.begin(), relS.rows.end(), sortFn2);

    for (auto const &s: relS.rows)
        cout << "row S" << s << endl;

    Table result;
    c3.join(relS, result, keyFunc,ops);

    cout << "Join Result" << endl;
    for (const Row &r : result.rows)
        cout << r << endl;


    return 0;

}
