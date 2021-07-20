#include "datagen/BidsGenerator.h"
#include <string>
#include <cstring>
#include <sstream>
#include <fstream>
#include <random>
#include <unordered_set>

void loadFromFile(Table &table, int logn, int logr, int logp, int logt) {
    string folder = "../../csvdata";
    stringbuf filename;
    ostream os(&filename);

    os << folder << "/bids_" << logn << "_" << logr << "_" << logp << "_" << logt << ".csv";
    ifstream fin(filename.str());
    if (!fin.is_open()) {
        throw std::runtime_error("File missing");
    }
    string line;
    getline(fin, line);
    double pval, tval, vval;
    getline(fin, line);
    while (fin.good()) {
        sscanf(line.c_str(), "%lf,%lf,%lf", &pval, &tval, &vval);
        getline(fin, line);
        table.rows.emplace_back(Row({pval, tval, vval}));
    }
}

void writeToFile(int logn, int logr, int logp, int logt) {
    string folder = "/var/data/csvdata";
    stringbuf filename;
    ostream os(&filename);
    cout << "Generating bids_" << logn << "_" << logr << "_" << logp << "_" << logt << ".csv" << endl;
    os << folder << "/bids_" << logn << "_" << logr << "_" << logp << "_" << logt << ".csv";
    ofstream fout(filename.str());
    if (!fout.is_open()) {
        throw std::runtime_error("File missing");
    }

    fout << "Price,Time,Volume\n";
    auto rng = std::default_random_engine{};
    unsigned long long price = 1ull << logp;
    unsigned long long time = 1ull << logt;
    unsigned long long pricetime = 1ull << logr;
    unsigned long long total = 1 << logn;
    uniform_int_distribution<unsigned long long> priceD(0, price - 1);
    uniform_int_distribution<unsigned long long> timeD(0, time - 1);
    uniform_int_distribution<unsigned long long> pricetimeD(0, pricetime - 1);
    uniform_int_distribution<unsigned long long> volumeD(0, 99);

    vector<unsigned long long> prices(price);
    vector<unsigned long long> times(time);
    unordered_set<unsigned long long> ptindex(pricetime);


    // i = p*T + t
    // p = i/T  t= i%T

    for (unsigned long long i = 0; i < price; i++)
        prices[i] = i;
    for (unsigned long long i = 0; i < time; ++i)
        times[i] = i;
    shuffle(prices.begin(), prices.end(), rng);
    shuffle(times.begin(), times.end(), rng);

    vector<pair<unsigned long long, unsigned long long>> pricetimes(pricetime);
    unsigned long long min = price < time ? price : time;
    unsigned long long max = price < time ? time : price;

    for (unsigned long long i = 0; i < min; i++)
        ptindex.insert(i * time + i);
    for (unsigned long long i = min; i < max; i++) {
        if (price < time)
            ptindex.insert(priceD(rng) * time + i);
        else
            ptindex.insert(i * time + timeD(rng));
    }

    // i = p*T + t
    // p = i/T  t= i%T
    unsigned long long count = ptindex.size();
    for (unsigned long long i = 0; i < 10 * pricetime && count < pricetime; i++) {
        ptindex.insert(time * priceD(rng) + timeD(rng));
        count = ptindex.size();
    }

    for (unsigned long long i = 0; i < price && count < pricetime; i++)
        for (unsigned long long j = 0; j < time && count < pricetime; j++) {
            ptindex.insert(time * i + j);
            count = ptindex.size();
        }

    if(count != pricetime) {
        throw std::runtime_error("Unique pricetime values not generated sufficiently");
    }
    unsigned long long i2 = 0;
    for (const auto pt: ptindex) {
        unsigned long long p = pt / time;
        unsigned long long t = pt % time;
        pricetimes[i2++] = make_pair(prices[p], times[t]);
    }

    for (unsigned long long i = 0; i < pricetime; i++) {
        fout << pricetimes[i].first << "," << pricetimes[i].second << "," << volumeD(rng) << "\n";
    }
    for (unsigned long long i = pricetime; i < total; i++) {
        unsigned long long r = pricetimeD(rng);
        fout << pricetimes[r].first << "," << pricetimes[r].second << "," << volumeD(rng) << "\n";
    }
    fout.close();
};
