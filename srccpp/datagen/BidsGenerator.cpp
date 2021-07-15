#include "datagen/BidsGenerator.h"
#include <string>
#include <cstring>
#include <sstream>
#include <fstream>
#include <random>
void loadFromFile(Table &table, int logn, int logr, int logp, int logt) {
    string folder = "../../csvdata";
    stringbuf filename;
    ostream os(&filename);

    os << folder << "/bids_" << logn << "_" << logr << "_" << logp << "_" << logt << ".csv";
    ifstream fin(filename.str());
    if(!fin.is_open()) {
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
    cout << "Generating bids_"<< logn << "_" << logr << "_" << logp << "_" << logt << ".csv" << endl;
    os << folder << "/bids_" << logn << "_" << logr << "_" << logp << "_" << logt << ".csv";
    ofstream fout(filename.str());
    if(!fout.is_open()) {
        throw std::runtime_error("File missing");
    }

    fout << "Price,Time,Volume\n";
    auto rng = std::default_random_engine {};
    int price = 1 << logp;
    int time = 1 << logt;
    int pricetime = 1 << logr;
    int total = 1 << logn;
    uniform_int_distribution<>priceD(0, price-1);
    uniform_int_distribution<>timeD(0, time-1);
    uniform_int_distribution<>pricetimeD(0, pricetime-1);
    uniform_int_distribution<>volumeD(0, 99);

    vector<int> prices( price);
    vector<int> times(time);
    for(int i = 0; i< price; i++)
        prices[i] = i;
    for(int i = 0; i < time; ++i)
        times[i] = i;
    shuffle(prices.begin(),prices.end(), rng);
    shuffle(times.begin(),times.end(), rng);

    vector<pair<int, int>> pricetimes(pricetime);
    int min = price < time ? price : time;
    int max = price < time ? time : price;

    for(int i = 0; i < min; i++)
        pricetimes[i] = make_pair(prices[i], times[i]);
    for(int i = min; i < max; i++) {
        if (price < time)
            pricetimes[i] = make_pair(priceD(rng), times[i]);
        else
            pricetimes[i] = make_pair(prices[i], timeD(rng));
    }
    prices.clear();
    times.clear();

    for(int i = max; i < pricetime; i++)
        pricetimes[i] = make_pair(priceD(rng), timeD(rng));

    for(int i = 0; i < pricetime; i++) {
        fout << pricetimes[i].first << "," << pricetimes[i].second << "," << volumeD(rng) << "\n";
    }
    for(int i = pricetime; i < total; i++) {
        int r = pricetimeD(rng);
        fout << pricetimes[r].first << "," << pricetimes[r].second << "," << volumeD(rng) << "\n";
    }
    fout.close();
};
