#include "datagen/BidsGenerator.h"
#include <string>
#include <cstring>
#include <sstream>
#include <fstream>
#include <filesystem>
namespace fs = std::__fs::filesystem;
void loadFromFile(Table &table, int total, int price, int time, int pricetime) {
    string folder = "../../csvdata";
    stringbuf filename;
    ostream os(&filename);

    os << folder << "/bids_" << total << "_" << pricetime << "_"  << price << "_" << time << ".csv";
    ifstream fin(filename.str());
    if(!fin.is_open()) {
        std::cerr << "Current path is " << fs::current_path() << '\n'; // (1)
        cerr << filename.str() <<endl;
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

//void generate(Table &table, int total, int price, int time, int pricetime) {
//    srand(0);
//    vector<double> allPT(price * time);
//    for (int i = 0; i < price * time; i++)
//        allPT.push_back(i);
//    random_shuffle(allPT.begin(), allPT.end());
//    for (int i = 0; i < total; ++i) {
//        double vol = rand() % 100;
//        int pt = allPT[rand() % pricetime];
//        double p = pt / time;
//        double t = pt % time;
//        table.rows.emplace_back(Row({p, t, vol}));
//    }
//}
