#include "BidsGenerator.h"

int main(int argc, char *argv[]) {
    if (argc < 5) {
        cerr << "Specify parameter" << endl;
    } else {

        int logn = stoi(argv[1]);
        int logr = stoi(argv[2]);
        int logp = stoi(argv[3]);
        int logt = stoi(argv[4]);
        writeToFile(logn, logr, logp, logt);
//        logp = allparam-5;
//        logt = 10;
//         writeToFile(logn, logr, logp, logt);
    }
}
