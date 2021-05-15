#include "BidsGenerator.h"

int main(int argc, char *argv[]) {
    if (argc < 2) {
        cerr << "Specify parameter" << endl;
    } else {
        int allparam = stoi(argv[1]);
        int logn = allparam;
        int logr = allparam;
        int logp = allparam;
        int logt = allparam;
        writeToFile(logn, logr, logp, logt);
        logp = allparam-5;
        logt = 10;
         writeToFile(logn, logr, logp, logt);
    }
}
