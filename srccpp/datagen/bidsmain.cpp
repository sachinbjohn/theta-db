#include "BidsGenerator.h"

int main(int argc, char *argv[]) {
    if(argc < 2) {
        cerr << "Specify parameter" << endl;
    } else {
        int allparam = stoi(argv[1]);
        int logn = allparam;
        int logp = allparam;
        int logt = allparam;
        int logr = allparam;
        writeToFile(logn, logp, logt, logr);
    }
}
