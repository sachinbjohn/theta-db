#ifndef CUBE_H
#define CUBE_H

#include <vector>
#include "ds/Table.h"
#include "utils/ComparatorOp.h"
#include "utils/Aggregator.h"
#include <iostream>

using namespace std;

struct BigArray {
    static const long P = 1 << 25;
    long long size;
    vector<double *> data;

    virtual ~BigArray() {
        for (auto ptr: data)
            delete ptr;
    }

    double &operator[](long long n) {
        int a = (int) n / P;
        int b = n % P;
        return data[a][b];
    }

    void reserve(long long s, double zero) {
        size = s;
        int N = size / P + 1;
        int mod = size % P;
        data.reserve(N);
        double *array;
        for (int i = 0; i < N - 1; ++i) {
            array = new double[P];
            fill(array, array + P, zero);
            data.push_back(array);
        }
        array = new double[mod];
        fill(array, array + mod, zero);
        data.push_back(array);
    }
};

struct Cube {
    const vector<Domain> *domains;
    BigArray data;
    const int D;
    long long totalSize;
    const Aggregator *agg;

    Cube(const vector<Domain> &d, const Aggregator &a) : domains(&d), D(domains->size()), agg(&a) {
        totalSize = 1;
        for (int i = 0; i < D; ++i)
            totalSize *= (*domains)[i].arr.size();
        data.reserve(totalSize, agg->zero);
    }


    double &operator[](const vector<int> &dims) {
        long long n = DtoOne(dims);
        return data[n];
    }

    long long DtoOne(const vector<int> &dims) {
        long long n = 0;
        for (int i = 0; i < D; ++i) {
            n = n * (*domains)[i].arr.size() + dims[i];
        }
        return n;
    }

    void OneToD(long long n, vector<int> &dims) {
        long long index = n;
        for (int i = 1; i <= D; ++i) {
            dims[D - i] = (index % (*domains)[D - i].arr.size());
            index /= (*domains)[D - i].arr.size();
        }
    }

    void
    join(const Table &input, Table &output, KeyFunc keyFunc, const vector<COp> &ops) {
        vector<int> dim(D, -1);
        Key key(D, 0.0);
        output.rows = input.rows;
        for (Row &row: output.rows) {
            keyFunc(row, key);
            bool reset = false;
            bool isZero = false;
            for (int i = 0; i < D; ++i) {
                int index = reset ? -1 : dim[i];
                bool opIsEqualTo = ops[i] == EqualTo::getInstance();

                double di = (index == -1) ? ops[i]->first : (*domains)[i].arr[index];
                double disucc = (index == (*domains)[i].arr.size() - 1) ? ops[i]->last : (*domains)[i].arr[index + 1];

                while (opIsEqualTo ? disucc <= key[i] : ops[i]->apply(disucc, key[i])) {
                    index++;
                    di = (index == -1) ? ops[i]->first : (*domains)[i].arr[index];
                    disucc = (index == (*domains)[i].arr.size() - 1) ? ops[i]->last : (*domains)[i].arr[index + 1];

                }
                isZero = isZero || (opIsEqualTo ? key[i] != di : index == -1);

                if (dim[i] != index) {
                    reset = true;
                    dim[i] = index;
                }
            }
            double v = agg->zero;
            if (!isZero)
                v = (*this)[dim];

            row.push_back(v);
//            cout << row << endl;

        }
    }

    void accumulate(const vector<COp> &ops) {
        long long skip = totalSize;
        vector<int> dims(D);
        for (int i = 0; i < D; ++i) {
            skip /= (*domains)[i].arr.size();
            if (ops[i] != EqualTo::getInstance()) {
                long long n = skip;
                while (n < totalSize) {
                    if ((n / skip) % (*domains)[i].arr.size() != 0) {
                        data[n] = agg->apply(data[n], data[n - skip]);
                    }
                    n++;
                }
            }
        }
    }

    void fillData(const Table &t, KeyFunc keyFunc, ValueFunc valueFunc) {
        vector<int> dim(D, 0);
        Key key(D, 0.0);
        for (const Row &row : t.rows) {
            keyFunc(row, key);
//            cout << "Row = " << row << "  key = " << key << endl;
            bool reset = false;
            for (int i = 0; i < D; ++i) {
                int index = reset ? 0 : dim[i];
                while (key[i] != (*domains)[i].arr[index])
                    index++;
                if (dim[i] != index) {
                    reset = true;
                    dim[i] = index;
                }
            }
            (*this)[dim] = agg->apply((*this)[dim], valueFunc(row));
        }
    }
};


#endif
