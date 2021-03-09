#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
from math import sqrt

input = "test.csv"
# input = sys.argv[1]
data = list(csv.DictReader(open(input, 'r')))

print("Input = " + input)

qf = lambda kv: kv[0][0]
nf = lambda kv: kv[0][1]
pf = lambda kv: kv[0][2]
tf = lambda kv: kv[0][3]
ptf = lambda kv: kv[0][4]
af = lambda kv: kv[0][5]


def keyFn(x):
    return x['Query'], int(x['Total']), int(x['Price']), int(x['Time']), int(x['PriceTime']), int(x['Algo'])


def valueFn(x):
    return (int(x['Ex1']) + int(x['Ex2']) + int(x['Ex3'])) / 3.0


processedData = {}

for x in data:
    processedData[keyFn(x)] = valueFn(x)


def plot(args):
    (f, k, xl, title, name) = args
    filteredData = sorted(filter(f, processedData.iteritems()), key=k)
    extractKV = lambda kv: (k(kv), kv[1])
    Z1 = map(extractKV, filter(lambda kv: af(kv) == 1, filteredData))
    Z2 = map(extractKV, filter(lambda kv: af(kv) == 2, filteredData))
    Z3 = map(extractKV, filter(lambda kv: af(kv) == 4, filteredData))
    Z4 = map(extractKV, filter(lambda kv: af(kv) == 8, filteredData))
    fig_size = plt.rcParams["figure.figsize"]
    # Prints: [8.0, 6.0]
    # fig_size[0] = 20
    getX = lambda z: map(lambda kv: kv[0], z)
    getY = lambda z: map(lambda kv: kv[1], z)
    fig_size[1] = 3
    fig, pl = plt.subplots()
    pl.plot(getX(Z1), getY(Z1), '-x', label='Naive')
    pl.plot(getX(Z3), getY(Z3), '-s', label='Inner Lookup')
    pl.plot(getX(Z4), getY(Z4), '-^', label='Merge Lookup')
    pl.plot(getX(Z2), getY(Z2), '-o', label='DBToaster')
    plt.xlabel(xl)
    plt.ylabel("Execution time (ms)")
    plt.legend(loc=2, fontsize='small', frameon=False)
    pl.set_yscale('log')
    pl.set_xscale('log')
    plt.title("\n".join(wrap(title, 60)))
    fig.tight_layout()
    plt.rcParams["figure.figsize"] = fig_size
    plt.savefig(name)


def totalP(qi):
    q = "Q" + str(qi)
    f1 = lambda kv: qf(kv) == q and nf(kv) == pf(kv) * 64 and 64 == tf(kv) and nf(kv) == ptf(kv) * 4
    x1 = "N"
    t1 = "Vary N"
    n1 = "Total {}.png".format(q)
    return f1, nf, x1, t1, n1


def priceP(qi):
    q = "Q" + str(qi)
    f2 = lambda kv: qf(kv) == q and nf(kv) == 2 ** 14 and tf(kv) == 2 ** 7 and ptf(kv) == 2 ** 12
    x2 = "P"
    t2 = "Vary P"
    n2 = "Price {}.png".format(q)
    return f2, pf, x2, t2, n2


def timeP(qi):
    q = "Q" + str(qi)
    f3 = lambda kv: qf(kv) == q and nf(kv) == 2 ** 14 and pf(kv) == 2 ** 7 and ptf(kv) == 2 ** 9
    x3 = "T"
    t3 = "Vary T"
    n3 = "Time {}.png".format(q)
    return f3, tf, x3, t3, n3


def pricetimeP1(qi):
    q = "Q" + str(qi)
    f4 = lambda kv: qf(kv) == q and nf(kv) == ptf(kv) * 4 and pf(kv) == 2 ** 8 and tf(kv) == 2 ** 8
    x4 = "PT"
    t4 = "Vary PT"
    n4 = "PriceTime1 {}.png".format(q)
    return f4, ptf, x4, t4, n4


def pricetimeP2(qi):
    q = "Q" + str(qi)

    def fs(kv):
        if qi == 1:
            return tf(kv) == 2 ** 12
        elif qi == 2:
            return tf(kv) * pf(kv) == 2 ** 14
        else:
            return tf(kv) == 2 ** 12 / int(sqrt(pf(kv)))

    f = lambda kv: qf(kv) == q and nf(kv) == 2 ** 14 and ptf(kv) == 2 ** 12 and fs(kv)
    x = "P"
    t = "Keeping P PT or PT^2 Constant"
    n = "PriceTime2 {}.png".format(q)
    return f, pf, x, t, n


plot(totalP(1))
plot(totalP(2))
plot(totalP(3))

plot(priceP(1))
plot(priceP(2))
plot(priceP(3))

plot(timeP(1))
plot(timeP(2))
plot(timeP(3))

plot(pricetimeP1(1))
plot(pricetimeP1(2))
plot(pricetimeP1(3))

plot(pricetimeP2(1))
plot(pricetimeP2(2))
plot(pricetimeP2(3))
