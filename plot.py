#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
from math import sqrt

# input = "test.csv"
folder = sys.argv[1]
input = folder + "/output.csv"
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
    Z1 = map(extractKV, filter(lambda kv: af(kv) == 16, filteredData))
    Z2 = map(extractKV, filter(lambda kv: af(kv) == 2, filteredData))
    Z3 = map(extractKV, filter(lambda kv: af(kv) == 4, filteredData))
    Z4 = map(extractKV, filter(lambda kv: af(kv) == 8, filteredData))

    getX = lambda z: map(lambda kv: kv[0], z)
    getY = lambda z: map(lambda kv: kv[1], z)


    fig_size = plt.rcParams["figure.figsize"]
    fig_size[1] = 3
    fig, pl = plt.subplots()
    pl.plot(getX(Z1), getY(Z1), '-x', label='DBT LMS')
    pl.plot(getX(Z3), getY(Z3), '-s', label='Inner Lookup')
    pl.plot(getX(Z4), getY(Z4), '-^', label='Merge Lookup')
    pl.plot(getX(Z2), getY(Z2), '-o', label='DBT My')
    plt.xlabel(xl)
    plt.ylabel("Execution time (ms)")
    plt.legend(loc=2, fontsize='small', frameon=False)
    pl.set_yscale('log', basey=2)
    pl.set_xscale('log', basex=2)
    plt.title("\n".join(wrap(title, 60)))
    fig.tight_layout()
    plt.rcParams["figure.figsize"] = fig_size
    plt.savefig(folder+"/"+name)



def exp1a(qi):
    q = "Q" + str(qi)
    P = 2 ** 6
    T = 2 ** 10
    filterf = lambda kv: qf(kv) == q and P == pf(kv) and T == tf(kv) and nf(kv) == ptf(kv)
    xlabel = "Number of rows N"
    title = "Vary N for Query {} with P={} T={}".format(q,P,T)
    name = "Exp1a-{}.png".format(q)
    return filterf, nf, xlabel, title, name


def exp1b(qi):
    q = "Q" + str(qi)
    P = 2 ** 8
    T = 2 ** 8

    filterf = lambda kv: qf(kv) == q and  P == pf(kv) and T == tf(kv) and nf(kv) == ptf(kv)
    xlabel = "Number of rows N"
    title = "Vary N for Query {} with P={} T={} R=N".format(q, P, T)
    name = "Exp1b-{}.png".format(q)
    return filterf, nf, xlabel, title, name


def exp2a(qi):
    q = "Q" + str(qi)
    N = 2 ** 18
    T = 2 ** 8
    R = 2 ** 12
    filterf = lambda kv: qf(kv) == q and nf(kv) == N and tf(kv) == T and ptf(kv) == R
    xlabel = "Number of unique price values P"
    title = "Vary P for Query {} with N={} R={} T={}".format(q, N, R, T)
    name = "Exp2a-{}.png".format(q)
    return filterf, pf, xlabel, title, name


def exp2b(qi):
    q = "Q" + str(qi)
    N = 2 ** 12
    T = 2 ** 8
    R = 2 ** 12
    filterf = lambda kv: qf(kv) == q and nf(kv) == N and tf(kv) == T and ptf(kv) == R
    xlabel = "Number of unique price values P"
    title = "Vary P for Query {} with N={} R={} T={}".format(q, N, R, T)
    name = "Exp2b-{}.png".format(q)
    return filterf, pf, xlabel, title, name


def exp3(qi):
    q = "Q" + str(qi)
    filterf = lambda kv: qf(kv) == q and nf(kv) == 2 ** 14 and pf(kv) == 2 ** 7 and ptf(kv) == 2 ** 9
    xlabel = "T"
    title = "Vary T"
    name = "Exp3-{}.png".format(q)
    return filterf, tf, xlabel, title, name


def exp4(qi):
    q = "Q" + str(qi)
    P = 2 ** 8
    T = 2 ** 8
    R = 2 ** 12
    filterf = lambda kv: qf(kv) == q and  pf(kv) == P and tf(kv) == T and ptf(kv) == R
    xlabel= "N/R"
    title = "Vary N for Query {} with R={} P={} T={}".format(q, R, P, T)
    name = "Exp4-{}.png".format(q)
    keyf = lambda kv: nf(kv)/ptf(kv)
    return filterf, keyf, xlabel, title, name



plot(exp1a(1))
plot(exp1a(2))
plot(exp1a(3))

plot(exp1b(1))
plot(exp1b(2))
plot(exp1b(3))


plot(exp2a(1))
plot(exp2a(2))
plot(exp2a(3))

plot(exp2b(1))
plot(exp2b(2))
plot(exp2b(3))


plot(exp4(1))
plot(exp4(2))
plot(exp4(3))

