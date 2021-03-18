#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
from math import sqrt

# input = "test.csv"
folder = "output/" + sys.argv[1]
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
    return x['Query'], int(x['Total']), int(x['Price']), int(x['Time']), int(x['PriceTime']), x['Algo']


def valueFn(x):
    return int(x['ExTime'])


def mean(x):
    return sum(x) / len(x)


data2 = defaultdict(lambda: [])

for x in data:
    data2[keyFn(x)].append(valueFn(x))

processedData = map(lambda x: (x[0], mean(x[1])), data2.iteritems())


def plot(args):
    (f, k, xl, title, name) = args
    filteredData = sorted(filter(f, processedData), key=k)

    extractKV = lambda kv: (k(kv), kv[1])
    Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'DBT_LMS', filteredData))
    Zinner = map(extractKV, filter(lambda kv: af(kv) == 'Inner', filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'Merge', filteredData))

    getX = lambda z: map(lambda kv: kv[0], z)
    getY = lambda z: map(lambda kv: kv[1], z)

    fig_size = plt.rcParams["figure.figsize"]
    fig_size[1] = 3
    fig, pl = plt.subplots()
    pl.plot(getX(Zdbt), getY(Zdbt), '-x', label='DBT LMS')
    pl.plot(getX(Zinner), getY(Zinner), '-s', label='Inner Lookup')
    pl.plot(getX(Zmerge), getY(Zmerge), '-^', label='Merge Lookup')
    plt.xlabel(xl)
    plt.ylabel("Execution time (ms)")
    plt.legend(loc=2, fontsize='small', frameon=False)
    pl.set_yscale('log', basey=2)
    pl.set_xscale('log', basex=2)
    plt.title("\n".join(wrap(title, 60)))
    fig.tight_layout()
    plt.rcParams["figure.figsize"] = fig_size
    plt.savefig(folder + "/" + name)


# Change N=R
def expPT(qi, params):
    (P, T) = params
    q = "Q" + str(qi)
    filterf = lambda kv: qf(kv) == q and (2 ** P) == pf(kv) and (2 ** T) == tf(kv) and nf(kv) == ptf(kv)
    xlabel = "Number of rows N"
    title = "Vary N for Query {} with P=2^{} T=2^{}".format(q, P, T)
    name = "ExpPT-{}-{}-{}.png".format(P, T, q)
    return filterf, nf, xlabel, title, name


# Change P
def expNRT(qi, params):
    (N, R, T) = params
    q = "Q" + str(qi)
    filterf = lambda kv: qf(kv) == q and nf(kv) == (2 ** N) and tf(kv) == (2 ** T) and ptf(kv) == (2 ** R)
    xlabel = "Number of unique price values P"
    title = "Vary P for Query {} with N=2^{} R=2^{} T=2^{}".format(q, N, R, T)
    name = "ExpNRT-{}-{}-{}-{}.png".format(N, R, T, q)
    return filterf, pf, xlabel, title, name

# Change T
def expNRP(qi, params):
    (N, R, P) = params
    q = "Q" + str(qi)
    filterf = lambda kv: qf(kv) == q and nf(kv) == (2 ** N) and pf(kv) == (2 ** P) and ptf(kv) == (2 ** R)
    xlabel = "Number of unique time values T"
    title = "Vary T for Query {} with N=2^{} R=2^{} P=2^{}".format(q, N, R, P)
    name = "ExpNRP-{}-{}-{}-{}.png".format(N, R, P, q)
    return filterf, tf, xlabel, title, name

# Change N
def expRPT(qi,params):
    (R, P, T) = params
    q = "Q" + str(qi)
    filterf = lambda kv: qf(kv) == q and pf(kv) == (2 ** P) and tf(kv) == (2 ** T) and ptf(kv) == (2 ** R)
    xlabel = "N/R"
    title = "Vary N for Query {} with R=2^{} P=2^{} T=2^{}".format(q, R, P, T)
    name = "ExpRPT-{}-{}-{}-{}.png".format( R, P, T , q)
    keyf = lambda kv: nf(kv) / ptf(kv)
    return filterf, keyf, xlabel, title, name


# Change R
def expNPT(qi, params):
    (N, P, T) = params
    q = "Q" + str(qi)
    filterf = lambda kv: qf(kv) == q and pf(kv) == (2 ** P) and tf(kv) == (2 ** T) and nf(kv) == (2 ** N)
    xlabel = "Number of unique price-time values R"
    title = "Vary R for Query {} with N=2^{} P=2^{} T=2^{}".format(q, N, P, T)
    name = "ExpNPT-{}-{}-{}-{}.png".format(N, P, T, q)

    return filterf, ptf, xlabel, title, name

paramsPT = (15, 7)
plot(expPT(1, paramsPT))
plot(expPT(2, paramsPT))
plot(expPT(3, paramsPT))

paramsNRT =(22, 17, 7)
plot(expNRT(1,  paramsNRT))
plot(expNRT(2,  paramsNRT))
plot(expNRT(3,  paramsNRT))


paramsNPT = (22, 15, 7)
plot(expNPT(1,  paramsNPT))
plot(expNPT(2,  paramsNPT))
plot(expNPT(3,  paramsNPT))

paramsNRP = (22, 17, 8)
plot(expNRP(1, paramsNRP))
plot(expNRP(2, paramsNRP))
plot(expNRP(3, paramsNRP))