#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
from math import sqrt

# input = "test.csv"
folder = "output/" + sys.argv[1]


qf = lambda kv: kv[0][0]
af = lambda kv: kv[0][1]
nf = lambda kv: kv[0][2]
rf = lambda kv: kv[0][3]
pf = lambda kv: kv[0][4]
tf = lambda kv: kv[0][5]

def keyFn(x):
    return x['Query'], x['Algo'], int(x['Total']), int(x['PriceTime']), int(x['Price']), int(x['Time']) 


def valueFn(x):
    return int(x['ExTime'])


def mean(x):
    return sum(x) / len(x)


def getData(expnum):
    input = folder + "/expt" + str(expnum) + "/output.csv"
    print("Input = " + input)
    data = list(csv.DictReader(open(input, 'r')))
    data2 = defaultdict(lambda: [])

    for x in data:
        data2[keyFn(x)].append(valueFn(x))
    return map(lambda x: (x[0], mean(x[1])), data2.iteritems())



def plot(args):
    (f, k, xl, title, name, data) = args
    filteredData = sorted(filter(f, data), key=k)

    extractKV = lambda kv: (k(kv), kv[1])
    Znaive = map(extractKV, filter(lambda kv: af(kv) == 'Naive', filteredData))
    Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'Smart', filteredData))
    Zinner = map(extractKV, filter(lambda kv: af(kv) == 'Range', filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'Merge', filteredData))

    getX = lambda z: map(lambda kv: kv[0], z)
    getY = lambda z: map(lambda kv: kv[1], z)

    fig_size = plt.rcParams["figure.figsize"]
    fig_size[1] = 3
    fig, pl = plt.subplots()
    pl.plot(getX(Znaive), getY(Znaive), '-o', label='Naive')
    pl.plot(getX(Zdbt), getY(Zdbt), '-x', label='Smart')
    pl.plot(getX(Zinner), getY(Zinner), '-s', label='Range')
    pl.plot(getX(Zmerge), getY(Zmerge), '-^', label='Merge')
    plt.xlabel(xl)
    plt.ylabel("Execution time (ms)")
    plt.legend(loc=2, fontsize='small', frameon=False)
    pl.set_yscale('log', basey=2)
    #pl.set_xscale('log', basex=2)
    plt.title("\n".join(wrap(title, 60)))
    fig.tight_layout()
    plt.rcParams["figure.figsize"] = fig_size
    plt.savefig(folder + "/" + name)


# Change N R and P
def exp2(q, params):
    (nc, pc, T) = params
    filterf = lambda kv: qf(kv) == q and nf(kv) == rf(kv)+nc and pf(kv) == rf(kv)+pc and tf(kv)==T 
    xlabel = "R"
    title = "Vary N R and P  with nc={}, pc={}, T=2^{} for Query {}".format( nc, pc, T, q)
    name = "Exp2-{}-{}-{}-{}.png".format(nc, pc, T, q)
    data = getData(2)
    return filterf, nf, xlabel, title, name, data

# Change T
def exp3(q, params):
    (N, R, P) = params
    filterf = lambda kv: qf(kv) == q and nf(kv) == N and pf(kv) == P and rf(kv) == R
    xlabel = "Number of unique time values T"
    title = "Vary T with N=2^{} R=2^{} P=2^{} for Query {} ".format(N, R, P, q)
    name = "Exp3-{}-{}-{}-{}.png".format(N, R, P, q)
    data = getData(3)
    return filterf, tf, xlabel, title, name, data


# Change All
def exp1(q, params):
    (nc, pc, tc) = params
    filterf = lambda kv: qf(kv) == q and nf(kv) == rf(kv)+nc and tf(kv) == rf(kv)+pc and pf(kv) == rf(kv)+tc
    xlabel = "Scale Factor"
    title = "Vary ScaleFactor with nc={} pc={} tc={} for Query {}".format(nc,pc,tc,q)
    name = "Exp1-{}-{}-{}-{}.png".format(nc,pc,tc,q)
    data = getData(1)
    return filterf, nf, xlabel, title, name, data

params1 = (1, -5, -5)
plot(exp1("MB2", params1))
plot(exp1("MB3", params1))
plot(exp1("MB4", params1))
plot(exp1("MB5", params1))
plot(exp1("MB7", params1))

params2 = (1, -5, 10)
plot(exp2("MB2", params2))
plot(exp2("MB3", params2))
plot(exp2("MB4", params2))
plot(exp2("MB5", params2))
plot(exp2("MB7", params2))

'''
params3 = (21, 20, 15)
plot(exp3("MB2", params2))
plot(exp3("MB3", params2))
plot(exp3("MB4", params2))
plot(exp3("MB5", params2))
plot(exp3("MB7", params2))
'''

