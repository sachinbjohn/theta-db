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
lf = lambda kv: kv[0][6]

def keyFn(x):
    return x['Query'], x['Algo'], int(x['Total']), int(x['PriceTime']), int(x['Price']), int(x['Time']), x['Lang']


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

