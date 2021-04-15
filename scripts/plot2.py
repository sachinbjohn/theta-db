#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap
from math import sqrt
import numpy
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
    return float(x['ExTime'])


def mean(x):
    return sum(x) / len(x)


data2 = defaultdict(lambda: [])

for x in data:
    data2[keyFn(x)].append(valueFn(x))

processedData = map(lambda x: (x[0], mean(x[1])), data2.iteritems())


def plot(lang):
    title="Expt {}".format(lang)
    name="Expt-{}.png".format(lang)
    xl="log(ScaleFactor)"
    k=pf
    filteredData = sorted(processedData, key=k)

    extractKV = lambda kv: (k(kv), kv[1])
    Znaive = map(extractKV, filter(lambda kv: af(kv) == 'Naive {}'.format(lang), filteredData))
    Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'DBT {}'.format(lang), filteredData))
    Zrange = map(extractKV, filter(lambda kv: af(kv) == 'Range {}'.format(lang), filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'Merge {}'.format(lang), filteredData))
    


    getX = lambda z: map(lambda kv: kv[0], z)
    getY = lambda z: map(lambda kv: kv[1], z)

    fig_size = plt.rcParams["figure.figsize"]
    fig_size[1] = 3
    fig, pl = plt.subplots()
    
        
    pl.plot(getX(Zmerge), getY(Zmerge), '-', label='Merge {}'.format(lang), color='g')
    pl.plot(getX(Zrange), getY(Zrange), '-', label='Range {}'.format(lang), color='r')
    pl.plot(getX(Znaive), getY(Znaive), '-', label='Naive {}'.format(lang), color='b')
    pl.plot(getX(Zdbt), getY(Zdbt), '-', label='DBT {}'.format(lang), color='c')
    
    plt.xlabel(xl)
    plt.ylabel("Execution time (ms)")
    plt.legend(loc=2, fontsize='small', frameon=False)
    pl.set_yscale('log', basey=2)
    plt.title("\n".join(wrap(title, 60)))
    fig.tight_layout()
    plt.rcParams["figure.figsize"] = fig_size
    plt.savefig(folder + "/" + name)


plot('Scala')
plot('CPP')
plot('SQL')

def plotAll():
    title="Expt All"
    name="Expt-All.png"
    xl="log(ScaleFactor)"
    k=pf
    filterf = lambda kv: pf(kv) % 3 == 0
    filteredData = sorted(filter(filterf, processedData), key=k)

    extractKV = lambda kv: (k(kv), kv[1])
    
    
    getX = lambda z: map(lambda kv: kv[0], z)
    getY = lambda z: map(lambda kv: kv[1], z)

    lang='CPP'
    Znaive = map(extractKV, filter(lambda kv: af(kv) == 'Naive {}'.format(lang), filteredData))
    Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'DBT {}'.format(lang), filteredData))
    Zrange = map(extractKV, filter(lambda kv: af(kv) == 'Range {}'.format(lang), filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'Merge {}'.format(lang), filteredData))
    
    

    labels = getX(Zmerge)
    N=len(labels)
    X = numpy.arange(N)
    
    fig_size = plt.rcParams["figure.figsize"]
    fig_size[1] = 4
    fig, pl = plt.subplots()
    
    w=1.0/14
    
    c=-4
    h=' '
    pl.bar(X + (c-1.5) * w, getY(Zrange) + [0] * (N - len(Zrange))   , w, bottom = 1, color='r', label='Range {}'.format(lang), hatch=h)
    pl.bar(X + (c-0.5) * w, getY(Zmerge) + [0] * (N - len(Zmerge)), w, bottom = 1, color='g',label='Merge {}'.format(lang),hatch=h)
    pl.bar(X + (c+0.5) * w, getY(Znaive)+ [0] * (N - len(Znaive)), w, bottom = 1, color='b', label='Naive {}'.format(lang),hatch=h)
    pl.bar(X + (c+1.5) * w, getY(Zdbt) + [0] * (N - len(Zdbt)), w, bottom = 1, color='c', label='DBT {}'.format(lang),hatch=h)
    
    
    c=0
    h='//'
    lang='Scala'
    
    Znaive = map(extractKV, filter(lambda kv: af(kv) == 'Naive {}'.format(lang), filteredData))
    Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'DBT {}'.format(lang), filteredData))
    Zrange = map(extractKV, filter(lambda kv: af(kv) == 'Range {}'.format(lang), filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'Merge {}'.format(lang), filteredData))
    
    pl.bar(X + (c-1.5) * w, getY(Zrange) + [0] * (N - len(Zrange))   , w, bottom = 1, color='r', label='Range {}'.format(lang), hatch=h)
    pl.bar(X + (c-0.5) * w, getY(Zmerge) + [0] * (N - len(Zmerge)), w, bottom = 1, color='g',label='Merge {}'.format(lang),hatch=h)
    pl.bar(X + (c+0.5) * w, getY(Znaive)+ [0] * (N - len(Znaive)), w, bottom = 1, color='b', label='Naive {}'.format(lang),hatch=h)
    pl.bar(X + (c+1.5) * w, getY(Zdbt) + [0] * (N - len(Zdbt)), w, bottom = 1, color='c', label='DBT {}'.format(lang),hatch=h)
    
    
    c=4
    h='\\'
    lang='SQL'
    
    Znaive = map(extractKV, filter(lambda kv: af(kv) == 'Naive {}'.format(lang), filteredData))
   # Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'DBT {}'.format(lang), filteredData))
    Zrange = map(extractKV, filter(lambda kv: af(kv) == 'Range {}'.format(lang), filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'Merge {}'.format(lang), filteredData))
    
    pl.bar(X + (c-1.5) * w, getY(Zrange) + [0] * (N - len(Zrange))   , w, bottom = 1, color='r', label='Range {}'.format(lang), hatch=h)
    pl.bar(X + (c-0.5) * w, getY(Zmerge) + [0] * (N - len(Zmerge)), w, bottom = 1, color='g',label='Merge {}'.format(lang),hatch=h)
    pl.bar(X + (c+1.5) * w, getY(Znaive)+ [0] * (N - len(Znaive)), w, bottom = 1, color='b', label='Naive {}'.format(lang),hatch=h)
   # pl.bar(X + (c+2.5) * w, getY(Zdbt) + [0] * (N - len(Zdbt)), w, bottom = 1, color='c', label='DBT {}'.format(lang),hatch=h)
    
    
    
    plt.xlabel(xl)
    plt.ylabel("Execution time (ms)")
    
    box = pl.get_position()
    pl.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    pl.legend(loc='center left', fontsize='small', bbox_to_anchor=(1, 0.5))
    
    pl.set_xticks(X)
    pl.set_xticklabels(labels)
    pl.set_yscale('log', basey=2)
    plt.title("\n".join(wrap(title, 60)))
    #fig.tight_layout()
    plt.rcParams["figure.figsize"] = fig_size
    plt.savefig(folder + "/" + name)

plotAll()
