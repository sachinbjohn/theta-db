#!/usr/bin/env python 
from common import *

def plotAll(processedData):
    title="Execution time for different queries"
    name="ExptJC-All.png"
    xl="Queries"
    k=qf
    filterf = lambda kv: True
    filteredData = sorted(filter(filterf, processedData), key=k)

    extractKV = lambda kv: (k(kv), kv[1])
    
    
    def getYforX(z, x):
        Yall = defaultdict(lambda : 0)
        for kv in z:
            Yall[kv[0]] = kv[1]
        Y = []
        for xi in x:
            Y.append(Yall[xi])
        return Y

    getY = lambda z: map(lambda kv: kv[1], z)

    lang='SQL'
    Znaive = map(extractKV, filter(lambda kv: af(kv) == 'Naive' and lf(kv) == lang, filteredData))
    Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'Smart' and lf(kv) == lang, filteredData))
    Zrange = map(extractKV, filter(lambda kv: af(kv) == 'Range' and lf(kv) == lang, filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'Merge' and lf(kv) == lang, filteredData))
    

    labels = sorted(set(map(k, filteredData)))
    N=len(labels)
    X = numpy.arange(N)
    
    fig_size = plt.rcParams["figure.figsize"]
    fig_size[1] = 4
    fig, pl = plt.subplots()
   
    w=1.0/6
    
    c=0
    h=' '
    pl.bar(X + (c-2) * w, getYforX(Zrange, labels), w, bottom = 1, color='r', label='Range', hatch=' ')
    pl.bar(X + (c-1) * w, getYforX(Zmerge, labels), w, bottom = 1, color='g',label='Merge',hatch='//')
    pl.bar(X + (c+0) * w, getYforX(Znaive, labels), w, bottom = 1, color='b', label='Naive',hatch='*')
    pl.bar(X + (c+1) * w, getYforX(Zdbt,   labels), w, bottom = 1, color='c', label='Magic',hatch='o')
    
    
    plt.xlabel(xl)
    plt.ylabel("Execution time (ms)")
    
    box = pl.get_position()
    pl.set_position([box.x0, box.y0, box.width * 0.8, box.height])
    pl.legend(loc='center left', fontsize='small', bbox_to_anchor=(1, 0.5))
    
    pl.set_xticks(X)
    pl.set_xticklabels(labels)
    pl.set_yscale('log', basey=2)
    pl.set_xlim([-2*w, N+w])
    plt.title("\n".join(wrap(title, 60)))
    #fig.tight_layout()
    plt.rcParams["figure.figsize"] = fig_size
    plt.savefig(folder + "/" + name)

plotAll(getData(0))
