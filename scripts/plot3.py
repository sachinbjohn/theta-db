#!/usr/bin/env python 
from common import *

def plotAll(processedData):
    title="ExptJC All"
    name="ExptJC-All.png"
    xl="ScaleFactor"
    k=qf
    filterf = lambda kv: nf(kv) == 15
    filteredData = sorted(filter(filterf, processedData), key=k)

    extractKV = lambda kv: (k(kv), kv[1])
    
    
    getX = lambda z: map(lambda kv: kv[0], z)
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
    pl.bar(X + (c-1.5) * w, getY(Zrange) + [0] * (N - len(Zrange))   , w, bottom = 1, color='r', label='Range {}'.format(lang), hatch=' ')
    pl.bar(X + (c-0.5) * w, getY(Zmerge) + [0] * (N - len(Zmerge)), w, bottom = 1, color='g',label='Merge {}'.format(lang),hatch='//')
    pl.bar(X + (c+0.5) * w, getY(Znaive)+ [0] * (N - len(Znaive)), w, bottom = 1, color='b', label='Naive {}'.format(lang),hatch='*')
    pl.bar(X + (c+1.5) * w, getY(Zdbt) + [0] * (N - len(Zdbt)), w, bottom = 1, color='c', label='Smart {}'.format(lang),hatch='o')
    
    
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

plotAll(getData(1))
