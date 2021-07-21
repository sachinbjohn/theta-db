#!/usr/bin/env python 
from common import *
def plot(lang,query):
    title="Expt {} {}".format(query,lang)
    name="Expt-{}-{}.png".format(query,lang)
    xl="log(ScaleFactor)"
    k=nf
    filterfn = lambda kv : qf(kv) == query
    filteredData = sorted(filter(filterfn, processedData), key=k)

    extractKV = lambda kv: (k(kv), kv[1])
    
    Znaive = map(extractKV, filter(lambda kv: af(kv) == 'Naive' and lf(kv) == lang, filteredData))
    Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'DBT' and lf(kv) == lang, filteredData))
    Zrange = map(extractKV, filter(lambda kv: af(kv) == 'Range' and lf(kv) == lang, filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'Merge' and lf(kv) == lang, filteredData))

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



def plotAll(query):
    title="Expt {} All".format(query)
    name="Expt-{}-All.png".format(query)
    xl="log(ScaleFactor)"
    k=pf
    filterf = lambda kv: pf(kv) % 3 == 0 and qf(kv) == query
    filteredData = sorted(filter(filterf, processedData), key=k)

    extractKV = lambda kv: (k(kv), kv[1])
    
    
    getX = lambda z: map(lambda kv: kv[0], z)
    getY = lambda z: map(lambda kv: kv[1], z)

    lang='CPP'
    Znaive = map(extractKV, filter(lambda kv: af(kv) == 'Naive' and lf(kv) == lang, filteredData))
    Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'DBT' and lf(kv) == lang, filteredData))
    Zrange = map(extractKV, filter(lambda kv: af(kv) == 'Range' and lf(kv) == lang, filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'Merge' and lf(kv) == lang, filteredData))
    
    print(Zmerge)

    labels = sorted(set(map(k, filteredData)))
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
    
    Znaive = map(extractKV, filter(lambda kv: af(kv) == 'Naive' and lf(kv) == lang, filteredData))
    Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'DBT' and lf(kv) == lang, filteredData))
    Zrange = map(extractKV, filter(lambda kv: af(kv) == 'Range' and lf(kv) == lang, filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'Merge' and lf(kv) == lang, filteredData))
    
    
    
    pl.bar(X + (c-1.5) * w, getY(Zrange) + [0] * (N - len(Zrange))   , w, bottom = 1, color='r', label='Range {}'.format(lang), hatch=h)
    pl.bar(X + (c-0.5) * w, getY(Zmerge) + [0] * (N - len(Zmerge)), w, bottom = 1, color='g',label='Merge {}'.format(lang),hatch=h)
    pl.bar(X + (c+0.5) * w, getY(Znaive)+ [0] * (N - len(Znaive)), w, bottom = 1, color='b', label='Naive {}'.format(lang),hatch=h)
    pl.bar(X + (c+1.5) * w, getY(Zdbt) + [0] * (N - len(Zdbt)), w, bottom = 1, color='c', label='DBT {}'.format(lang),hatch=h)
    
    
    
    c=4
    h='*'
    lang='SQL'
    
    Znaive = map(extractKV, filter(lambda kv: af(kv) == 'Naive' and lf(kv) == lang, filteredData))
    Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'DBT' and lf(kv) == lang, filteredData))
    Zrange = map(extractKV, filter(lambda kv: af(kv) == 'Range' and lf(kv) == lang, filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'Merge' and lf(kv) == lang, filteredData))
    
    
    pl.bar(X + (c-1.5) * w, getY(Zrange) + [0] * (N - len(Zrange))   , w, bottom = 1, color='r', label='Range {}'.format(lang), hatch=h)
    pl.bar(X + (c-0.5) * w, getY(Zmerge) + [0] * (N - len(Zmerge)), w, bottom = 1, color='g',label='Merge {}'.format(lang),hatch=h)
    pl.bar(X + (c+0.5) * w, getY(Znaive)+ [0] * (N - len(Znaive)), w, bottom = 1, color='b', label='Naive {}'.format(lang),hatch=h)
    #pl.bar(X + (c+1.5) * w, getY(Zdbt) + [0] * (N - len(Zdbt)), w, bottom = 1, color='c', label='DBT {}'.format(lang),hatch=h)
    
    
    
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

#plotAll('Q1')
#plotAll('Q2')
plot('CPP','Q1')
plot('CPP','Bids2')
plot('CPP','Bids3')
plot('CPP','Bids4')
plot('CPP','Bids5')
plot('CPP','Bids6')
