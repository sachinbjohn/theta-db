#!/usr/bin/env python 
from common import *


def plot(args):
    (f, k, xl, title, name, data) = args
    filteredData = sorted(filter(f, data), key=k)

    extractKV = lambda kv: (k(kv), kv[1])
    Znaive = map(extractKV, filter(lambda kv: af(kv) == 'RSRange', filteredData))
    Zdbt = map(extractKV, filter(lambda kv: af(kv) == 'RSMerge', filteredData))
    Zinner = map(extractKV, filter(lambda kv: af(kv) == 'SRRange', filteredData))
    Zmerge = map(extractKV, filter(lambda kv: af(kv) == 'SRMerge', filteredData))

    getX = lambda z: map(lambda kv: kv[0], z)
    getY = lambda z: map(lambda kv: kv[1], z)

    fig_size = plt.rcParams["figure.figsize"]
    fig_size[1] = 3
    fig, pl = plt.subplots()
    pl.plot(getX(Znaive), getY(Znaive), '-o', color='b', label='R RangeJoin S')
    pl.plot(getX(Zdbt), getY(Zdbt), '-x', color='c', label='R MergeJoin S')
    pl.plot(getX(Zinner), getY(Zinner), '-s', color='r', label='S RangeJoin R')
    pl.plot(getX(Zmerge), getY(Zmerge), '-^', color='g', label='S MergeJoin R ')
    plt.xlabel(xl)
    plt.ylabel("Execution time (ms)")
    plt.legend(loc=2, fontsize='small', frameon=False)
    pl.set_yscale('log', basey=2)
    #pl.set_xscale('log', basex=2)
    plt.title("\n".join(wrap(title, 60)))
    fig.tight_layout()
    plt.rcParams["figure.figsize"] = fig_size
    plt.savefig(folder + "/" + name)


# Change All
def exp1(q, params):
    (nc, pc, tc) = params
    filterf = lambda kv: qf(kv) == q and nf(kv) == rf(kv)+nc and tf(kv) == rf(kv)+pc and pf(kv) == rf(kv)+tc
    xlabel = "Scale Factor"
    title = "Vary ScaleFactor with nc={} pc={} tc={} for Query {}".format(nc,pc,tc,q)
    name = "Exp1-{}-{}-{}-{}.png".format(nc,pc,tc,q)
    data = getData(1)
    return filterf, nf, xlabel, title, name, data

params1 = (2, 0, 0)
plot(exp1("MB8", params1))
plot(exp1("MB9", params1))


