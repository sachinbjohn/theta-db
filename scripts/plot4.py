#!/usr/bin/env python 
from common import *


def plot(args):
    (f, k, xl, title, name, data, xpoint) = args
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
    pl.plot(getX(Znaive), getY(Znaive), '-o', color='red', label='R RangeJoin S')
    pl.plot(getX(Zdbt), getY(Zdbt), '-x', color='green', label='R MergeJoin S')
    pl.plot(getX(Zinner), getY(Zinner), '-s', color='chocolate', label='S RangeJoin R')
    pl.plot(getX(Zmerge), getY(Zmerge), '-^', color='lime', label='S MergeJoin R ')
    plt.axvline(x=xpoint,color='gray',linestyle='--')
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
    (nc, pc, tc, r2, expnum) = params
    filterf = lambda kv: qf(kv) == q and nf(kv) == rf(kv)+nc and tf(kv) == rf(kv)+tc and pf(kv) == rf(kv)+pc
    xlabel = "R"
    title = "N=R+{} P=R-{} T=R-{}   Query {}".format(nc,-pc,-tc,q)
    name = "Exp1-e{}-{}-{}-{}-{}.png".format(expnum, nc,pc,tc,q)
    data = getData(expnum)
    return filterf, rf, xlabel, title, name, data, r2

# Change NRP
def exp2(q, params):
    (nc, pc, T, r2) = params
    filterf = lambda kv: qf(kv) == q and nf(kv) == rf(kv)+nc and tf(kv) == T and pf(kv) == rf(kv)+pc
    xlabel = "R"
    title = "N=R+{} P=R-{} T={}   Query {}".format(nc,-pc,T,q)
    name = "Exp2-{}-{}-{}-{}.png".format(nc,pc,T,q)
    data = getData(2)
    return filterf, rf, xlabel, title, name, data, r2


params1 = (1, -5, 0, 13, 1)
plot(exp1("MB8", params1))

params2 = (1, 0, -10, 15, 1)
plot(exp1("MB9", params2))
params2 = (1, 0, -10, 15, 2)
plot(exp1("MB9", params2))

params3 = (1, -5, 5, 13)
plot(exp2("MB8", params3))
