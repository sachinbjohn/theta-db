#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from math import log
from textwrap import wrap


def log2(x): return log(x, 2)


def VWAP1naive(N, R, P, T):
    return N ** 2 + N
def VWAP1range(N, R, P, T):
    return N + P * log2(P) + P * log2(P) + P
def VWAP1merge(N, R, P, T):
    return N + P * log2(P) + P + P

VWAP1 = ("VWAP1", VWAP1naive, VWAP1range, VWAP1merge)


def VWAP2naive(N, R, P , T):
    return N**2 + N
def VWAP2range(N, R, P, T):
    return N + R * ((log2(R)) ** 2) + R * ((log2(R)) ** 2) + R
def VWAP2merge(N, R, P, T):
    return N + P*log2(P) + T*log2(T) + P*T + R + P*T + R

VWAP2 = ("VWAP2", VWAP2naive, VWAP2range, VWAP2merge)


def Bids2naive(N, R, P, T):
    return N**2 
def Bids2range(N, R, P, T):
    return N + P * log2(P) + N * log2(P)
def Bids2merge(N, R, P, T):
    return N + P*log2(P) + P + N + P

Bids2 = ("Bids2", Bids2naive, Bids2range, Bids2merge)



def Bids3naive(N, R, P, T):
    return N*(4*N) 
def Bids3range(N, R, P, T):
    return N + 4 * T * log2(T) + 4 * N * log2(T)
def Bids3merge(N, R, P, T):
    return N + 4 * T*log2(T) + 4 * T + 4 * (N + T)

Bids3 = ("Bids3", Bids3naive, Bids3range, Bids3merge)

def Bids4naive(N, R, P, T):
    return N*(4*N) 
def Bids4range(N, R, P, T):
    return N + 4 * T * (log2(T)**2) + 4 * N * (log2(T) ** 2)
def Bids4merge(N, R, P, T):
    return N + 8 * T*log2(T) + 4 * T*T + 4 * (N + T*T)

Bids4 = ("Bids4", Bids4naive, Bids4range, Bids4merge)



def Bids5naive(N, R, P, T):
    return N**3
def Bids5range(N, R, P, T):
    return N + 2 * (N + T)* log2(T)
def Bids5merge(N, R, P, T):
    return N + T*log2(T) + T + 2 *(N + T) 

Bids5 = ("Bids5", Bids5naive, Bids5range, Bids5merge)



def Bids6naive(N, R, P, T):
    return N**2 
def Bids6range(N, R, P, T):
    return N + R * log2(R) + R * log2(R)
def Bids6merge(N, R, P, T):
    return N + P*log2(P) + T*log2(T) + P*T + (P*T + R)

Bids6 = ("Bids6", Bids6naive, Bids6range, Bids6merge)


def Bids71naive(N, R, P, T):
    return N**3
def Bids71range(N, R, P, T):
    return N + 2 * R * log2(R) + (R + T) * log2(T) 
def Bids71merge(N, R, P, T):
    return N + P*log2(P) + T*log2(T) + P*T + (R + P*T) + T*log2(T) + T + N

Bids71 = ("Bids71", Bids71naive, Bids71range, Bids71merge)

def Bids72naive(N, R, P, T):
    return N**3
def Bids72range(N, R, P, T):
    return N + R * log2(R) + R * T * log2(R)
def Bids72merge(N, R, P, T):
    return N + P*log2(P) + T*log2(T) + P*T*T + (R + P*T*T) + T*log2(T) + T + N

Bids72 = ("Bids72", Bids72naive, Bids72range, Bids72merge)

'''
def naive(N, P, T, R, I):
    return N ** 2


def dbtQ1(N, P, T, R, I):
    return N + P * P


def dbtQ2(N, P, T, R, I):
    return N + R * (R + T)


def dbtQ3(N, P, T, R, I):
    return N + R * (R + T)


def innerQ1(N, P, T, R, I):
    return N + P * log2(P)


def innerQ2(N, P, T, R, I):
    return N + R * (log2(R) ** 2)


def innerQ3(N, P, T, R, I):
    return N + R * (log2(R) ** 3)


def mergeQ1(N, P, T, R, I):
    return N + P * log2(P)


def mergeQ2(N, P, T, R, I):
    return N + P * T + R * log2(R)


def mergeQ3(N, P, T, R, I):
    return N + P * T * T + R * log2(R)
'''


def plot(X, Ynaive, Yrange, Ymerge, xlabel, title, name):
    fig_size = plt.rcParams["figure.figsize"]
    fig_size[1] = 3
    fig, pl = plt.subplots()
    pl.plot(X, Ynaive, '-x', label='Naive')
    pl.plot(X, Yrange, '-s', label='Range')
    pl.plot(X, Ymerge, '-^', label='Merge')
    plt.xlabel(xlabel)
    plt.ylabel("Expected Execution time")
    plt.legend(loc=2, fontsize='small', frameon=False)
    pl.set_yscale('log', basey=2)
    pl.set_xscale('log', basex=2)
    plt.title("\n".join(wrap(title, 60)))
    fig.tight_layout()
    plt.rcParams["figure.figsize"] = fig_size
    plt.savefig("demo/" + name)
    plt.close()


def expPT(qa, params):
    (q, naiveA, rangeA, mergeA) = qa
    (P, T, N0, N1) = params
    N = list(map(lambda x: 2 ** x, range(N0, N1)))
    I = 1

    data = lambda A: list(map(lambda x: A(x, x, 2**P, 2**T), N))
    naiveData = data(naiveA)
    rangeData = data(rangeA)
    mergeData = data(mergeA)

    xlabel = "Number of rows N=R"
    title = "DEMO Vary N=R for Query {} with P=2^{} T=2^{}".format(q, P, T)
    name = "ExpPT-{}-{}-Q{}-demo.png".format(P, T, q)
    plot(N, naiveData, rangeData, mergeData, xlabel, title, name)

def expT(qa, params):
    (q, naiveA, rangeA, mergeA) = qa
    (Pf, T, N0, N1) = params
    N = list(map(lambda x: 2 ** x, range(N0, N1)))
    P = lambda x: x/(2**Pf)

    data = lambda A: list(map(lambda x: A(x, x, P(x), 2**T), N))
    naiveData = data(naiveA)
    rangeData = data(rangeA)
    mergeData = data(mergeA)

    xlabel = "Number of rows N=R"
    title = "DEMO Vary N=R=P*2^{} for Query {} with T=2^{}".format(Pf, q, T)
    name = "ExpT-{}-{}-Q{}-demo.png".format(Pf, T, q)
    plot(N, naiveData, rangeData, mergeData, xlabel, title, name)

def expP(qa, params):
    (q, naiveA, rangeA, mergeA) = qa
    (Tf, P, N0, N1) = params
    N = list(map(lambda x: 2 ** x, range(N0, N1)))
    T = lambda x: x/(2**Tf)

    data = lambda A: list(map(lambda x: A(x, x, 2**P, T(x)), N))
    naiveData = data(naiveA)
    rangeData = data(rangeA)
    mergeData = data(mergeA)

    xlabel = "Number of rows N=R"
    title = "DEMO Vary N=R=T*2^{} for Query {} with P=2^{}".format(Tf, q, P)
    name = "ExpP-{}-{}-Q{}-demo.png".format(Tf, P, q)
    plot(N, naiveData, rangeData, mergeData, xlabel, title, name)


def expNRT(qa, params):
    (q, naiveA, rangeA, mergeA) = qa
    ( N, R, T, P0, P1) = params
    P = list(map(lambda x: 2 ** x, range(P0, P1)))
   

    data = lambda A: list(map(lambda x: A(2**N, 2**R, x, 2**T), P))
    naiveData = data(naiveA)
    rangeData = data(rangeA)
    mergeData = data(mergeA)

    xlabel = "Number of unique price values P"
    title = "DEMO Vary P for Query {} with N=2^{} R=2^{} T=2^{}".format(q, N, R, T)
    name = "ExpNRT-{}-{}-{}-Q{}-demo.png".format(N, R, T, q)

    plot(P, naiveData, rangeData, mergeData, xlabel, title, name)


def expscaling(qa, params):
    (q, naiveA, rangeA, mergeA) = qa
    (N0, N1) = params
    N = list(map(lambda x: 2 ** x, range(N0, N1)))

    data = lambda A: list(map(lambda x: A(x,x,x,x), N))
    naiveData = data(naiveA)
    rangeData = data(rangeA)
    mergeData = data(mergeA)

    xlabel = "Scale Factor "
    title = "DEMO Vary N for Query {} ".format(q)
    name = "ExpScale-Q{}-demo.png".format(q)

    plot(N, naiveData, rangeData, mergeData, xlabel, title, name)


def expNRP(qa, params):
    (q, naiveA, rangeA, mergeA) = qa
    ( N, R, P, T0, T1) = params
    T = list(map(lambda x: 2 ** x, range(T0, T1)))

    data = lambda A: list(map(lambda x: A(2**N, 2**R, 2**P, x), T))
    naiveData = data(naiveA)
    rangeData = data(rangeA)
    mergeData = data(mergeA)

    xlabel = "Number of unique time values T"
    title = "DEMO Vary T for Query {} with N=2^{} R=2^{} P=2^{}".format(q, N, R, P)
    name = "ExpNRP-{}-{}-{}-Q{}-demo.png".format(N, R, P, q)

    plot(T, naiveData, rangeData, mergeData, xlabel, title, name)

def expRPT(qa, params):
    (q, naiveA, rangeA, mergeA) = qa
    (R, P, T, N0, N1) = params
    N = list(map(lambda x: 2 ** x, range(N0, N1)))
  
    data = lambda A: list(map(lambda x: A(x, 2**R, 2**P, 2**T), N))
    naiveData = data(naiveA)
    rangeData = data(rangeA)
    mergeData = data(mergeA)

    xlabel = "N/R"
    title = "DEMO Vary N for Query {} with R=2^{} P=2^{} T=2^{}".format(q, R, P, T)
    name = "ExpRPT-{}-{}-{}-Q{}-demo.png".format(R, P, T, q)

    NbyR = list(map(lambda x: x / (2**R), N))
    plot(NbyR, naiveData, rangeData, mergeData, xlabel, title, name)


def expNPT(qa, params):
    (q, naiveA, rangeA, mergeA) = qa
    (N, P, T, R0, R1) = params
    R = list(map(lambda x: 2 ** x, range(R0, R1)))

    data = lambda A: list(map(lambda x: A(2**N, x, 2**P, 2**T), R))
    naiveData = data(naiveA)
    rangeData = data(rangeA)
    mergeData = data(mergeA)

    xlabel = "Number of unique price-time values R"
    title = "Vary R for Query {} with N=2^{} P=2^{} T=2^{}".format(q, N, P, T)
    name = "ExpNPT-{}-{}-{}-Q{}-demo.png".format(N, P, T, q)


    plot(R, naiveData, rangeData, mergeData, xlabel, title, name)



'''
paramsPT = (15, 7, 15, 23)
expPT(VWAP1, paramsPT)
expPT(VWAP2, paramsPT)
expPT(Bids2, paramsPT)
expPT(Bids3, paramsPT)
expPT(Bids4, paramsPT)
expPT(Bids5, paramsPT)
expPT(Bids6, paramsPT)
expPT(Bids71, paramsPT)
expPT(Bids72, paramsPT)
'''

paramsNRT = (10, 10, 5, 5, 10)
expNRT(VWAP1, paramsNRT)
expNRT(VWAP2, paramsNRT)
expNRT(Bids2, paramsNRT)
expNRT(Bids3, paramsNRT)
expNRT(Bids4, paramsNRT)
expNRT(Bids5, paramsNRT)
expNRT(Bids6, paramsNRT)
expNRT(Bids71, paramsNRT)
expNRT(Bids72, paramsNRT)

'''
paramsNPT = (22, 15, 7, 15, 23)
expNPT(VWAP1, paramsNPT )
expNPT(VWAP2, paramsNPT )
expNPT(Bids2, paramsNPT )
expNPT(Bids3, paramsNPT )
expNPT(Bids4, paramsNPT )
expNPT(Bids5, paramsNPT )
expNPT(Bids6, paramsNPT )
expNPT(Bids71, paramsNPT )
expNPT(Bids72, paramsNPT )
'''

paramsNRP = (10, 10, 5, 5, 20)
expNRP(VWAP1, paramsNRP)
expNRP(VWAP2, paramsNRP)
expNRP(Bids2, paramsNRP)
expNRP(Bids3, paramsNRP)
expNRP(Bids4, paramsNRP)
expNRP(Bids5, paramsNRP)
expNRP(Bids6, paramsNRP)
expNRP(Bids71, paramsNRP)
expNRP(Bids72, paramsNRP)

paramsScaling = (5, 10)
expscaling(VWAP1, paramsScaling)
expscaling(VWAP2, paramsScaling)
expscaling(Bids2, paramsScaling)
expscaling(Bids3, paramsScaling)
expscaling(Bids4, paramsScaling)
expscaling(Bids5, paramsScaling)
expscaling(Bids5, paramsScaling)
expscaling(Bids6, paramsScaling)
expscaling(Bids71, paramsScaling)
expscaling(Bids72, paramsScaling)

paramsT = (0, 5, 5, 10)
expT(VWAP1, paramsT)
expT(VWAP2, paramsT)
expT(Bids2, paramsT)
expT(Bids3, paramsT)
expT(Bids4, paramsT)
expT(Bids5, paramsT)
expT(Bids6, paramsT)
expT(Bids71, paramsT)
expT(Bids72, paramsT)

paramsP = (0, 5, 5, 10)
expP(VWAP1, paramsP)
expP(VWAP2, paramsP)
expP(Bids2, paramsP)
expP(Bids3, paramsP)
expP(Bids4, paramsP)
expP(Bids5, paramsP)
expP(Bids6, paramsP)
expP(Bids71, paramsP)
expP(Bids72, paramsP)
