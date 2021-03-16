#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from math import log
from textwrap import wrap


def log2(x): return log(x, 2)


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


def plot(X, Ydbt, Yinner, Ymerge, xlabel, title, name):
    fig_size = plt.rcParams["figure.figsize"]
    fig_size[1] = 3
    fig, pl = plt.subplots()
    pl.plot(X, Ydbt, '-x', label='DBT LMS')
    pl.plot(X, Yinner, '-s', label='Inner Lookup')
    pl.plot(X, Ymerge, '-^', label='Merge Lookup')
    plt.xlabel(xlabel)
    plt.ylabel("Expected Execution time")
    plt.legend(loc=2, fontsize='small', frameon=False)
    pl.set_yscale('log', basey=2)
    pl.set_xscale('log', basex=2)
    plt.title("\n".join(wrap(title, 60)))
    fig.tight_layout()
    plt.rcParams["figure.figsize"] = fig_size
    plt.savefig("demo/" + name)


def expPT(q, dbtA, innerA, mergeA, P, T, N0, N1):
    N = list(map(lambda x: 2 ** x, range(N0, N1)))
    I = 1

    data = lambda A: list(map(lambda x: A(x, 2**P, 2**T, x, I), N))
    dbtData = data(dbtA)
    innerData = data(innerA)
    mergeData = data(mergeA)

    xlabel = "Number of rows N"
    title = "DEMO Vary N for Query {} with P=2^{} T=2^{}".format(q, P, T)
    name = "ExpPT-{}-{}-Q{}-demo.png".format(P, T, q)
    plot(N, dbtData, innerData, mergeData, xlabel, title, name)


def expNRT(q, dbtA, innerA, mergeA, N, R, T, P0, P1):
    P = list(map(lambda x: 2 ** x, range(P0, P1)))
    I = 1

    data = lambda A: list(map(lambda x: A(2**N, x, 2**T, 2**R, I), P))
    dbtData = data(dbtA)
    innerData = data(innerA)
    mergeData = data(mergeA)

    xlabel = "Number of unique price values P"
    title = "DEMO Vary P for Query {} with N=2^{} R=2^{} T=2^{}".format(q, N, R, T)
    name = "ExpNRT-{}-{}-{}-Q{}-demo.png".format(N, R, T, q)

    plot(P, dbtData, innerData, mergeData, xlabel, title, name)


def expRPT(q, dbtA, innerA, mergeA, R, P, T, N0, N1):
    N = list(map(lambda x: 2 ** x, range(N0, N1)))
    I = 1

    data = lambda A: list(map(lambda x: A(x, 2**P, 2**T, 2**R, I), N))
    dbtData = data(dbtA)
    innerData = data(innerA)
    mergeData = data(mergeA)

    xlabel = "N/R"
    title = "DEMO Vary N for Query {} with R=2^{} P=2^{} T=2^{}".format(q, R, P, T)
    name = "ExpRPT-{}-{}-{}-Q{}-demo.png".format(R, P, T, q)

    NbyR = list(map(lambda x: x / (2**R), N))
    plot(NbyR, dbtData, innerData, mergeData, xlabel, title, name)


def expNPT(q, dbtA, innerA, mergeA, N, P, T, R0, R1):
    R = list(map(lambda x: 2 ** x, range(R0, R1)))
    I = 1

    data = lambda A: list(map(lambda x: A(2**N, 2**P, 2**T, x, I), R))
    dbtData = data(dbtA)
    innerData = data(innerA)
    mergeData = data(mergeA)

    xlabel = "N/R"
    title = "Vary R for Query {} with N=2^{} P=2^{} T=2^{}".format(q, N, P, T)
    name = "ExpNPT-{}-{}-{}-Q{}-demo.png".format(N, P, T, q)

    NbyR = list(map(lambda x: 2**N / x, R))
    plot(NbyR, dbtData, innerData, mergeData, xlabel, title, name)


expPT(1, dbtQ1, innerQ1, mergeQ1, 10, 10, 10, 20)
expPT(2, dbtQ2, innerQ2, mergeQ2, 10, 10, 10, 20)
expPT(3, dbtQ3, innerQ3, mergeQ3, 10, 10, 10, 20)

expNRT(1, dbtQ1, innerQ1, mergeQ1, 20, 17, 7, 10, 17)
expNRT(2, dbtQ2, innerQ2, mergeQ2, 20, 17, 7, 10, 17)
expNRT(3, dbtQ3, innerQ3, mergeQ3, 20, 17, 7, 10, 17)


expNPT(1, dbtQ1, innerQ1, mergeQ1, 20, 10, 10, 10, 18)
expNPT(2, dbtQ2, innerQ2, mergeQ2, 20, 10, 10, 10, 18)
expNPT(3, dbtQ3, innerQ3, mergeQ3, 20, 10, 10, 10, 18)
