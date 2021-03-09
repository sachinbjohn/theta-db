#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap 
input = "output2021.03.08.21.19.31.csv"

data = list(csv.DictReader(open(input, 'r')))


def keyFn(x):
	return x['Query'],int(x['Total']),int(x['Price']),int(x['Time']),int(x['PriceTime'])

def naiveFn(x):
	return (int(x['Naive1'])+int(x['Naive2'])+int(x['Naive3']))/3.0

def DBTFn(x):
	return (int(x['DBT1'])+int(x['DBT2'])+int(x['DBT3']))/3.0

def Algo1Fn(x):
	return (int(x['Algo11'])+int(x['Algo12'])+int(x['Algo13']))/3.0

def Algo2Fn(x):
	return (int(x['Algo21'])+int(x['Algo22'])+int(x['Algo23']))/3.0

processedData = {}

for x in data:
	processedData[keyFn(x)] = (naiveFn(x), DBTFn(x), Algo1Fn(x), Algo2Fn(x))



def plot(X, Y1, Y2, Y3, Y4, xl, title, name)
	fig_size = plt.rcParams["figure.figsize"]
	# Prints: [8.0, 6.0]
	 # fig_size[0] = 20
	fig_size[1] = 3
	fig,pl=plt.subplots()
	pl.plot(X, Y1, '-x', label='Naive')
	pl.plot(X, Y3, '-s', label='Inner Lookup')
	pl.plot(X, Y4, '-^', label='Merge Lookup')
	pl.plot(X, Y2, '-o', label = 'DBToaster')
	plt.xlabel(xl)
	plt.ylabel("Execution time (ms)")
	plt.legend()
	pl.set_yscale('log')
	pl.set_xscale('log')
	plt.title("\n".join(wrap(title,60)))
	fig.tight_layout()
	plt.rcParams["figure.figsize"] = fig_size
	plt.savefig(name)

def plotTotal(q, p, t, pt):
	filteredData = filter(lambda kv: kv[0][0] == q and kv[0][2] == p and kv[0][3] == t and kv[0][4] == pt, processedData.iteritems())
	X = map(lambda kv : kv[0][1], filteredData)
	Y1 = map(lambda kv : kv[1][0], filteredData)
	Y2 = map(lambda kv : kv[1][1], filteredData)
	Y3 = map(lambda kv : kv[1][2], filteredData)
	Y4 = map(lambda kv : kv[1][3], filteredData)
	xl = "Total number of rows"
	title = "Query {},  Price = {}, Time = {}, PriceTime = {}".format(q,p,t,pt)
	name = "Total {}.{}.{}.{}.png".format(q,p,t,pt)
	plot(X,Y1,Y2,Y3,Y4, xl, title, name)

def plotPrice(q, n, t, pt):
	filteredData = filter(lambda kv: kv[0][0] == q and kv[0][1] == n and kv[0][3] == t and kv[0][4] == pt, processedData.iteritems())
	X = map(lambda kv : kv[0][2], filteredData)
	Y1 = map(lambda kv : kv[1][0], filteredData)
	Y2 = map(lambda kv : kv[1][1], filteredData)
	Y3 = map(lambda kv : kv[1][2], filteredData)
	Y4 = map(lambda kv : kv[1][3], filteredData)
	xl = "Number of distinct price values"
	title = "Query {},  Total = {}, Time = {}, PriceTime = {}".format(q,n,t,pt)
	name = "Price {}.{}.{}.{}.png".format(q,n,t,pt)
	plot(X,Y1,Y2,Y3,Y4, xl, title, name)

def plotTime(q, n, p, pt):
	filteredData = filter(lambda kv: kv[0][0] == q and kv[0][1] == n and kv[0][2] == p and kv[0][4] == pt, processedData.iteritems())
	X = map(lambda kv : kv[0][3], filteredData)
	Y1 = map(lambda kv : kv[1][0], filteredData)
	Y2 = map(lambda kv : kv[1][1], filteredData)
	Y3 = map(lambda kv : kv[1][2], filteredData)
	Y4 = map(lambda kv : kv[1][3], filteredData)
	xl = "Number of distinct time values"
	title = "Query {},  Total = {}, Price = {}, PriceTime = {}".format(q,n,p,pt)
	name = "Time {}.{}.{}.{}.png".format(q,n,p,pt)
	plot(X,Y1,Y2,Y3,Y4, xl, title, name)

def plotPriceTime(q, n, p, t):
	filteredData = filter(lambda kv: kv[0][0] == q and kv[0][1] == n and kv[0][2] == p and kv[0][3] == t, processedData.iteritems())
	X = map(lambda kv : kv[0][4], filteredData)
	Y1 = map(lambda kv : kv[1][0], filteredData)
	Y2 = map(lambda kv : kv[1][1], filteredData)
	Y3 = map(lambda kv : kv[1][2], filteredData)
	Y4 = map(lambda kv : kv[1][3], filteredData)
	xl ="Number of distinct time values"
	title = "Query {},  Total = {}, Price = {}, PriceTime = {}".format(q,n,p,t)
	name = ("PriceTime {}.{}.{}.{}.png".format(q,n,p,t))
	plot(X,Y1,Y2,Y3,Y4, xl, title, name)

