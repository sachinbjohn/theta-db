#!/usr/bin/env python 
import csv
import sys
import matplotlib.pyplot as plt
from collections import defaultdict
from textwrap import wrap 
#input = "output2021.03.08.21.19.31.csv"
input = sys.argv[1]
data = list(csv.DictReader(open(input, 'r')))

print("Input = " + input)

def keyFn(x):
	return x['Query'],int(x['Total']),int(x['Price']),int(x['Time']),int(x['PriceTime']),int(x['Algo'])

def valueFn(x):
	return (int(x['Ex1'])+int(x['Ex2'])+int(x['Ex3']))/3.0


processedData = {}

for x in data:
	processedData[keyFn(x)] = valueFn(x)
	
def plot(X, Y1, Y2, Y3, Y4, xl, title, name):
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

def plotTotal( f, xl, title, name):
	filteredData = sorted(map(lambda kv: filter(f , processedData.iteritems())), key = lambda kv: kv[0][1])
	X = map(lambda kv : kv[0][1], filteredData)
	Y1 = map(lambda kv : kv[1][0], filteredData)
	Y2 = map(lambda kv : kv[1][1], filteredData)
	Y3 = map(lambda kv : kv[1][2], filteredData)
	Y4 = map(lambda kv : kv[1][3], filteredData)
	plot(X,Y1,Y2,Y3,Y4, xl, title, name)

def plotPrice(f, xl, title, name):
	filteredData = sorted(filter(f , processedData.iteritems()), key = lambda kv: kv[0][2])
	X = map(lambda kv : kv[0][2], filteredData)
	Y1 = map(lambda kv : kv[1][0], filteredData)
	Y2 = map(lambda kv : kv[1][1], filteredData)
	Y3 = map(lambda kv : kv[1][2], filteredData)
	Y4 = map(lambda kv : kv[1][3], filteredData)
	plot(X,Y1,Y2,Y3,Y4, xl, title, name)

def plotTime(f, xl, title, name):
	filteredData = sorted(filter(f , processedData.iteritems()), key = lambda kv: kv[0][3])
	X = map(lambda kv : kv[0][3], filteredData)
	Y1 = map(lambda kv : kv[1][0], filteredData)
	Y2 = map(lambda kv : kv[1][1], filteredData)
	Y3 = map(lambda kv : kv[1][2], filteredData)
	Y4 = map(lambda kv : kv[1][3], filteredData)
	plot(X,Y1,Y2,Y3,Y4, xl, title, name)

def plotPriceTime(f, xl, title, name):
	filteredData = sorted(filter(f, processedData.iteritems()), key = lambda kv: kv[0][4])
	X = map(lambda kv : kv[0][4], filteredData)
	Y1 = map(lambda kv : kv[1][0], filteredData)
	Y2 = map(lambda kv : kv[1][1], filteredData)
	Y3 = map(lambda kv : kv[1][2], filteredData)
	Y4 = map(lambda kv : kv[1][3], filteredData)
	plot(X,Y1,Y2,Y3,Y4, xl, title, name)


nf = lambda kv : kv[0][1]
qf = lambda kv : kv[0][0]
pf = lambda kv : kv[0][2]
tf = lambda kv : kv[0][3]
ptf = lambda kv : kv[0][4]

f1 = lambda q: lambda kv : qf(kv) == q and nf(kv) == pf(kv) * 64 and nf(kv) == tf(kv) * 64 and nf(kv) == ptf(kv) * 16
x1 = "N"
t1 = "Vary N"
n1 = lambda q: "Total {}.png".format(q)
plotTotal(f1('Q1'), x1, t1, n1('Q1'))
plotTotal(f1('Q2'), x1, t1, n1('Q2'))
plotTotal(f1('Q2'), x1, t1, n1('Q3'))

f2 = lambda q: lambda kv : qf(kv) == q and nf(kv) == 2**14 and tf(kv) ==  2**5 and ptf(kv)  == 2**10
x2 = "P"
t2 = "Vary P"
n2 = lambda q: "Price {}.png".format(q)
plotPrice(f2('Q1'), x2, t2, n2('Q1'))
plotPrice(f2('Q2'), x2, t2, n2('Q2'))
plotPrice(f2('Q3'), x2, t2, n2('Q3'))

f3 = lambda q: lambda kv : qf(kv) == q and nf(kv) == 2**14 and pf(kv) ==  2**5 and ptf(kv)  == 2**10
x3 = "T"
t3 = "Vary T"
n3 = lambda q: "Time {}.png".format(q)
plotTime(f3('Q1'), x3, t3, n3('Q1'))
plotTime(f3('Q2'), x3, t3, n3('Q2'))
plotTime(f3('Q3'), x3, t3, n3('Q3'))

f4 = lambda q: lambda kv : qf(kv) == q and nf(kv) == ptf(kv) * 4 and pf(kv) ==  2**8  and tf(kv) == 2**8
x4 = "PT"
t4 = "Vary PT"
n4 = lambda q: "PriceTime {}.png".format(q)
plotPriceTime(f4('Q1'), x4, t4, n4('Q1'))
plotPriceTime(f4('Q2'), x4, t4, n4('Q2'))
plotPriceTime(f4('Q3'), x4, t4, n4('Q3'))