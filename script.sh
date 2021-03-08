#!/usr/bin/env zsh
rm -f out.txt
set -x
execute() {
	query=$1
	total=$2
	price=$3
	time=$4
	density=$5
	numRuns=3
	sbt --error "runMain query.$query $total $price $time $density $numRuns" >> out.txt

}



for n in 12 14 16 18 20
do
	for p in  4 8 12 16
	do

		for t in 4 8 12 16
		do
			for d in 2 4 6 8 10
			do
				execute "VWAP1" $((2**n)) $((2**p)) $((2**t)) $((0.5 ** d))
				execute "VWAP2" $((2**n)) $((2**p)) $((2**t)) $((0.5 ** d))
				execute "VWAP3"  $((2**n)) $((2**p)) $((2**t)) $((0.5 ** d))
			done 
		done
	done 
	
done