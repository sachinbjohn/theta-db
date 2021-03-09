#!/usr/bin/env zsh

current=$(date "+%Y.%m.%d.%H.%M.%S")
out="output${current}.csv"

echo "Query,Total,Price,Time,PriceTime,Naive1,DBT1,Algo11,Algo21,Naive2,DBT2,Algo12,Algo22,Naive3,DBT3,Algo13,Algo23"
exp_num=0
execute() {
	exp_num=$((exp_num + 1))
	query=$1
	total=$2
	price=$3
	time=$4
	pricetime=$5
	pt=$((price * time))
	numRuns=3
	
	if [ "$pricetime" -lt "$pt" ] 
	then
		echo "Running $exp_num" $1 $2 $3 $4 $5 "at " $(date "+%Y.%m.%d.%H.%M.%S")
		sbt --error "runMain query.$query $total $price $time $pricetime $numRuns" >> $out
	else
		echo "Skip $exp_num" $1 $2 $3 $4 $5
	fi

}



for allparams in `cat allparams.txt`
do
    
    n=`echo $allparams | cut -d: -f1`
    p=`echo $allparams | cut -d: -f2`
    t=`echo $allparams | cut -d: -f3`
    d=`echo $allparams | cut -d: -f4`

	execute "VWAP1" $((2**n)) $((2**p)) $((2**t)) $((2 ** d))
	execute "VWAP2" $((2**n)) $((2**p)) $((2**t)) $((2 ** d))
	execute "VWAP3"  $((2**n)) $((2**p)) $((2**t)) $((2 ** d))
	 
done