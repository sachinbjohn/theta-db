#!/usr/bin/env zsh

current="output/$(date "+%Y.%m.%d_%H.%M.%S")"
mkdir  $current
out="$current/output.csv"

echo "Query,Algo,Total,Price,Time,PriceTime,Ex1,Ex2,Ex3" >> $out
exp_num=0
execute() {
  exp_num=$((exp_num + 1))
	local query=$1
	local total=$2
	local price=$3
	local time=$4
	local pricetime=$5
	local numRuns=3
	local algo=$6
	
#	if [ "$pricetime" -le "$pt" ]
#	then
		echo "Running $exp_num Query $query Algo $algo Total $total Price $price Time $time PT $pricetime at " $(date "+%Y.%m.%d %H.%M.%S")
		sbt --error "runMain query.$query $total $price $time $pricetime $numRuns $algo" >> $out
#	else
#		echo "Skip $exp_num" $1 $2 $3 $4 $5
#	fi

}


{
for allparams in `cat allparams.txt`
do
    
    n=`echo $allparams | cut -d: -f1`
    p=`echo $allparams | cut -d: -f2`
    t=`echo $allparams | cut -d: -f3`
    pt=`echo $allparams | cut -d: -f4`
    q=`echo $allparams | cut -d: -f5`
    a=`echo $allparams | cut -d: -f6`


	execute $q $n $p $t $pt $a
done
} 2>&1 | tee "$current/log.txt"