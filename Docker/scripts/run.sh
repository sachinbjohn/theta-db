#!/usr/bin/env bash
n=$1
current="$(date "+%Y.%m.%d_%H.%M.%S")"
mkdir  $current
for i in `seq 1 $n`
do
	java -Xms14g -Xmx14g -cp "vwap_2.11-0.1.jar:lib/*" exec.Executor $i $n $current &
	#echo $i
done
wait