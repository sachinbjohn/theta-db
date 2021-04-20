#!/usr/bin/env bash
n=$1
current="$(date "+%Y.%m.%d_%H.%M.%S")"
mkdir  $current
java -Xms34g -Xmx34g -cp "vwap_2.11-0.1.jar:lib/*" queries.VWAP1 >> $current/output.csv
java -Xms34g -Xmx34g -cp "vwap_2.11-0.1.jar:lib/*" queries.VWAP2 >> $current/output.csv
cd srccpp/cmake-build-release
cmake -DCMAKE_BUILD_TYPE=Release ..
make all
./vwap1 >> ../../$current/output.csv
./vwap2 >> ../../$current/output.csv
#for i in `seq 1 $n`
#do
#	java -Xms34g -Xmx34g -cp "vwap_2.11-0.1.jar:lib/*" exec.Executor $i $n $current &
	#echo $i
#done
#wait