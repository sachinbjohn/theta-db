#!/usr/bin/env bash
n=$1
current="$(date "+%Y.%m.%d_%H.%M.%S")"
mkdir  $current

psql -f setup.sql -U postgres -h localhost
psql -c 'call expt1(10,26,60)' -U postgres -h localhost >> $current/sql.csv  2>&1 &
#cd srccpp/cmake-build-release
#rm -f vwap*
#cmake -DCMAKE_BUILD_TYPE=Release ..
#make all

#cd /var/data
#java -Xms34g -Xmx34g -cp "vwap_2.11-0.1.jar:lib/*" queries.VWAP1 1 30 >> $current/naivescala.csv &
#java -Xms34g -Xmx34g -cp "vwap_2.11-0.1.jar:lib/*" queries.VWAP1 2 30 >> $current/dbtscala.csv &
#java -Xms34g -Xmx34g -cp "vwap_2.11-0.1.jar:lib/*" queries.VWAP1 4 60 >> $current/rangescala.csv &
#java -Xms34g -Xmx34g -cp "vwap_2.11-0.1.jar:lib/*" queries.VWAP1 8 60 >> $current/mergescala.csv &

#cd srccpp/cmake-build-release
#./vwap1 1 30  >> ../../$current/naivecpp.csv &
#./vwap1 2 30  >> ../../$current/dbtcpp.csv &
#./vwap1 4 60  >> ../../$current/rangecpp.csv &
#./vwap1 8 60  >> ../../$current/mergecpp.csv &
#for i in `seq 1 $n`
#do
#	java -Xms34g -Xmx34g -cp "vwap_2.11-0.1.jar:lib/*" exec.Executor $i $n $current &
	#echo $i
#done
wait