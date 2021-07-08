#!/usr/bin/env bash
testflag=$1
timemin=$2
query=$3
current=$FOLDER
mkdir -p $current

sleep 2m #wait for server to start

./sql/$query/init.sh

#warmup
psql -c "call expt1(10,10,$testflag,$timemin)" -d $query -U postgres -h localhost 

psql -c "call expt1(10,13,$testflag,$timemin)" -d $query -U postgres -h localhost > "$current/$query-sql${testflag}.out"  2>&1 
grep ",SQL," "$current/$query-sql${testflag}.out"  | sed 's/NOTICE:  //' > "$current/$query-sql${testflag}.csv" 

echo "Killing the server"
psql -c "COPY (SELECT 1) TO PROGRAM 'pg_ctl stop -m smart --no-wait';" -U postgres -h localhost