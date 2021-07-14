#!/usr/bin/env bash
testflag=$1
timemin=$2
query=$3
current="output/$FOLDER"
mkdir -p "result/$query/sql"
chmod 777 "result/$query/sql" -R
mkdir -p $current

sleep 30s #wait for server to start

./sql/$query/init.sh

#warmup
psql -c "call expt1(5,5,$testflag,$timemin)" -d $query -U postgres -h localhost 

psql -c "call expt1(5,15,$testflag,$timemin)" -d $query -U postgres -h localhost > "$current/$query-sql${testflag}.out"  2>&1 
grep ",SQL," "$current/$query-sql${testflag}.out"  | sed 's/NOTICE:  //' > "$current/$query-sql${testflag}.csv" 

echo "Killing the server"
psql -c "COPY (SELECT 1) TO PROGRAM 'pg_ctl stop -m smart --no-wait';" -U postgres -h localhost