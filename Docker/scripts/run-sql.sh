#!/usr/bin/env bash
testflag=$1
timemin=$2
current=$FOLDER
mkdir -p $current

sleep 2m #wait for server to start
psql -f setup.sql -U postgres -h localhost
psql -c "call expt1(10,12,$testflag,$timemin)" -U postgres -h localhost > "$current/sql${testflag}.csv"  2>&1 
