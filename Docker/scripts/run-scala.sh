#!/usr/bin/env bash
testflag=$1
timemin=$2
current=$FOLDER
mkdir -p $current

java -Xms34g -Xmx34g -cp "vwap_2.11-0.1.jar:lib/*" queries.VWAP1 $testflag $timemin >> "$current/scala${testflag}.csv" 
