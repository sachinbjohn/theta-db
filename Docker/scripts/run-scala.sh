#!/usr/bin/env bash
testflag=$1
timemin=$2
query=$3
current=$FOLDER
mkdir -p $current

java -Xms34g -Xmx34g -cp "vwap_2.11-0.1.jar:lib/*" queries.$query $testflag $timemin >> "$current/${query}-scala${testflag}.csv" 
