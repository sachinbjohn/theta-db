#!/usr/bin/env bash
testflag=$1
timemin=$2
current=$FOLDER
mkdir -p $current


cd srccpp/cmake-build-release
./vwap1 $testflag $timemin  >> "../../$current/cpp${testflag}.csv" 
