#!/usr/bin/env bash
testflag=$1
timemin=$2
query=$3
current=$FOLDER
mkdir -p $current


cd srccpp/cmake-build-release
./$query $testflag $timemin  >> "../../$current/$query-cpp${testflag}.csv" 
