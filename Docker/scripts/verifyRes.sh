#!/usr/bin/env bash

folder="/var/data/result"

for q in $folder/*/*
do
  #echo " "
  #echo "Folder $q"
  arr=($(ls $q))
  for a in "${arr[@]}"
  do
    diff -q $q/$a $q/${arr[0]}
  done
done