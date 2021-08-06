#!/bin/bash
# Absolute path to this script, e.g. /home/user/bin/foo.sh
SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")

query="MB0"
resultTable="result"
algo=$1
if [[ "$algo" == "Range" || "$algo" == "Merge" ]]; then
  exit
fi
folder=$2
expnum=$3
maxminutes=$4
run_expt() {
  n=$1
  r=$2
  p=$3
  t=$4

  tablename="bids_${n}_${r}_${p}_${t}"
  outdir="/var/data/result/$query/$tablename"
  csvpath="/var/data/csvdata/$tablename.csv"

  mkdir -p $outdir -m 777

  psql -c "call init();" -d $query -U postgres -h localhost
  psql -c "COPY bids FROM '$csvpath' DELIMITER ',' CSV HEADER" -d $query -U postgres -h localhost

  sleep 10s

  exectime=$(psql -tAc "select query$algo();" -d $query -U postgres -h localhost)
  if [[ $? -eq 0 ]]; then
    echo "$query,$algo,SQL,$n,$r,$p,$t,$exectime" >>"$folder/$query-sql-$algo.csv"
    psql -c "COPY (SELECT * FROM $resultTable r ORDER BY r.*) TO '$outdir/sql-$algo.csv' DELIMITER ',' CSV HEADER" -d $query -U postgres -h localhost
  else
    exectime=$((maxmillis + 10))
    echo "ERROR IN QUERY"
  fi

} >>"$folder/$query-sql-$algo.log" 2>&1

maxmillis=$((60000 * maxminutes))
export PGOPTIONS="-c statement_timeout=$maxmillis"
case $expnum in
0)
  run_expt 15 14 5 14
  ;;
*) ;;

esac
