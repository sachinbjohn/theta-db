#!/bin/bash
set -x
# Absolute path to this script, e.g. /home/user/bin/foo.sh
SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")

query="MB3"
resultTable="result"
algo=$1
folder=$2

run_expt() {
  n=$1
  r=$2
  p=$3
  t=$4

  tablename="bids_${n}_${r}_${p}_${t}"
  outdir="/var/data/result/$query/$tablename"
  csvpath="/var/data/csvdata/$tablename.csv"

  mkdir -p $outdir -m 777

  psql -c "call init$algo();" -d $query -U postgres -h localhost
  psql -c "COPY bids FROM '$csvpath' DELIMITER ',' CSV HEADER" -d $query -U postgres -h localhost

  sleep 10s

  exectime=$(psql -tAc "select query$algo($p, $t);" -d $query -U postgres -h localhost)
  echo "$query,$algo,SQL,$n,$r,$p,$t,$exectime" >>"$folder/$query-sql-$algo.csv"
  psql -c "COPY (SELECT * FROM $resultTable r ORDER BY r.*) TO '$outdir/sql-$algo.csv' DELIMITER ',' CSV HEADER" -d $query -U postgres -h localhost
} >>"$folder/$query-sql-$algo.out" 2>&1

maxminutes=1
maxmillis=$((60000 * maxminutes))
for i in $(seq 15 25); do
  run_expt $((i+1)) $i $((i-5)) $((i-5))
  if [[ "$exectime" -gt "$maxmillis" ]]; then
    break
  fi
done
