#!/bin/bash
# Absolute path to this script, e.g. /home/user/bin/foo.sh
SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")

query="MB8"
resultTable="result"
algoorig=$1

case $algoorig in
"Naive") algo="RSMerge" ;;
"Smart") algo="RSRange" ;;
"Merge") algo="SRMerge" ;;
"Range") algo="SRRange" ;;
esac

folder=$2
expnum=$3
maxminutes=$4
run_expt() {
  n=$1
  r=$2
  p=$3
  t=$4

  tablenameR="bids_${n}_${r}_${p}_${t}"
  tablenameS="bids_15_13_13_13"

  outdir="/var/data/result/$query/$tablenameR"
  csvpathR="/var/data/csvdata/$tablenameR.csv"
  csvpathS="/var/data/csvdata/$tablenameS.csv"

  mkdir -p $outdir -m 777

  psql -c "call init();" -d $query -U postgres -h localhost
  psql -c "COPY bidsR FROM '$csvpathR' DELIMITER ',' CSV HEADER" -d $query -U postgres -h localhost
  psql -c "COPY bidsS FROM '$csvpathS' DELIMITER ',' CSV HEADER" -d $query -U postgres -h localhost

  sleep 10s

  exectime=$(psql -tAc "select query$algo($p, $t);" -d $query -U postgres -h localhost)
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
1)
  for i in $(seq 10 20); do
    run_expt $((i + 2)) $i $i $i
    if [[ "$exectime" -gt "$maxmillis" ]]; then
      break
    fi
  done
  ;;
*)
 ;;
esac
