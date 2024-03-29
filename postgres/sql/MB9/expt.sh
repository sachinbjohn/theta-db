#!/bin/bash
# Absolute path to this script, e.g. /home/user/bin/foo.sh
SCRIPT=$(readlink -f "$0")
# Absolute path this script is in, thus /home/user/bin
SCRIPTPATH=$(dirname "$SCRIPT")

query="MB9"
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
run_exptR() {
  n=$1
  r=$2
  p=$3
  t=$4

  n2=16
  r2=15
  p2=15
  t2=5

  tablenameR="bids_${n}_${r}_${p}_${t}"
  tablenameS="bids_${n2}_${r2}_${p2}_${t2}"

  outdir="/var/data/result/$query/$tablenameR-$tablenameS"
  csvpathR="/var/data/csvdata/$tablenameR.csv"
  csvpathS="/var/data/csvdata/$tablenameS.csv"

  mkdir -p $outdir -m 777

  psql -c "call init();" -d $query -U postgres -h localhost
  psql -c "COPY bidsR FROM '$csvpathR' DELIMITER ',' CSV HEADER" -d $query -U postgres -h localhost
  psql -c "COPY bidsS FROM '$csvpathS' DELIMITER ',' CSV HEADER" -d $query -U postgres -h localhost

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


run_exptS() {
  n=$1
  r=$2
  p=$3
  t=$4

  n2=16
  r2=15
  p2=15
  t2=5

  tablenameS="bids_${n}_${r}_${p}_${t}"
  tablenameR="bids_${n2}_${r2}_${p2}_${t2}"

  outdir="/var/data/result/$query/$tablenameR-$tablenameS"
  csvpathR="/var/data/csvdata/$tablenameR.csv"
  csvpathS="/var/data/csvdata/$tablenameS.csv"

  mkdir -p $outdir -m 777

  psql -c "call init();" -d $query -U postgres -h localhost
  psql -c "COPY bidsR FROM '$csvpathR' DELIMITER ',' CSV HEADER" -d $query -U postgres -h localhost
  psql -c "COPY bidsS FROM '$csvpathS' DELIMITER ',' CSV HEADER" -d $query -U postgres -h localhost

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
1)
  for i in $(seq 10 2 20); do
    run_exptR $((i + 1)) $i $i $((i-10))
    if [[ "$exectime" -gt "$maxmillis" ]]; then
      break
    fi
  done
  ;;
2)
  for i in $(seq 10 2 20); do
    run_exptS $((i + 1)) $i $i $((i-10))
    if [[ "$exectime" -gt "$maxmillis" ]]; then
      break
    fi
  done
  ;;
*) ;;

esac
