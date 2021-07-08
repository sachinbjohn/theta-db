
current="$(date "+%Y.%m.%d_%H.%M.%S")"

echo "FOLDER = $current"
queries="VWAP1 VWAP2" # Bids2 Bids3 Bids4 Bids5 Bids6"
for q in $queries
do 
ql=$(echo "$q" | tr '[:upper:]' '[:lower:]')	
#sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-naive/g" -e 's/TESTFLAG/1/g' -e 's/TIMEMIN/30/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-$q-naive.yaml
#sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-naive/g" -e 's/TESTFLAG/1/g' -e 's/TIMEMIN/10/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-naive.yaml
 sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-naive/g" -e 's/TESTFLAG/1/g' -e 's/TIMEMIN/20/g' -e "s/FOLDERNAME/$current/g" Docker/expt-sql.yaml.template > Docker/expt-sql-$q-naive.yaml


#sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-dbt/g" -e 's/TESTFLAG/2/g' -e 's/TIMEMIN/30/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-$q-dbt.yaml
#sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-dbt/g" -e 's/TESTFLAG/2/g' -e 's/TIMEMIN/10/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-dbt.yaml


#sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-range/g" -e 's/TESTFLAG/4/g' -e 's/TIMEMIN/60/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-$q-range.yaml
#sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-range/g" -e 's/TESTFLAG/4/g' -e 's/TIMEMIN/30/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-range.yaml
 sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-range/g" -e 's/TESTFLAG/4/g' -e 's/TIMEMIN/20/g' -e "s/FOLDERNAME/$current/g" Docker/expt-sql.yaml.template > Docker/expt-sql-$q-range.yaml


#sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-merge/g" -e 's/TESTFLAG/8/g' -e 's/TIMEMIN/60/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-$q-merge.yaml
#sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-merge/g" -e 's/TESTFLAG/8/g' -e 's/TIMEMIN/30/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-merge.yaml
 sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-merge/g" -e 's/TESTFLAG/8/g' -e 's/TIMEMIN/60/g' -e "s/FOLDERNAME/$current/g" Docker/expt-sql.yaml.template > Docker/expt-sql-$q-merge.yaml


#kubectl create -f Docker/expt-cpp-$q-naive.yaml
#kubectl create -f Docker/expt-scala-$q-naive.yaml
 kubectl create -f Docker/expt-sql-$q-naive.yaml

#kubectl create -f Docker/expt-cpp-$q-merge.yaml
#kubectl create -f Docker/expt-scala-$q-merge.yaml
 kubectl create -f Docker/expt-sql-$q-merge.yaml

#kubectl create -f Docker/expt-cpp-$q-range.yaml
#kubectl create -f Docker/expt-scala-$q-range.yaml
 kubectl create -f Docker/expt-sql-$q-range.yaml

#kubectl create -f Docker/expt-cpp-$q-dbt.yaml
#kubectl create -f Docker/expt-scala-$q-dbt.yaml

done