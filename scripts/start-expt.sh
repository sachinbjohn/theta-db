
current="$(date "+%Y.%m.%d_%H.%M.%S")"

echo "FOLDER = $current"
queries="VWAP1 Bids2 Bids3 Bids4 Bids5 Bids6"
for q in $queries
do 
ql=$(echo "$q" | tr '[:upper:]' '[:lower:]')	
#sed -e 's/EXPNAME/naive/g' -e 's/TESTFLAG/1/g' -e 's/TIMEMIN/20/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-naive.yaml
sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-naive/g" -e 's/TESTFLAG/1/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-naive.yaml
#sed -e 's/EXPNAME/naive/g' -e 's/TESTFLAG/1/g' -e 's/TIMEMIN/20/g' -e "s/FOLDERNAME/$current/g" Docker/expt-sql.yaml.template > Docker/expt-sql-naive.yaml


#sed -e 's/EXPNAME/dbt/g' -e 's/TESTFLAG/2/g' -e 's/TIMEMIN/20/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-dbt.yaml
sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-dbt/g" -e 's/TESTFLAG/2/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-dbt.yaml


#sed -e 's/EXPNAME/range/g' -e 's/TESTFLAG/4/g' -e 's/TIMEMIN/60/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-range.yaml
sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-range/g" -e 's/TESTFLAG/4/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-range.yaml
#sed -e 's/EXPNAME/range/g' -e 's/TESTFLAG/4/g' -e 's/TIMEMIN/20/g' -e "s/FOLDERNAME/$current/g" Docker/expt-sql.yaml.template > Docker/expt-sql-range.yaml


#sed -e 's/EXPNAME/merge/g' -e 's/TESTFLAG/8/g' -e 's/TIMEMIN/60/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-merge.yaml
sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-merge/g" -e 's/TESTFLAG/8/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-merge.yaml
#sed -e 's/EXPNAME/merge/g' -e 's/TESTFLAG/8/g' -e 's/TIMEMIN/60/g' -e "s/FOLDERNAME/$current/g" Docker/expt-sql.yaml.template > Docker/expt-sql-merge.yaml


#kubectl create -f Docker/expt-cpp-naive.yaml
kubectl create -f Docker/expt-scala-$q-naive.yaml
#kubectl create -f Docker/expt-sql-naive.yaml

#kubectl create -f Docker/expt-cpp-merge.yaml
kubectl create -f Docker/expt-scala-$q-merge.yaml
#kubectl create -f Docker/expt-sql-merge.yaml

#kubectl create -f Docker/expt-cpp-range.yaml
kubectl create -f Docker/expt-scala-$q-range.yaml
#kubectl create -f Docker/expt-sql-range.yaml

#kubectl create -f Docker/expt-cpp-dbt.yaml
kubectl create -f Docker/expt-scala-$q-dbt.yaml

done