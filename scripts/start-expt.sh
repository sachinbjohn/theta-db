
current="$(date "+%Y.%m.%d_%H.%M.%S")"

echo "FOLDER = $current"
sed -e 's/EXPNAME/vwap1-naive/g' -e 's/TESTFLAG/1/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-naive.yaml
sed -e 's/EXPNAME/vwap1-naive/g' -e 's/TESTFLAG/1/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-naive.yaml
sed -e 's/EXPNAME/vwap1-naive/g' -e 's/TESTFLAG/1/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-sql.yaml.template > Docker/expt-sql-naive.yaml


sed -e 's/EXPNAME/vwap1-dbt/g' -e 's/TESTFLAG/2/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-dbt.yaml
sed -e 's/EXPNAME/vwap1-dbt/g' -e 's/TESTFLAG/2/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-dbt.yaml


sed -e 's/EXPNAME/vwap1-range/g' -e 's/TESTFLAG/4/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-range.yaml
sed -e 's/EXPNAME/vwap1-range/g' -e 's/TESTFLAG/4/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-range.yaml
sed -e 's/EXPNAME/vwap1-range/g' -e 's/TESTFLAG/4/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-sql.yaml.template > Docker/expt-sql-range.yaml


sed -e 's/EXPNAME/vwap1-merge/g' -e 's/TESTFLAG/8/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-merge.yaml
sed -e 's/EXPNAME/vwap1-merge/g' -e 's/TESTFLAG/8/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-merge.yaml
sed -e 's/EXPNAME/vwap1-merge/g' -e 's/TESTFLAG/8/g' -e 's/TIMEMIN/1/g' -e "s/FOLDERNAME/$current/g" Docker/expt-sql.yaml.template > Docker/expt-sql-merge.yaml


kubectl create -f Docker/expt-cpp-naive.yaml
kubectl create -f Docker/expt-scala-naive.yaml
kubectl create -f Docker/expt-sql-naive.yaml

kubectl create -f Docker/expt-cpp-merge.yaml
kubectl create -f Docker/expt-scala-merge.yaml
kubectl create -f Docker/expt-sql-merge.yaml

kubectl create -f Docker/expt-cpp-range.yaml
kubectl create -f Docker/expt-scala-range.yaml
kubectl create -f Docker/expt-sql-range.yaml

kubectl create -f Docker/expt-cpp-dbt.yaml
kubectl create -f Docker/expt-scala-dbt.yaml

