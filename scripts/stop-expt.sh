queries="VWAP1 Bids2 Bids3 Bids4 Bids5 Bids6"
for q in $queries
do 
#kubectl delete -f Docker/expt-cpp-naive.yaml
kubectl delete -f Docker/expt-scala-$q-naive.yaml
#kubectl delete -f Docker/expt-sql-naive.yaml

#kubectl delete -f Docker/expt-cpp-merge.yaml
kubectl delete -f Docker/expt-scala-$q-merge.yaml
#kubectl delete -f Docker/expt-sql-merge.yaml

#kubectl delete -f Docker/expt-cpp-range.yaml
kubectl delete -f Docker/expt-scala-$q-range.yaml
#kubectl delete -f Docker/expt-sql-range.yaml

#kubectl delete -f Docker/expt-cpp-dbt.yaml
kubectl delete -f Docker/expt-scala-$q-dbt.yaml
	
#rm -f Docker/expt-cpp-naive.yaml
rm -f Docker/expt-scala-$q-naive.yaml
#rm -f Docker/expt-sql-naive.yaml

#rm -f Docker/expt-cpp-merge.yaml
rm -f Docker/expt-scala-$q-merge.yaml
#rm -f Docker/expt-sql-merge.yaml

#rm -f Docker/expt-cpp-range.yaml
rm -f Docker/expt-scala-$q-range.yaml
#rm -f Docker/expt-sql-range.yaml

#rm -f Docker/expt-cpp-dbt.yaml
rm -f Docker/expt-scala-$q-dbt.yaml


done