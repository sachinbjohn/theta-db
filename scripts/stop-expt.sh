queries="MB0 MB1 MB2 MB3 MB4 MB5 MB6 MB7" #Bids2 Bids3 Bids4 Bids5 Bids6"
for q in $queries
do 
#kubectl delete -f Docker/expt-cpp-$q-naive.yaml
#kubectl delete -f Docker/expt-scala-$q-naive.yaml
 kubectl delete -f Docker/expt-sql-$q-naive.yaml

#kubectl delete -f Docker/expt-cpp-$q-merge.yaml
#kubectl delete -f Docker/expt-scala-$q-merge.yaml
 kubectl delete -f Docker/expt-sql-$q-merge.yaml

#kubectl delete -f Docker/expt-cpp-$q-range.yaml
#kubectl delete -f Docker/expt-scala-$q-range.yaml
kubectl delete -f Docker/expt-sql-$q-range.yaml

#kubectl delete -f Docker/expt-cpp-$q-dbt.yaml
#kubectl delete -f Docker/expt-scala-$q-dbt.
kubectl delete -f Docker/expt-sql-$q-smart.yaml

	
#rm -f Docker/expt-cpp-$q-naive.yaml
#rm -f Docker/expt-scala-$q-naive.yaml
rm -f Docker/expt-sql-$q-naive.yaml

#rm -f Docker/expt-cpp-$q-merge.yaml
#rm -f Docker/expt-scala-$q-merge.yaml
rm -f Docker/expt-sql-$q-merge.yaml

#rm -f Docker/expt-cpp-$q-range.yaml
#rm -f Docker/expt-scala-$q-range.yaml
rm -f Docker/expt-sql-$q-range.yaml

#rm -f Docker/expt-cpp-$q-dbt.yaml
#rm -f Docker/expt-scala-$q-dbt.yaml
rm -f Docker/expt-sql-$q-smart.yaml

done