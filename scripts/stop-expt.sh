kubectl delete -f Docker/expt-cpp-naive.yaml
kubectl delete -f Docker/expt-scala-naive.yaml
kubectl delete -f Docker/expt-sql-naive.yaml

kubectl delete -f Docker/expt-cpp-merge.yaml
kubectl delete -f Docker/expt-scala-merge.yaml
kubectl delete -f Docker/expt-sql-merge.yaml

kubectl delete -f Docker/expt-cpp-range.yaml
kubectl delete -f Docker/expt-scala-range.yaml
kubectl delete -f Docker/expt-sql-range.yaml

kubectl delete -f Docker/expt-cpp-dbt.yaml
kubectl delete -f Docker/expt-scala-dbt.yaml