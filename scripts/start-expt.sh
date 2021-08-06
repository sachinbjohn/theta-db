current="$(date "+%Y.%m.%d_%H.%M.%S")"
echo $current >current
echo "FOLDER = $current"
queries="MB2 MB3 MB4 MB5 MB7 MB10 MB0 MB1 MB6 MB8 MB9" # Bids2 Bids3 Bids4 Bids5 Bids6"
minutes="120"
expts="0 1 2 3"

for e in $expts; do
for q in $queries; do
  ql=$(echo "$q" | tr '[:upper:]' '[:lower:]')

 
    #sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-naive/g" -e 's/TESTFLAG/1/g' -e 's/TIMEMIN/30/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-$q-naive.yaml
    #sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-naive/g" -e 's/TESTFLAG/1/g' -e 's/TIMEMIN/10/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-naive.yaml
    sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-naive/g" -e 's/TESTFLAG/1/g' -e "s/TIMEMIN/$minutes/g" -e "s/FOLDERNAME/$current/g" -e "s/EXPNUM/$e/g" Docker/expt-sql.yaml.template >Docker/expt-sql-$q-naive-e$e.yaml

    #sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-dbt/g" -e 's/TESTFLAG/2/g' -e 's/TIMEMIN/30/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-$q-dbt.yaml
    #sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-dbt/g" -e 's/TESTFLAG/2/g' -e 's/TIMEMIN/10/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-dbt.yaml
    sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-smart/g" -e 's/TESTFLAG/16/g' -e "s/TIMEMIN/$minutes/g" -e "s/FOLDERNAME/$current/g" -e "s/EXPNUM/$e/g" Docker/expt-sql.yaml.template >Docker/expt-sql-$q-smart-e$e.yaml

    #sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-range/g" -e 's/TESTFLAG/4/g' -e 's/TIMEMIN/60/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-$q-range.yaml
    #sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-range/g" -e 's/TESTFLAG/4/g' -e 's/TIMEMIN/30/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-range.yaml
    sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-range/g" -e 's/TESTFLAG/4/g' -e "s/TIMEMIN/$minutes/g" -e "s/FOLDERNAME/$current/g" -e "s/EXPNUM/$e/g" Docker/expt-sql.yaml.template >Docker/expt-sql-$q-range-e$e.yaml

    #sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-merge/g" -e 's/TESTFLAG/8/g' -e 's/TIMEMIN/60/g' -e "s/FOLDERNAME/$current/g" Docker/expt-cpp.yaml.template > Docker/expt-cpp-$q-merge.yaml
    #sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-merge/g" -e 's/TESTFLAG/8/g' -e 's/TIMEMIN/30/g' -e "s/FOLDERNAME/$current/g" Docker/expt-scala.yaml.template > Docker/expt-scala-$q-merge.yaml
    sed -e "s/QUERYNAME/$q/g" -e "s/EXPNAME/${ql}-merge/g" -e 's/TESTFLAG/8/g' -e "s/TIMEMIN/$minutes/g" -e "s/FOLDERNAME/$current/g" -e "s/EXPNUM/$e/g" Docker/expt-sql.yaml.template >Docker/expt-sql-$q-merge-e$e.yaml

    #kubectl create -f Docker/expt-cpp-$q-naive.yaml
    #kubectl create -f Docker/expt-scala-$q-naive.yaml
    kubectl create -f Docker/expt-sql-$q-naive-e$e.yaml

    #kubectl create -f Docker/expt-cpp-$q-merge.yaml
    #kubectl create -f Docker/expt-scala-$q-merge.yaml
    kubectl create -f Docker/expt-sql-$q-merge-e$e.yaml

    #kubectl create -f Docker/expt-cpp-$q-range.yaml
    #kubectl create -f Docker/expt-scala-$q-range.yaml
    kubectl create -f Docker/expt-sql-$q-range-e$e.yaml

    #kubectl create -f Docker/expt-cpp-$q-dbt.yaml
    #kubectl create -f Docker/expt-scala-$q-dbt.yaml
    kubectl create -f Docker/expt-sql-$q-smart-e$e.yaml
  done
done
