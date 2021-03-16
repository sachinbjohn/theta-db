d=$1
f="output/$d"
kubectl cp test:/data/$d $f

echo "Query,Algo,Total,Price,Time,PriceTime,ExTime,ExecutorId,Run" > $f/all.csv
cat $f/out*.csv >> $f/all.csv

mv $f/all.csv $f/output.csv  