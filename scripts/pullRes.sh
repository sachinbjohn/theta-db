d=$1
f="output/$d"
kubectl cp test:/var/data/$d $f

echo "Query,Algo,Lang,Total,Price,Time,PriceTime,ExTime,ExecutorId,Run" > $f/all
cat $f/*.csv >> $f/all

mv $f/all $f/output.csv  