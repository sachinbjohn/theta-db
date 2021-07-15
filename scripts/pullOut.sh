d=$1
f="output/$d"
kubectl cp test:/var/data/output/$d $f

echo "Query,Algo,Lang,Total,PriceTime,Price,Time,ExTime,ExecutorId,Run" > $f/all
cat $f/*.csv >> $f/all

mv $f/all $f/output.csv  