d=$1
f="output/$d"
kubectl cp test:/var/data/output/$d $f

for e in $f/exp*
do
	echo "Query,Algo,Lang,Total,PriceTime,Price,Time,ExTime,ExecutorId,Run" > $e/all
	cat $e/*.csv >> $e/all
	mv $e/all $e/output.csv  
done