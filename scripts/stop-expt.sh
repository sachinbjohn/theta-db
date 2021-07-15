for f in  "Docker"/"expt"*.yaml; do
  kubectl delete -f $f && rm $f
done