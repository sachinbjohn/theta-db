sbt package
kubectl cp Docker/scripts/ test:/var/data/

kubectl cp target/scala-2.11/vwap_2.11-0.1.jar test:/var/data/
kubectl cp srccpp/ds test:/var/data/srccpp
kubectl cp srccpp/main.cpp test:/var/data/srccpp
kubectl cp srccpp/utils test:/var/data/srccpp
kubectl cp srccpp/exec test:/var/data/srccpp
kubectl cp srccpp/queries test:/var/data/srccpp
kubectl cp srccpp/datagen test:/var/data/srccpp
kubectl cp srccpp/CMakeLists.txt test:/var/data/srccpp
kubectl exec -it test /bin/bash /var/data/scripts/build-cpp.sh

kubectl cp postgres/queries/vwap1/vwap1.sql test:/var/data/
kubectl cp postgres/queries/vwap2/vwap2.sql test:/var/data/