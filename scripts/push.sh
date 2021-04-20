sbt package
kubectl cp Docker/scripts/run.sh test:/data/
kubectl cp target/scala-2.11/vwap_2.11-0.1.jar test:/data/
kubectl cp srccpp/ds test:/data/srccpp
kubectl cp srccpp/utils test:/data/srccpp
kubectl cp srccpp/exec test:/data/srccpp
kubectl cp srccpp/queries test:/data/srccpp
kubectl cp srccpp/datagen test:/data/srccpp
kubectl cp srccpp/CMakeLists.txt test:/data/srccpp