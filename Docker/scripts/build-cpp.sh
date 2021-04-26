cd srccpp/cmake-build-release
rm -f vwap*
cmake -DCMAKE_BUILD_TYPE=Release ..
make all
