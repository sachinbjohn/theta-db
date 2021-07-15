cd srccpp/cmake-build-release
rm -f vwap*
cmake -DCMAKE_BUILD_TYPE=Release ..
make -j8 all 
