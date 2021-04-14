cmake_minimum_required(VERSION 3.19)
project(VWAP)

set(CMAKE_CXX_STANDARD 14)
add_executable(VWAP main.cpp  utils/Aggregator.h utils/ComparatorOp.h ds/Table.h ds/Cube.h utils/ComparatorOp.cpp ds/Table.cpp ds/Cube.cpp ds/RangeTree.cpp ds/RangeTree.h)
target_include_directories(VWAP PUBLIC .)