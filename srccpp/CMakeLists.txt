cmake_minimum_required(VERSION 3.19)
project(VWAP)

set(CMAKE_CXX_STANDARD 14)
set(SRC_utils utils/Aggregator.h utils/ComparatorOp.h  utils/ComparatorOp.cpp exec/VWAP.h datagen/BidsGenerator.h datagen/BidsGenerator.cpp)
set(SRC_ds ds/Table.h ds/Cube.h ds/RangeTree.h  ds/Table.cpp ds/Cube.cpp ds/RangeTree.cpp)
set(SRC_all ${SRC_utils} ${SRC_ds})

add_executable(test main.cpp ${SRC_all})
add_executable(bidsgen datagen/bidsmain.cpp datagen/BidsGenerator.cpp)
add_executable(vwap1  queries/VWAP1_DBT.h queries/vwap1.cpp ${SRC_all})
add_executable(vwap2  queries/VWAP2_DBT.h queries/vwap2.cpp ${SRC_all})

target_include_directories(test PUBLIC .)
target_include_directories(vwap1 PUBLIC . dbtoasterlib)
target_include_directories(vwap2 PUBLIC . dbtoasterlib)
target_include_directories(bidsgen PUBLIC .)
