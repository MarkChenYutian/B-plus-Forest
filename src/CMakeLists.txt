# Minimum required CMake version
cmake_minimum_required(VERSION 3.10)

# Project name
project(BForest)

include(CTest)

# Specify the C++ standard (e.g., C++17)
set(CMAKE_CXX_STANDARD 17)
set(ENV{TSAN_OPTIONS} "second_deadlock_stack=1")

# Get required packages
find_package(Threads REQUIRED)
find_package(Boost REQUIRED)
find_package(MPI REQUIRED)

# Check for the compiler and set the appropriate flags
if(CMAKE_CXX_COMPILER_ID STREQUAL "GNU" OR CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    # Add -msse2 to the compile options
    add_compile_options(-msse2)
elseif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    # MSVC enables SSE2 by default for x64 targets
    # For x86 targets, use /arch:SSE2
    if(CMAKE_SIZEOF_VOID_P EQUAL 4)
        add_compile_options(/arch:SSE2)
    endif()
endif()


# Define header files
set(HEADERS
    # Utility
    includes/timing.h

    # Project file
    includes/tree.h
    includes/engine.hpp

    # Free Tree
    includes/freeTree/freeTree.hpp
    includes/freeTree/freeNode.hpp
    includes/freeTree/scheduler.hpp
    includes/freeTree/worker.hpp
    includes/freeTree/background.hpp

    # Seq Tree
    includes/seqTree/seqTree.hpp
    includes/seqTree/seqNode.hpp

    # Coarse Tree
    includes/coarseTree/coarseTree.hpp

    # Fine Tree
    includes/fineTree/fineTree.hpp
    includes/fineTree/fineNode.hpp

    # Distri Tree
    # includes/distriTree/distriTree.h
    # includes/distriTree/distriTree.hpp
)

# Include the 'includes' directory for header files
include_directories(${Boost_INCLUDE_DIRS})
# include_directories(${MPI_INCLUDE_PATH})
include_directories(${CMAKE_SOURCE_DIR}/includes)



################# Sequential Tree ####################
set(SEQ_TEST_SRC src/seqTest.cpp)
# Unit test build
add_executable(seqTestDebug ${HEADERS} ${SEQ_TEST_SRC})
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
target_compile_options(seqTestDebug PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer -g -DDEBUG)
target_link_libraries(seqTestDebug ${MPI_LIBRARIES})
set_target_properties(seqTestDebug PROPERTIES 
    COMPILE_FLAGS "-fsanitize=address,undefined -fno-omit-frame-pointer -g -DDEBUG ${MPI_COMPILE_FLAGS}"
    LINK_FLAGS "-fsanitize=address,undefined ${MPI_LINK_FLAGS}"
)

add_executable(seqTestRelease ${HEADERS} ${SEQ_TEST_SRC})
target_compile_options(seqTestRelease PRIVATE -O3)
target_link_libraries(seqTestRelease ${MPI_LIBRARIES})
set_target_properties(seqTestRelease PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}" LINK_FLAGS "${MPI_LINK_FLAGS}")



################# Coarse-grain Tree ####################
set(COARSE_TEST_SRC src/coarseTest.cpp)
# Test engine build
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
add_executable(coarseTestDebug ${HEADERS} ${COARSE_TEST_SRC})
target_compile_options(coarseTestDebug PRIVATE -fsanitize=undefined,address -fno-omit-frame-pointer -g -DDEBUG)
target_link_libraries(coarseTestDebug PRIVATE Threads::Threads ${MPI_LIBRARIES})
set_target_properties(coarseTestDebug PROPERTIES 
    COMPILE_FLAGS "-fsanitize=address,undefined -fno-omit-frame-pointer -g -DDEBUG ${MPI_COMPILE_FLAGS}"
    LINK_FLAGS "-fsanitize=address,undefined ${MPI_LINK_FLAGS}"
)

# Test engine build release
add_executable(coarseTestRelease ${HEADERS} ${COARSE_TEST_SRC})
target_compile_options(coarseTestRelease PRIVATE -O3)
target_link_libraries(coarseTestRelease PRIVATE Threads::Threads  ${MPI_LIBRARIES})
set_target_properties(coarseTestRelease PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}" LINK_FLAGS "${MPI_LINK_FLAGS}")


################# Fine-grain Tree ####################
set(FINE_TEST_SRC src/fineTest.cpp)
# Test engine build
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
add_executable(fineTestDebug ${HEADERS} ${FINE_TEST_SRC})
target_compile_options(fineTestDebug PRIVATE -fsanitize=undefined,address -fno-omit-frame-pointer -g -DDEBUG)
target_link_libraries(fineTestDebug PRIVATE Threads::Threads ${MPI_LIBRARIES})
set_target_properties(fineTestDebug PROPERTIES 
    COMPILE_FLAGS "-fsanitize=address,undefined -fno-omit-frame-pointer -g -DDEBUG ${MPI_COMPILE_FLAGS}"
    LINK_FLAGS "-fsanitize=address,undefined ${MPI_LINK_FLAGS}"
)


# Test engine build release
add_executable(fineTestRelease ${HEADERS} ${COARSE_TEST_SRC})
target_compile_options(fineTestRelease PRIVATE -O3)
target_link_libraries(fineTestRelease PRIVATE Threads::Threads ${MPI_LIBRARIES})
set_target_properties(fineTestRelease PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}" LINK_FLAGS "${MPI_LINK_FLAGS}")



################ PALM TREE ##############
set(PALM_TEST_SRC src/freeTest.cpp)
# PalmTree Test build
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
add_executable(FreeTreeDebug ${HEADERS} ${PALM_TEST_SRC})
target_compile_options(FreeTreeDebug PRIVATE -fsanitize=undefined,address,leak -fno-omit-frame-pointer -g -DDEBUG)
target_link_libraries(FreeTreeDebug PRIVATE Threads::Threads)
target_link_libraries(FreeTreeDebug PRIVATE ${Boost_LIBRARIES} ${MPI_LIBRARIES})
set_target_properties(FreeTreeDebug PROPERTIES 
    COMPILE_FLAGS "-fsanitize=address,undefined -fno-omit-frame-pointer -g -DDEBUG ${MPI_COMPILE_FLAGS}"
    LINK_FLAGS "-fsanitize=address,undefined ${MPI_LINK_FLAGS}"
)

# PalmTree Test Release build
add_executable(FreeTreeRelease ${HEADERS} ${PALM_TEST_SRC})
target_compile_options(FreeTreeRelease PRIVATE -O3)
target_link_libraries(FreeTreeRelease PRIVATE Threads::Threads)
target_link_libraries(FreeTreeRelease PRIVATE ${Boost_LIBRARIES} ${MPI_LIBRARIES})
set_target_properties(FreeTreeRelease PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}" LINK_FLAGS "${MPI_LINK_FLAGS}")



################# DISTRIBUTED TREE ####################
# set(DISTRI_TREE_SRC src/distriTest.cpp)
# # Enable UndefinedBehaviorSanitizer (UBSan)
# # No address sanitizer since the OpenMPI itself has memory leak, and will cause a lot of false positive.
# add_executable(distributeTreeDebug ${HEADERS} ${DISTRI_TREE_SRC})
# target_link_libraries(distributeTreeDebug ${MPI_LIBRARIES})
# target_compile_options(distributeTreeDebug PRIVATE -fsanitize=undefined -fno-omit-frame-pointer -g -DDEBUG)
# set_target_properties(distributeTreeDebug PROPERTIES 
#     COMPILE_FLAGS "-fsanitize=undefined -fno-omit-frame-pointer -g -DDEBUG ${MPI_COMPILE_FLAGS}"
#     LINK_FLAGS "-fsanitize=undefined ${MPI_LINK_FLAGS}"
# )

################ BENCHMARKING #########################
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pg")
# set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pg")

set(BENCHMARK_SRC src/benchmark.cpp)
add_executable(benchmarkOptimized ${HEADERS} ${BENCHMARK_SRC})
target_compile_options(benchmarkOptimized PRIVATE -O0)
target_link_libraries(benchmarkOptimized PRIVATE Threads::Threads)
target_link_libraries(benchmarkOptimized PRIVATE ${Boost_LIBRARIES} ${MPI_LIBRARIES})
set_target_properties(benchmarkOptimized PROPERTIES COMPILE_FLAGS "${MPI_COMPILE_FLAGS}" LINK_FLAGS "${MPI_LINK_FLAGS}")


################ Automated Testing #####################
set(BENCHMARK_SRC src/ctestRunner.cpp)
add_executable(AutoTest ${HEADERS} ${BENCHMARK_SRC})
target_compile_options(AutoTest PRIVATE -O2 -DDEBUG)
target_link_libraries(AutoTest PRIVATE Threads::Threads)

# Sequential Tree
add_test(NAME SeqTreeSmall0_ord3 COMMAND ./AutoTest 3 1 Seq small_0.case)
set_tests_properties(SeqTreeSmall0_ord3 PROPERTIES LABELS "Sequential")

add_test(NAME SeqTreeSmall0_ord4 COMMAND ./AutoTest 4 1 Seq small_0.case)
set_tests_properties(SeqTreeSmall0_ord4 PROPERTIES LABELS "Sequential")

add_test(NAME SeqTreeSmall0_ord5 COMMAND ./AutoTest 5 1 Seq small_0.case)
set_tests_properties(SeqTreeSmall0_ord5 PROPERTIES LABELS "Sequential")

add_test(NAME SeqTreeSmall0_ord6 COMMAND ./AutoTest 6 1 Seq small_0.case)
set_tests_properties(SeqTreeSmall0_ord6 PROPERTIES LABELS "Sequential")

add_test(NAME SeqTreeLarge0_ord3 COMMAND ./AutoTest 3 1 Seq large_0.case)
set_tests_properties(SeqTreeLarge0_ord3 PROPERTIES LABELS "Sequential")

add_test(NAME SeqTreeLarge0_ord4 COMMAND ./AutoTest 4 1 Seq large_0.case)
set_tests_properties(SeqTreeLarge0_ord4 PROPERTIES LABELS "Sequential")

add_test(NAME SeqTreeLarge0_ord5 COMMAND ./AutoTest 5 1 Seq large_0.case)
set_tests_properties(SeqTreeLarge0_ord5 PROPERTIES RUN_SERIAL TRUE LABELS "Sequential")

add_test(NAME SeqTreeLarge0_ord6 COMMAND ./AutoTest 6 1 Seq large_0.case)
set_tests_properties(SeqTreeLarge0_ord6 PROPERTIES RUN_SERIAL TRUE LABELS "Sequential")



# CoarseLock Tree
add_test(NAME CoarseTreeSmall0_ord3 COMMAND ./AutoTest 3 4 Coarse small_0.case)
set_tests_properties(CoarseTreeSmall0_ord3 PROPERTIES RUN_SERIAL TRUE LABELS "CoarseLock")

add_test(NAME CoarseTreeSmall0_ord4 COMMAND ./AutoTest 4 4 Coarse small_0.case)
set_tests_properties(CoarseTreeSmall0_ord4 PROPERTIES RUN_SERIAL TRUE LABELS "CoarseLock")

add_test(NAME CoarseTreeSmall0_ord5 COMMAND ./AutoTest 5 4 Coarse small_0.case)
set_tests_properties(CoarseTreeSmall0_ord5 PROPERTIES RUN_SERIAL TRUE LABELS "CoarseLock")

add_test(NAME CoarseTreeSmall0_ord6 COMMAND ./AutoTest 6 4 Coarse small_0.case)
set_tests_properties(CoarseTreeSmall0_ord6 PROPERTIES RUN_SERIAL TRUE LABELS "CoarseLock")

add_test(NAME CoarseTreeLarge0_ord3 COMMAND ./AutoTest 3 4 Coarse large_0.case)
set_tests_properties(CoarseTreeLarge0_ord3 PROPERTIES RUN_SERIAL TRUE LABELS "CoarseLock")

add_test(NAME CoarseTreeLarge0_ord4 COMMAND ./AutoTest 4 4 Coarse large_0.case)
set_tests_properties(CoarseTreeLarge0_ord4 PROPERTIES RUN_SERIAL TRUE LABELS "CoarseLock")

add_test(NAME CoarseTreeLarge0_ord5 COMMAND ./AutoTest 5 4 Coarse large_0.case)
set_tests_properties(CoarseTreeLarge0_ord5 PROPERTIES RUN_SERIAL TRUE LABELS "CoarseLock")

add_test(NAME CoarseTreeLarge0_ord6 COMMAND ./AutoTest 6 4 Coarse large_0.case)
set_tests_properties(CoarseTreeLarge0_ord6 PROPERTIES RUN_SERIAL TRUE LABELS "CoarseLock")



# FineLock Tree
add_test(NAME FineTreeSmall0_ord3 COMMAND ./AutoTest 3 4 Fine small_0.case)
set_tests_properties(FineTreeSmall0_ord3 PROPERTIES RUN_SERIAL TRUE LABELS "FineLock")

add_test(NAME FineTreeSmall0_ord4 COMMAND ./AutoTest 4 4 Fine small_0.case)
set_tests_properties(FineTreeSmall0_ord4 PROPERTIES RUN_SERIAL TRUE LABELS "FineLock")

add_test(NAME FineTreeSmall0_ord5 COMMAND ./AutoTest 5 4 Fine small_0.case)
set_tests_properties(FineTreeSmall0_ord5 PROPERTIES RUN_SERIAL TRUE LABELS "FineLock")

add_test(NAME FineTreeSmall0_ord6 COMMAND ./AutoTest 6 4 Fine small_0.case)
set_tests_properties(FineTreeSmall0_ord6 PROPERTIES RUN_SERIAL TRUE LABELS "FineLock")

add_test(NAME FineTreeLarge0_ord3 COMMAND ./AutoTest 3 4 Fine large_0.case)
set_tests_properties(FineTreeLarge0_ord3 PROPERTIES RUN_SERIAL TRUE LABELS "FineLock")

add_test(NAME FineTreeLarge0_ord4 COMMAND ./AutoTest 4 4 Fine large_0.case)
set_tests_properties(FineTreeLarge0_ord4 PROPERTIES RUN_SERIAL TRUE LABELS "FineLock")

add_test(NAME FineTreeLarge0_ord5 COMMAND ./AutoTest 5 4 Fine large_0.case)
set_tests_properties(FineTreeLarge0_ord5 PROPERTIES RUN_SERIAL TRUE LABELS "FineLock")

add_test(NAME FineTreeLarge0_ord6 COMMAND ./AutoTest 6 4 Fine large_0.case)
set_tests_properties(FineTreeLarge0_ord6 PROPERTIES RUN_SERIAL TRUE LABELS "FineLock")


# FreeLock Tree
add_test(NAME FreeTreeSmall0_ord3 COMMAND ./AutoTest 3 1 Free small_0.case)
set_tests_properties(FreeTreeSmall0_ord3 PROPERTIES RUN_SERIAL TRUE LABELS "FreeLock")

add_test(NAME FreeTreeSmall0_ord4 COMMAND ./AutoTest 4 1 Free small_0.case)
set_tests_properties(FreeTreeSmall0_ord4 PROPERTIES RUN_SERIAL TRUE LABELS "FreeLock")

add_test(NAME FreeTreeSmall0_ord5 COMMAND ./AutoTest 5 1 Free small_0.case)
set_tests_properties(FreeTreeSmall0_ord5 PROPERTIES RUN_SERIAL TRUE LABELS "FreeLock")

add_test(NAME FreeTreeSmall0_ord6 COMMAND ./AutoTest 6 1 Free small_0.case)
set_tests_properties(FreeTreeSmall0_ord6 PROPERTIES RUN_SERIAL TRUE LABELS "FreeLock")

add_test(NAME FreeTreeLarge0_ord3 COMMAND ./AutoTest 3 1 Free large_0.case)
set_tests_properties(FreeTreeLarge0_ord3 PROPERTIES RUN_SERIAL TRUE LABELS "FreeLock")

add_test(NAME FreeTreeLarge0_ord4 COMMAND ./AutoTest 4 1 Free large_0.case)
set_tests_properties(FreeTreeLarge0_ord4 PROPERTIES RUN_SERIAL TRUE LABELS "FreeLock")

add_test(NAME FreeTreeLarge0_ord5 COMMAND ./AutoTest 5 1 Free large_0.case)
set_tests_properties(FreeTreeLarge0_ord5 PROPERTIES RUN_SERIAL TRUE LABELS "FreeLock")

add_test(NAME FreeTreeLarge0_ord6 COMMAND ./AutoTest 6 1 Free large_0.case)
set_tests_properties(FreeTreeLarge0_ord6 PROPERTIES RUN_SERIAL TRUE LABELS "FreeLock")

add_test(NAME FreeTreeLarge0_ord7 COMMAND ./AutoTest 7 1 Free large_0.case)
set_tests_properties(FreeTreeLarge0_ord7 PROPERTIES RUN_SERIAL TRUE LABELS "FreeLock")

add_test(NAME FreeTreeLarge0_ord8 COMMAND ./AutoTest 8 1 Free large_0.case)
set_tests_properties(FreeTreeLarge0_ord8 PROPERTIES RUN_SERIAL TRUE LABELS "FreeLock")
