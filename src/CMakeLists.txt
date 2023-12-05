# Minimum required CMake version
cmake_minimum_required(VERSION 3.10)

# Project name
project(BForest)

# Specify the C++ standard (e.g., C++17)
set(CMAKE_CXX_STANDARD 17)
set(ENV{TSAN_OPTIONS} "second_deadlock_stack=1")

# Get required packages
find_package(Threads REQUIRED)
find_package(Boost REQUIRED)
find_package(MPI REQUIRED)

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
)

# Include the 'includes' directory for header files
include_directories(${Boost_INCLUDE_DIRS})
include_directories(${MPI_INCLUDE_PATH})
include_directories(${CMAKE_SOURCE_DIR}/includes)



################# Sequential Tree ####################
set(UNIT_TEST_SRC src/seqUnitTest.cpp)
# Unit test build
add_executable(seqTestDebug ${HEADERS} ${UNIT_TEST_SRC})
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
target_compile_options(seqTestDebug PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer -g -DDEBUG)
target_link_libraries(seqTestDebug ${MPI_LIBRARIES})
set_target_properties(seqTestDebug PROPERTIES 
    COMPILE_FLAGS "-fsanitize=address,undefined -fno-omit-frame-pointer -g -DDEBUG ${MPI_COMPILE_FLAGS}"
    LINK_FLAGS "-fsanitize=address,undefined ${MPI_LINK_FLAGS}"
)



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
add_executable(fineTestDebug ${HEADERS} ${COARSE_TEST_SRC})
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
set(DISTRI_TREE_SRC src/distriTest.cpp)
# Enable UndefinedBehaviorSanitizer (UBSan)
# No address sanitizer since the OpenMPI itself has memory leak, and will cause a lot of false positive.
add_executable(distributeTreeDebug ${HEADERS} ${DISTRI_TREE_SRC})
target_link_libraries(distributeTreeDebug ${MPI_LIBRARIES})
target_compile_options(distributeTreeDebug PRIVATE -fsanitize=undefined -fno-omit-frame-pointer -g -DDEBUG)
set_target_properties(distributeTreeDebug PROPERTIES 
    COMPILE_FLAGS "-fsanitize=undefined -fno-omit-frame-pointer -g -DDEBUG ${MPI_COMPILE_FLAGS}"
    LINK_FLAGS "-fsanitize=undefined ${MPI_LINK_FLAGS}"
)
