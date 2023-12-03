# Minimum required CMake version
cmake_minimum_required(VERSION 3.10)

# Project name
project(MyCppProject)

# Specify the C++ standard (e.g., C++17)
set(CMAKE_CXX_STANDARD 17)
set(ENV{TSAN_OPTIONS} "second_deadlock_stack=1")

# Define source files
set(SOURCES src/main.cpp)

# Define header files
set(HEADERS
    includes/timing.h
    includes/tree.h
    includes/freetree.hpp
    includes/freetree/scheduler.hpp
    includes/freetree/worker.hpp
    includes/freetree/background.hpp
    includes/seqTree.hpp
    includes/coarseTree.hpp
    includes/fineTree.hpp
    includes/node.hpp
    includes/lockNode.hpp
    includes/engine.hpp
)


# Test cases
set(UNIT_TEST_SRC src/unitTest.cpp)

# Test engine
set(ENGINE_TEST_SRC src/testEngine.cpp)


# Include the 'includes' directory for header files
include_directories(${CMAKE_SOURCE_DIR}/includes)

# Debug build
# add_executable(BtreeDebug ${HEADERS} ${SOURCES})
# # Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
# target_compile_options(BtreeDebug PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer -g)
# set_target_properties(BtreeDebug PROPERTIES LINK_FLAGS "-fsanitize=address,undefined,leak")

# Release build
# add_executable(BtreeRelease ${HEADERS} ${SOURCES})
# target_compile_options(BtreeRelease PRIVATE -O3)


# Unit test build
add_executable(BtreeUnitTest ${HEADERS} ${UNIT_TEST_SRC})
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
target_compile_options(BtreeUnitTest PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer -g)
set_target_properties(BtreeUnitTest PROPERTIES LINK_FLAGS "-fsanitize=address,undefined")


# Test engine build
add_executable(BtreeTestEngine ${HEADERS} ${ENGINE_TEST_SRC})
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
target_compile_options(BtreeTestEngine PRIVATE -fsanitize=undefined,address -fno-omit-frame-pointer -g)
set_target_properties(BtreeTestEngine PROPERTIES LINK_FLAGS "-fsanitize=undefined,address")


# PalmTree Test build
add_executable(PalmTreeDebug ${HEADERS} ${SOURCES})
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
target_compile_options(PalmTreeDebug PRIVATE -fsanitize=undefined,thread -fno-omit-frame-pointer -g -DDEBUG)
set_target_properties(PalmTreeDebug PROPERTIES LINK_FLAGS "-fsanitize=undefined,thread")

# PalmTree Test Release build
add_executable(PalmTreeRelease ${HEADERS} ${SOURCES})
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
target_compile_options(PalmTreeRelease PRIVATE -fno-omit-frame-pointer)


# Packages
find_package(Threads REQUIRED)
target_link_libraries(BtreeTestEngine PRIVATE Threads::Threads)
target_link_libraries(PalmTreeDebug PRIVATE Threads::Threads)
target_link_libraries(PalmTreeRelease PRIVATE Threads::Threads)

find_package(Boost REQUIRED)
include_directories(${Boost_INCLUDE_DIRS})
target_link_libraries(PalmTreeDebug PRIVATE ${Boost_LIBRARIES})
target_link_libraries(PalmTreeRelease PRIVATE ${Boost_LIBRARIES})

add_definitions(-DASAN_OPTIONS=detect_leaks=0)
