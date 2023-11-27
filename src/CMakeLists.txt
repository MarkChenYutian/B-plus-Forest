# Minimum required CMake version
cmake_minimum_required(VERSION 3.10)

# Project name
project(MyCppProject)

# Specify the C++ standard (e.g., C++17)
set(CMAKE_CXX_STANDARD 17)

# Define source files
set(SOURCES
    src/main.cpp
)

# Define header files
set(HEADERS
    includes/tree.h
    includes/seqTree.hpp
    includes/coarseTree.hpp
    includes/node.hpp
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
set_target_properties(BtreeUnitTest PROPERTIES LINK_FLAGS "-fsanitize=address,undefined,leak")


# Test engine build
add_executable(BtreeTestEngine ${HEADERS} ${ENGINE_TEST_SRC})
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
target_compile_options(BtreeTestEngine PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer -g)
set_target_properties(BtreeTestEngine PROPERTIES LINK_FLAGS "-fsanitize=address,undefined,leak")


find_package(Threads REQUIRED)
target_link_libraries(BtreeTestEngine PRIVATE Threads::Threads)