# Minimum required CMake version
cmake_minimum_required(VERSION 3.10)

# Project name
project(MyCppProject)

# Specify the C++ standard (e.g., C++17)
set(CMAKE_CXX_STANDARD 17)

# Define source files
set(SOURCES
    src/tree.cpp
    src/util.cpp
    src/main.cpp
)

# Define header files
set(HEADERS
    includes/tree.h
)


# Test cases
set(TESTSOURCES
    src/tree.cpp
    src/util.cpp
    src/test.cpp
)

set(TESTHEADERS
    includes/tree.h
)

# Include the 'includes' directory for header files
include_directories(${CMAKE_SOURCE_DIR}/includes)

# Debug build
add_executable(BtreeDebug ${HEADERS} ${SOURCES})
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
target_compile_options(BtreeDebug PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer -g)
set_target_properties(BtreeDebug PROPERTIES LINK_FLAGS "-fsanitize=address,undefined,leak")

# Release build
add_executable(BtreeRelease ${HEADERS} ${SOURCES})
target_compile_options(BtreeRelease PRIVATE -O3)


# Unit test build
add_executable(BtreeUnitTest ${TESTHEADERS} ${TESTSOURCES})
# Enable AddressSanitizer (ASan), UndefinedBehaviorSanitizer (UBSan), and LeakSanitizer (LSan)
target_compile_options(BtreeUnitTest PRIVATE -fsanitize=address,undefined -fno-omit-frame-pointer -g)
set_target_properties(BtreeUnitTest PROPERTIES LINK_FLAGS "-fsanitize=address,undefined,leak")
