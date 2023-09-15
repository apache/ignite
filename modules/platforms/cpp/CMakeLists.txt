#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

cmake_minimum_required(VERSION 3.6)
project(Ignite.C++ VERSION 2.16.0.23164)

set(CMAKE_CXX_STANDARD 98)

set(CMAKE_PROJECT_VERSION ${PROJECT_VERSION})
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DIGNITE_IMPL -DIGNITE_FRIEND -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS")

set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

list(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)

if("${isSystemDir}" STREQUAL "-1")
    set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
endif("${isSystemDir}" STREQUAL "-1")

if (WIN32)
    add_definitions(-DUNICODE=1)

    set (CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})

    set (CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR})
endif()

if (MSVC)
    add_compile_options(/source-charset:utf-8 /execution-charset:utf-8)

    add_definitions(-D_CRT_SECURE_NO_WARNINGS -D_SCL_SECURE_NO_WARNINGS)
endif()

option (WITH_CORE "Build Ignite.C++ Core module" ON)
option (WITH_ODBC "Build Ignite.C++ ODBC driver module" OFF)
option (WITH_ODBC_MSI "Build Ignite.C++ ODBC driver installer for Windows" OFF)
option (WITH_THIN_CLIENT "Build Ignite.C++ Thin Client module" OFF)
option (WITH_TESTS "Build Ignite.C++ tests" OFF)
option (WARNINGS_AS_ERRORS "Treat warning as errors" OFF)
option (WITH_SANITIZERS "Build with sanitizers" OFF)

if (${WITH_SANITIZERS} AND (CMAKE_CXX_COMPILER_ID MATCHES "Clang" OR CMAKE_CXX_COMPILER_ID MATCHES "GNU"))
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fsanitize=undefined -fno-omit-frame-pointer -fno-sanitize-recover=all -g")

    if (DEFINED ENV{ASAN_OPTIONS})
        list(APPEND SANITIZERS_ENV "ASAN_OPTIONS=$ENV{ASAN_OPTIONS}")
    else()
        list(APPEND SANITIZERS_ENV "ASAN_OPTIONS=handle_segv=0:detect_leaks=0")
    endif()

    if (DEFINED ENV{UBSAN_OPTIONS})
        list(APPEND SANITIZERS_ENV "UBSAN_OPTIONS=$ENV{UBSAN_OPTIONS}")
    else()
        list(APPEND SANITIZERS_ENV "UBSAN_OPTIONS=print_stacktrace=1")
    endif()

    message("Built with sanitizers. Sanitizers options for ctest: ${SANITIZERS_ENV}")
endif()

if (CMAKE_BUILD_TYPE STREQUAL "Debug")
    if (CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fstandalone-debug")
    endif()
endif()

if (${WITH_CORE} OR ${WITH_TESTS})
    find_package(Java 1.8 REQUIRED)
    find_package(JNI REQUIRED)
endif()

if (${WARNINGS_AS_ERRORS})
    if (MSVC)
        add_compile_options(/WX)
    else()
        add_compile_options(-Wall -Wextra -Werror -Wno-variadic-macros)
    endif()
endif()

add_subdirectory(common)
add_subdirectory(binary)

if (${WITH_CORE} OR ${WITH_TESTS})
    add_subdirectory(core)
    add_subdirectory(ignite)
endif()

if (${WITH_TESTS})
    enable_testing()

    if (EXISTS ${CMAKE_SOURCE_DIR}/core-test)
        add_subdirectory(core-test)
    endif()
endif()

if (${WITH_THIN_CLIENT} OR ${WITH_ODBC})
    add_subdirectory(network)
endif()

if (${WITH_THIN_CLIENT})
    add_subdirectory(thin-client)

    if (${WITH_TESTS} AND EXISTS ${CMAKE_SOURCE_DIR}/thin-client-test)
        add_subdirectory(thin-client-test)
    endif()
endif()

if (${WITH_ODBC})
    add_subdirectory(odbc)

    if (${WITH_TESTS} AND EXISTS ${CMAKE_SOURCE_DIR}/odbc-test)
        add_subdirectory(odbc-test)
    endif()
endif()
