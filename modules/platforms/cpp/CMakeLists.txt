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
project(Ignite.C++ VERSION 2.13.0.58917)

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

option (WITH_CORE ON)
option (WITH_ODBC OFF)
option (WITH_ODBC_MSI OFF)
option (WITH_THIN_CLIENT OFF)
option (WITH_TESTS OFF)
option (WARNINGS_AS_ERRORS OFF)

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
