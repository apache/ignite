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

cmake_minimum_required(VERSION 3.16)
project(Ignite.C++ VERSION 2.9.0.42453)

set(CMAKE_CXX_STANDARD 98)

find_package(Java 1.8 REQUIRED)
find_package(JNI REQUIRED)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -DIGNITE_IMPL -D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS")

set(CMAKE_SKIP_BUILD_RPATH FALSE)
set(CMAKE_BUILD_WITH_INSTALL_RPATH FALSE)
set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
set(CMAKE_INSTALL_RPATH_USE_LINK_PATH TRUE)

list(FIND CMAKE_PLATFORM_IMPLICIT_LINK_DIRECTORIES "${CMAKE_INSTALL_PREFIX}/lib" isSystemDir)
if("${isSystemDir}" STREQUAL "-1")
    set(CMAKE_INSTALL_RPATH "${CMAKE_INSTALL_PREFIX}/lib")
endif("${isSystemDir}" STREQUAL "-1")

option (WITH_ODBC OFF)
option (WITH_THIN_CLIENT OFF)
option (WITH_TESTS OFF)

add_subdirectory(common)
add_subdirectory(binary)
add_subdirectory(jni)
add_subdirectory(core)
add_subdirectory(ignite)

if (${WITH_TESTS})
    enable_testing()

    add_subdirectory(core-test)
endif()

install(TARGETS ignite-common ignite-binary ignite-jni ignite ignite-bin)

if ((${WITH_THIN_CLIENT}) OR (${WITH_ODBC}))
    add_subdirectory(network)

    install(TARGETS ignite-network)
endif()

if (${WITH_THIN_CLIENT})
    add_subdirectory(thin-client)

    if (${WITH_TESTS})
        add_subdirectory(thin-client-test)
    endif()

    install(TARGETS ignite-thin-client)
endif()

if (${WITH_ODBC})
    add_subdirectory(odbc)

    if (${WITH_TESTS})
        add_subdirectory(odbc-test)
    endif()

    install(TARGETS ignite-odbc)
endif()

get_cmake_property(_variableNames VARIABLES)
list (SORT _variableNames)
foreach (_variableName ${_variableNames})
    message(STATUS "${_variableName}=${${_variableName}}")
endforeach()
