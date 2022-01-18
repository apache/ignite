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
project(numa_alloc)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS "-shared -D_FORTIFY_SOURCE=2 -z noexecstack -z,relro -z,now -fPIC -Wformat -Wformat-security -Werror=format-security")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -pie")

find_package(JNI REQUIRED)
find_package(Threads REQUIRED)
include_directories(${CMAKE_THREAD_LIBS_INIT})

find_library(NUMA_LIBRARIES NAMES libnuma.so)
if (NOT NUMA_LIBRARIES)
    message(FATAL_ERROR "not found numa library")
endif (NOT NUMA_LIBRARIES)

include_directories(${JAVA_INCLUDE_PATH})
include_directories(${JAVA_INCLUDE_PATH2})
include_directories(include)

file(GLOB SOURCES
        src/numa/numa_alloc.cpp
        src/org_apache_ignite_internal_mem_NumaAllocUtil.cpp
)
add_library(numa_alloc SHARED ${SOURCES})
message(STATUS "NUMA_LIBRARIES: ${NUMA_LIBRARIES}")
target_link_libraries(numa_alloc PRIVATE -static-libgcc -static-libstdc++ ${NUMA_LIBRARIES})
