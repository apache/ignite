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
project(mmap)

set (CMAKE_CXX_STANDARD 11)
set(CMAKE_BUILD_TYPE Release)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -shared -D_FORTIFY_SOURCE=2 -z noexecstack -z,relro -z,now -Wformat -Wformat-security -Werror=format-security")
set(CMAKE_SHARED_LINKER_FLAGS "${CMAKE_SHARED_LINKER_FLAGS} -pie")

find_package(Java REQUIRED)
find_package(JNI REQUIRED)
find_package(Threads REQUIRED)
include_directories(${CMAKE_THREAD_LIBS_INIT})

find_library(LIBPMEM_LIBRARIES NAMES libpmem.a libpmem.so)
if (NOT LIBPMEM_LIBRARIES)
    message(FATAL_ERROR "not found pmem library")
endif (NOT LIBPMEM_LIBRARIES)

include_directories(${JAVA_INCLUDE_PATH})
include_directories(${JAVA_INCLUDE_PATH2})

file(GLOB SOURCES "*.cpp")
add_library(mmap SHARED ${SOURCES})
target_link_options(mmap PRIVATE -static-libgcc -static-libstdc++)
target_link_libraries(mmap ${LIBPMEM_LIBRARIES})
