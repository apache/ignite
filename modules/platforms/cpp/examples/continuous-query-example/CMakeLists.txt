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

project(continuous-query-example)

set(TARGET ignite-${PROJECT_NAME})

find_package(Ignite)
find_package(Threads)
find_package(Java 1.8 REQUIRED)
find_package(JNI REQUIRED)

include_directories(SYSTEM ${IGNITE_INCLUDE_DIR} ${JNI_INCLUDE_DIRS})
include_directories(../include)

add_executable(${TARGET} src/continuous_query_example.cpp)

target_link_libraries(${TARGET} ${IGNITE_LIB} ${IGNITE_BINARY_LIB} ${IGNITE_COMMON_LIB} ${CMAKE_THREAD_LIBS_INIT})
