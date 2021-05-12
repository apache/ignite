#
# Copyright 2019 GridGain Systems, Inc. and Contributors.
#
# Licensed under the GridGain Community Edition License (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

project(cluster-compute-example)

set(TARGET ignite-${PROJECT_NAME})

find_package(Ignite)
find_package(Threads)
find_package(Java 1.8 REQUIRED)
find_package(JNI REQUIRED)

include_directories(SYSTEM ${IGNITE_INCLUDE_DIR} ${JNI_INCLUDE_DIRS})
include_directories(../include)

add_executable(${TARGET} src/cluster_compute_example.cpp)

target_link_libraries(${TARGET} ${IGNITE_LIB} ${IGNITE_BINARY_LIB} ${IGNITE_COMMON_LIB} ${CMAKE_THREAD_LIBS_INIT})
