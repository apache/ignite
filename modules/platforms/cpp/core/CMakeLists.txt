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

project(ignite)

find_library(JVM_LIBRARY jvm ${JAVA_JVM_LIBRARY_DIRECTORIES})

include_directories(SYSTEM ${JNI_INCLUDE_DIRS})
include_directories(include)

set(TARGET ${PROJECT_NAME})

set(SOURCES src/ignite.cpp
        src/jni/java.cpp
        src/ignition.cpp
        src/impl/ignite_environment.cpp
        src/impl/binary/binary_type_updater_impl.cpp
        src/impl/handle_registry.cpp
        src/impl/cache/query/continuous/continuous_query_handle_impl.cpp
        src/impl/cache/query/query_impl.cpp
        src/impl/cache/cache_impl.cpp
        src/impl/cache/query/query_batch.cpp
        src/impl/interop/interop_external_memory.cpp
        src/impl/interop/interop_target.cpp
        src/impl/transactions/transaction_impl.cpp
        src/impl/transactions/transactions_impl.cpp
        src/impl/cluster/cluster_group_impl.cpp
        src/impl/compute/cancelable_impl.cpp
        src/impl/compute/compute_impl.cpp
        src/impl/ignite_impl.cpp
        src/impl/ignite_binding_impl.cpp
        src/transactions/transaction.cpp
        src/transactions/transactions.cpp
        src/cluster/cluster_node.cpp
        src/cluster/cluster_group.cpp
        src/cluster/ignite_cluster.cpp
        src/impl/cluster/ignite_cluster_impl.cpp
        src/impl/cluster/cluster_node_impl.cpp
        src/impl/cache/cache_affinity_impl.cpp)

if (WIN32)
    list(APPEND SOURCES src/jni/os/win/utils.cpp)
else()
    list(APPEND SOURCES src/jni/os/linux/utils.cpp)
endif()

add_library(${TARGET} SHARED ${SOURCES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})

target_link_libraries(${TARGET} ignite-binary ${JVM_LIBRARY})

if (WIN32)
    target_link_libraries(${TARGET} delayimp)

    set_target_properties(${TARGET} PROPERTIES LINK_FLAGS "/DELAYLOAD:jvm.dll")

    set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME "ignite.core")
endif()

target_include_directories(${TARGET} INTERFACE include)

install(TARGETS ${TARGET} 
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_PREFIX}/include FILES_MATCHING PATTERN "*.h*")
