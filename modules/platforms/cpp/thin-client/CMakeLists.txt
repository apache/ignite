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

project(ignite-thin-client)

set(TARGET ${PROJECT_NAME})

include_directories(include src)

set(SOURCES src/impl/data_channel.cpp
        src/impl/utility.cpp
        src/impl/protocol_version.cpp
        src/impl/ignite_node.cpp
        src/impl/ignite_client_impl.cpp
        src/impl/data_router.cpp
        src/impl/affinity/affinity_topology_version.cpp
        src/impl/affinity/affinity_assignment.cpp
        src/impl/affinity/affinity_manager.cpp
        src/impl/remote_type_updater.cpp
        src/impl/message.cpp
        src/impl/cache/cache_client_proxy.cpp
        src/impl/cache/cache_client_impl.cpp
        src/impl/compute/compute_client_impl.cpp
        src/impl/transactions/transaction_impl.cpp
        src/impl/transactions/transactions_impl.cpp
        src/impl/transactions/transactions_proxy.cpp
        src/compute/compute_client.cpp
        src/ignite_client.cpp
        src/cache/query/query_fields_cursor.cpp
        src/cache/query/query_fields_row.cpp)

add_library(${TARGET} SHARED ${SOURCES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})

if (WIN32)
    set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME "ignite.thin-client")
endif()

target_link_libraries(${TARGET} ignite-binary ignite-network)

target_include_directories(${TARGET} INTERFACE include)

install(TARGETS ${TARGET} 
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_PREFIX}/include FILES_MATCHING PATTERN "*.h*")
