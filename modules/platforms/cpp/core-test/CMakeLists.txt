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

project(ignite-tests)

set(TARGET ${PROJECT_NAME})

if (WIN32)
    set(Boost_USE_STATIC_LIBS ON)
endif()

find_package(Boost 1.53 REQUIRED COMPONENTS unit_test_framework chrono thread system)

include_directories(SYSTEM ${Boost_INCLUDE_DIRS} ${JNI_INCLUDE_DIRS})
include_directories(include)

set(SOURCES src/reference_test.cpp
        src/bits_test.cpp
        src/binary_identity_resolver_test.cpp
        src/cache_test.cpp
        src/cache_query_test.cpp
        src/cache_store_test.cpp
        src/continuous_query_test.cpp
        src/concurrent_test.cpp
        src/compute_test.cpp
        src/compute_java_test.cpp
        src/ignition_test.cpp
        src/interop_memory_test.cpp
        src/interop_test.cpp
        src/cluster_test.cpp
        src/cache_invoke_test.cpp
        src/handle_registry_test.cpp
        src/ignite_error_test.cpp
        src/binary_test_defs.cpp
        src/binary_object_test.cpp
        src/binary_reader_writer_raw_test.cpp
        src/binary_reader_writer_test.cpp
        src/binary_session_test.cpp
        src/date_time_test.cpp
        src/decimal_test.cpp
        src/dynamic_size_array_test.cpp
        src/fixed_size_array_test.cpp
        src/future_test.cpp
        src/transactions_test.cpp
        src/teamcity_messages.cpp
        src/teamcity_boost.cpp
        src/test_utils.cpp
        src/cluster_node_test.cpp
        src/affinity_test.cpp
        src/cluster_group_test.cpp)

add_executable(${TARGET} ${SOURCES})

target_link_libraries(${TARGET} ignite ${Boost_LIBRARIES})

if (NOT WIN32)
    add_definitions(-DBOOST_TEST_DYN_LINK)

    target_link_libraries(${TARGET} -rdynamic)
endif()

set(TEST_TARGET IgniteCoreTest)

add_test(NAME ${TEST_TARGET} COMMAND ${TARGET} --catch_system_errors=no --log_level=all)

set_tests_properties(${TEST_TARGET} PROPERTIES ENVIRONMENT
        IGNITE_NATIVE_TEST_CPP_CONFIG_PATH=${PROJECT_SOURCE_DIR}/config)
