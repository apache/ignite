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

project(ignite-thin-client-tests)

set(TARGET ${PROJECT_NAME})

if (WIN32)
    set(Boost_USE_STATIC_LIBS ON)
endif()

find_package(Boost 1.53 REQUIRED COMPONENTS unit_test_framework chrono thread system)

include_directories(SYSTEM ${Boost_INCLUDE_DIRS} ${JNI_INCLUDE_DIRS})
include_directories(include)

set(SOURCES src/teamcity/teamcity_boost.cpp
        src/teamcity/teamcity_messages.cpp
        src/cache_client_test.cpp
        src/compute_client_test.cpp
        src/test_utils.cpp
        src/ignite_client_test.cpp
        src/interop_test.cpp
        src/sql_fields_query_test.cpp
        src/auth_test.cpp
        src/tx_test.cpp
        src/ssl_test.cpp
        )

add_executable(${TARGET} ${SOURCES})

target_link_libraries(${TARGET} ignite-thin-client ignite ${Boost_LIBRARIES})

if (NOT WIN32)
    add_definitions(-DBOOST_TEST_DYN_LINK)

    target_link_libraries(${TARGET} -rdynamic)
endif()

set(TEST_TARGET IgniteThinClientTest)

add_test(NAME ${TEST_TARGET} COMMAND ${TARGET} --catch_system_errors=no --log_level=all)

set_tests_properties(${TEST_TARGET} PROPERTIES ENVIRONMENT
        IGNITE_NATIVE_TEST_CPP_THIN_CONFIG_PATH=${PROJECT_SOURCE_DIR}/config)
