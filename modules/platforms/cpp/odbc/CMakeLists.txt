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

project(ignite-odbc)

set(TARGET ${PROJECT_NAME})

find_package(ODBC REQUIRED)

include_directories(include ${ODBC_INCLUDE_DIRS})

set(SOURCES src/app/application_data_buffer.cpp
        src/app/parameter.cpp
        src/app/parameter_set.cpp
        src/common_types.cpp
        src/config/config_tools.cpp
        src/config/configuration.cpp
        src/config/connection_info.cpp
        src/config/connection_string_parser.cpp
        src/connection.cpp
        src/cursor.cpp
        src/diagnostic/diagnosable_adapter.cpp
        src/diagnostic/diagnostic_record.cpp
        src/diagnostic/diagnostic_record_storage.cpp
        src/environment.cpp
        src/meta/column_meta.cpp
        src/meta/table_meta.cpp
        src/odbc.cpp
        src/entry_points.cpp
        src/dsn_config.cpp
        src/query/column_metadata_query.cpp
        src/query/data_query.cpp
        src/query/batch_query.cpp
        src/query/foreign_keys_query.cpp
        src/query/primary_keys_query.cpp
        src/query/table_metadata_query.cpp
        src/query/type_info_query.cpp
        src/query/special_columns_query.cpp
        src/query/streaming_query.cpp
        src/sql/sql_parser.cpp
        src/sql/sql_lexer.cpp
        src/sql/sql_set_streaming_command.cpp
        src/sql/sql_utils.cpp
        src/streaming/streaming_batch.cpp
        src/streaming/streaming_context.cpp
        src/ssl_mode.cpp
        src/protocol_version.cpp
        src/result_page.cpp
        src/row.cpp
        src/nested_tx_mode.cpp
        src/message.cpp
        src/column.cpp
        src/statement.cpp
        src/type_traits.cpp
        src/utility.cpp
        src/log.cpp)

if (WIN32)
    include_directories(os/win/include)

    list(APPEND SOURCES os/win/src/system_dsn.cpp
            os/win/src/system/ui/custom_window.cpp
            os/win/src/system/ui/dsn_configuration_window.cpp
            os/win/src/system/ui/window.cpp
            project/vs/module.def)
endif ()

add_library(${TARGET} SHARED ${SOURCES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})

target_link_libraries(${TARGET} ${ODBC_LIBRARIES})

if (WIN32)
    target_link_libraries(${TARGET} ignite-common-objlib ignite-binary-objlib ignite-network-objlib odbccp32 shlwapi)

    remove_definitions(-DUNICODE=1)

    add_definitions(-DTARGET_MODULE_FULL_NAME="${TARGET}")

    if (MSVC_VERSION GREATER_EQUAL 1900)
        target_link_libraries(${TARGET} legacy_stdio_definitions)
    endif()
				
	set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME "ignite.odbc")
else()
    target_link_libraries(${TARGET} ignite-common ignite-binary ignite-network)
endif()

target_include_directories(${TARGET} INTERFACE include)

install(TARGETS ${TARGET} LIBRARY DESTINATION lib)
