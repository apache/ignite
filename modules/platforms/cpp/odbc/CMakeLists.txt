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

include_directories(SYSTEM ${ODBC_INCLUDE_DIRS})
include_directories(include)

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
            module.def)
endif ()

add_library(${TARGET} SHARED ${SOURCES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})

target_link_libraries(${TARGET} ${ODBC_LIBRARIES})

if (WIN32)
    target_link_libraries(${TARGET} ignite-common-objlib ignite-binary-objlib ignite-network-objlib odbccp32 shlwapi)

    remove_definitions(-DUNICODE=1)

    add_definitions(-DTARGET_MODULE_FULL_NAME="$<TARGET_FILE_NAME:${TARGET}>")

    if (MSVC_VERSION GREATER_EQUAL 1900)
        target_link_libraries(${TARGET} legacy_stdio_definitions)
    endif()
                
    set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME "ignite.odbc")
else()
    target_link_libraries(${TARGET} ignite-common ignite-binary ignite-network)
endif()

if (WIN32 AND ${WITH_ODBC_MSI})
    find_program(WIX_CANDLE candle)
    if(NOT WIX_CANDLE)
        message(FATAL_ERROR "WIX candle.exe not found! Have you installed WIX Toolset or forgotten to add it to Path?")
    endif()

    find_program(WIX_LIGHT light)
    if(NOT WIX_LIGHT)
        message(FATAL_ERROR "WIX light.exe not found! Have you installed WIX Toolset or forgotten to add it to Path?")
    endif()

    set(WIX_ODBC_LIBRARY_PATH ".\\ignite.odbc.dll")

    set(WIX_PROJECT_NAME "Apache Ignite")
    set(WIX_MANUFACTURER "The Apache Software Foundation")
    set(WIX_COPYRIGHT_COMMENT "Apache, Apache Ignite, the Apache feather and the Apache Ignite logo are trademarks of The Apache Software Foundation.")

    if(CMAKE_SIZEOF_VOID_P EQUAL 8)
        set(WIX_BIT_SUFFIX "64-bit")
        set(WIX_PRODUCT_ID "F3E308E4-910C-4AF5-82DE-2ACF4D64830E")
        set(WIX_UPGRADE_CODE "1D7AEFDF-6CD2-4FB5-88F2-811A89832D6D")
        set(WIX_COMPONENT_ID "E5F0DDF2-DD3C-4196-8A08-70921858A52F")
        set(WIX_PROGRAM_FILES_FOLDER_ID "ProgramFiles64Folder")
        set(WIX_COMPONENT_IS_WIN64 "yes")
        set(WIX_PACKAGE_PLATFORM "x64")
        set(WIX_INSTALLER_PREFIX ${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/ignite-odbc-amd64)
    elseif(CMAKE_SIZEOF_VOID_P EQUAL 4)
        set(WIX_BIT_SUFFIX "32-bit")
        set(WIX_PRODUCT_ID "D39CBABA-1E21-4701-AA5C-91EDA07B383B")
        set(WIX_UPGRADE_CODE "743902A4-365C-424E-B226-5B2898A3941E")
        set(WIX_COMPONENT_ID "4AFA26EE-C639-4EF2-A9B2-281119BB4BB5")
        set(WIX_PROGRAM_FILES_FOLDER_ID "ProgramFilesFolder")
        set(WIX_COMPONENT_IS_WIN64 "no")
        set(WIX_PACKAGE_PLATFORM "x86")
        set(WIX_INSTALLER_PREFIX ${CMAKE_BINARY_DIR}/${CMAKE_BUILD_TYPE}/ignite-odbc-x86)
    endif()

    set(WIX_PRODUCT_NAME "${WIX_PROJECT_NAME} ODBC ${WIX_BIT_SUFFIX} Driver")

    configure_file(
        "${CMAKE_CURRENT_LIST_DIR}/install/ignite-odbc.wxs.in"
        "${WIX_INSTALLER_PREFIX}.wxs"
        @ONLY
    )

    add_custom_command(
        TARGET ${TARGET} POST_BUILD
        COMMAND ${WIX_CANDLE} ${WIX_INSTALLER_PREFIX}.wxs -out ${WIX_INSTALLER_PREFIX}.wxobj
        COMMAND ${WIX_LIGHT} -ext WixUIExtension ${WIX_INSTALLER_PREFIX}.wxobj -out ${WIX_INSTALLER_PREFIX}.msi
    )
endif()

target_include_directories(${TARGET} INTERFACE include)

install(TARGETS ${TARGET} 
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)

if (WIN32 AND ${WITH_ODBC_MSI})
   install(FILES ${WIX_INSTALLER_PREFIX}.msi DESTINATION bin)
endif()
