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

project(ignite-binary)

include_directories(include)

set(TARGET ${PROJECT_NAME})

set(SOURCES src/binary/binary_containers.cpp
        src/binary/binary_raw_writer.cpp
        src/binary/binary_writer.cpp
        src/binary/binary_reader.cpp
        src/binary/binary_type.cpp
        src/binary/binary_raw_reader.cpp
        src/impl/binary/binary_type_manager.cpp
        src/impl/binary/binary_type_impl.cpp
        src/impl/binary/binary_utils.cpp
        src/impl/binary/binary_reader_impl.cpp
        src/impl/binary/binary_type_handler.cpp
        src/impl/binary/binary_writer_impl.cpp
        src/impl/binary/binary_schema.cpp
        src/impl/binary/binary_type_snapshot.cpp
        src/impl/binary/binary_object_header.cpp
        src/impl/binary/binary_object_impl.cpp
        src/impl/binary/binary_field_meta.cpp
        src/impl/interop/interop_memory.cpp
        src/impl/interop/interop_output_stream.cpp
        src/impl/interop/interop_input_stream.cpp)

add_library(${TARGET} SHARED ${SOURCES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})

target_link_libraries(${TARGET} ignite-common)

target_include_directories(${TARGET} INTERFACE include)

install(TARGETS ${TARGET} LIBRARY DESTINATION lib)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_PREFIX}/include FILES_MATCHING PATTERN "*.h*")
