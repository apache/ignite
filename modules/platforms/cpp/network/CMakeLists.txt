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

project(ignite-network)

set(TARGET ${PROJECT_NAME})

find_package(OpenSSL REQUIRED)

include_directories(include src ${OPENSSL_INCLUDE_DIR})

set(SOURCES src/network/network.cpp
        src/network/ssl/secure_socket_client.cpp
        src/network/ssl/ssl_gateway.cpp)

if (WIN32)
    include_directories(os/win/src)

    list(APPEND SOURCES os/win/src/network/tcp_socket_client.cpp
            os/win/src/network/sockets.cpp
            os/win/src/network/utils.cpp)
else()
    include_directories(os/linux/src)

    list(APPEND SOURCES os/linux/src/network/tcp_socket_client.cpp
            os/linux/src/network/sockets.cpp
            os/linux/src/network/utils.cpp)
endif()

add_library(${TARGET} SHARED ${SOURCES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})

target_link_libraries(${TARGET} ignite-common)

target_include_directories(${TARGET} INTERFACE include)

install(TARGETS ${TARGET} LIBRARY DESTINATION lib)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_PREFIX}/include FILES_MATCHING PATTERN "*.h*")
