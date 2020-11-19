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

project(ignite-jni)

set(TARGET ${PROJECT_NAME})

find_library(JVM_LIBRARY jvm ${JAVA_JVM_LIBRARY_DIRECTORIES})

include_directories(${JNI_INCLUDE_DIRS} include)

set(SOURCES src/java.cpp src/exports.cpp)

if (WIN32)
    list(APPEND SOURCES os/win/src/utils.cpp)
else()
    list(APPEND SOURCES os/linux/src/utils.cpp)
endif()

add_library(${TARGET} SHARED ${SOURCES})

set_target_properties(${TARGET} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})

target_link_libraries(${TARGET} ignite-common ${JVM_LIBRARY})

if (WIN32)
    target_link_libraries(${TARGET} delayimp)

    set_target_properties(${TARGET} PROPERTIES LINK_FLAGS "/DELAYLOAD:jvm.dll")
	
	set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME "ignite.jni")
endif()

target_include_directories(${TARGET} INTERFACE include)

install(TARGETS ${TARGET} LIBRARY DESTINATION lib)
install(DIRECTORY include/ DESTINATION ${CMAKE_INSTALL_PREFIX}/include FILES_MATCHING PATTERN "*.h*")
