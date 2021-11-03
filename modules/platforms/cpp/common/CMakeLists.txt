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

project(ignite-common)

include_directories(include)

set(TARGET ${PROJECT_NAME})

set(SOURCES src/common/big_integer.cpp
        src/common/bits.cpp
        src/common/concurrent.cpp
        src/common/decimal.cpp
        src/common/utils.cpp
        src/date.cpp
        src/ignite_error.cpp
        src/guid.cpp
        src/timestamp.cpp
        src/time.cpp)

if (WIN32)
    set(OS_INCLUDE os/win/include)

    list(APPEND SOURCES os/win/src/common/concurrent_os.cpp
            os/win/src/common/platform_utils.cpp
            os/win/src/common/dynamic_load_os.cpp)
else()
    set(OS_INCLUDE os/linux/include)

    list(APPEND SOURCES os/linux/src/common/concurrent_os.cpp
            os/linux/src/common/platform_utils.cpp
            os/linux/src/common/dynamic_load_os.cpp)
endif()

include_directories(${OS_INCLUDE})

list(APPEND _target_libs ${TARGET})

if (WIN32)
    add_library(${TARGET}-objlib OBJECT ${SOURCES})

    add_library(${TARGET} SHARED $<TARGET_OBJECTS:${TARGET}-objlib>)

    set_target_properties(${TARGET} PROPERTIES OUTPUT_NAME "ignite.common")

    list(APPEND _target_libs ${TARGET}-objlib)
else()
    add_library(${TARGET} SHARED ${SOURCES})
endif()

foreach(_target_lib IN LISTS _target_libs)
    set_target_properties(${_target_lib} PROPERTIES VERSION ${CMAKE_PROJECT_VERSION})

    if (${_target_lib} STREQUAL ${TARGET}-objlib)
        set_target_properties(${_target_lib} PROPERTIES POSITION_INDEPENDENT_CODE 1)
    endif()

    if (NOT WIN32)
        find_package(Threads REQUIRED)

        target_link_libraries(${_target_lib} ${CMAKE_THREAD_LIBS_INIT} ${CMAKE_DL_LIBS})
    endif()

    target_include_directories(${_target_lib} INTERFACE include ${OS_INCLUDE})
endforeach()
unset(_target_libs)

install(TARGETS ${TARGET}
    RUNTIME DESTINATION bin
    ARCHIVE DESTINATION lib
    LIBRARY DESTINATION lib
)
install(DIRECTORY include/ ${OS_INCLUDE}/ DESTINATION ${CMAKE_INSTALL_PREFIX}/include FILES_MATCHING PATTERN "*.h*")
