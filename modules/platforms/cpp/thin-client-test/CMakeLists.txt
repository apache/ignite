project(ignite-thin-client-tests)

set(TARGET ${PROJECT_NAME})

find_package(Boost 1.56 REQUIRED COMPONENTS unit_test_framework chrono thread)

include_directories(include ${Boost_INCLUDE_DIRS} ${JNI_INCLUDE_DIRS})

set(SOURCES src/teamcity/teamcity_boost.cpp
        src/teamcity/teamcity_messages.cpp
        src/cache_client_test.cpp
        src/test_utils.cpp
        src/ignite_client_test.cpp
        src/auth_test.cpp
        src/ssl_test.cpp)

add_executable(${TARGET} ${SOURCES})

target_link_libraries(${TARGET} ignite-thin-client ignite ${Boost_LIBRARIES})

if (NOT WIN32)
    target_link_libraries(${TARGET} -rdynamic)
endif()

set(TEST_TARGET IgniteThinClientTest)

add_test(NAME ${TEST_TARGET} COMMAND ${TARGET} -s false -l all)

set_tests_properties(${TEST_TARGET} PROPERTIES ENVIRONMENT
        IGNITE_NATIVE_TEST_CPP_THIN_CONFIG_PATH=${PROJECT_SOURCE_DIR}/config)
