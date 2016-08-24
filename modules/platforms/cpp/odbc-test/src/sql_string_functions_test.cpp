/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define _USE_MATH_DEFINES

#ifdef _WIN32
#   include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <cmath>

#include <vector>
#include <string>

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/impl/binary/binary_utils.h"

#include "test_type.h"
#include "test_utils.h"

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;

using namespace boost::unit_test;

using ignite::impl::binary::BinaryUtils;

/**
 * Test setup fixture.
 */
struct SqlStringFunctionTestSuiteFixture
{
    /**
     * Constructor.
     */
    SqlStringFunctionTestSuiteFixture() :
        testCache(0),
        env(NULL),
        dbc(NULL),
        stmt(NULL)
    {
        IgniteConfiguration cfg;

        cfg.jvmOpts.push_back("-Xdebug");
        cfg.jvmOpts.push_back("-Xnoagent");
        cfg.jvmOpts.push_back("-Djava.compiler=NONE");
        cfg.jvmOpts.push_back("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
        cfg.jvmOpts.push_back("-XX:+HeapDumpOnOutOfMemoryError");

#ifdef IGNITE_TESTS_32
        cfg.jvmInitMem = 256;
        cfg.jvmMaxMem = 768;
#else
        cfg.jvmInitMem = 1024;
        cfg.jvmMaxMem = 4096;
#endif

        char* cfgPath = getenv("IGNITE_NATIVE_TEST_ODBC_CONFIG_PATH");

        BOOST_REQUIRE(cfgPath != 0);

        cfg.springCfgPath.assign(cfgPath).append("/queries-test.xml");

        IgniteError err;

        grid = Ignition::Start(cfg, &err);

        if (err.GetCode() != IgniteError::IGNITE_SUCCESS)
            BOOST_FAIL(err.GetText());

        testCache = grid.GetCache<int64_t, TestType>("cache");

        // Allocate an environment handle
        SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

        BOOST_REQUIRE(env != NULL);

        // We want ODBC 3 support
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

        // Allocate a connection handle
        SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

        BOOST_REQUIRE(dbc != NULL);

        // Connect string
        SQLCHAR connectStr[] = "DRIVER={Apache Ignite};SERVER=localhost;PORT=10800;CACHE=cache";

        SQLCHAR outstr[ODBC_BUFFER_SIZE];
        SQLSMALLINT outstrlen;

        // Connecting to ODBC server.
        SQLRETURN ret = SQLDriverConnect(dbc, NULL, connectStr, static_cast<SQLSMALLINT>(sizeof(connectStr)),
            outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

        if (!SQL_SUCCEEDED(ret))
        {
            Ignition::Stop(grid.GetName(), true);

            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_DBC, dbc));
        }

        // Allocate a statement handle
        SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

        BOOST_REQUIRE(stmt != NULL);
    }

    /**
     * Destructor.
     */
    ~SqlStringFunctionTestSuiteFixture()
    {
        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // Disconneting from the server.
        SQLDisconnect(dbc);

        // Releasing allocated handles.
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);

        Ignition::Stop(grid.GetName(), true);
    }

    void CheckSingleResult0(const char* request, SQLSMALLINT type, void* column, SQLLEN bufSize, SQLLEN* resSize)
    {
        SQLRETURN ret;

        ret = SQLBindCol(stmt, 1, type, column, bufSize, resSize);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLExecDirect(stmt, reinterpret_cast<SQLCHAR*>(const_cast<char*>(request)), SQL_NTS);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFetch(stmt);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFetch(stmt);
        BOOST_CHECK(ret == SQL_NO_DATA);
    }

    /**
     * Run query returning single result and check it to be equal to expected.
     *
     * @param request SQL request.
     * @param expected Expected result.
     */
    template<typename T>
    void CheckSingleResult(const char* request, const T& expected)
    {
        BOOST_FAIL("Function is not defined for the type.");
    }
    
    /**
     * Run query returning single result and check it to be equal to expected.
     *
     * @param request SQL request.
     * @param expected Expected result.
     * @param type Result type.
     */
    template<typename T>
    void CheckSingleResultNum0(const char* request, const T& expected, SQLSMALLINT type)
    {
        T res = 0;

        CheckSingleResult0(request, type, &res, 0, 0);

        BOOST_CHECK_EQUAL(res, expected);
    }


    /** Node started during the test. */
    Ignite grid;

    /** Test cache instance. */
    Cache<int64_t, TestType> testCache;

    /** ODBC Environment. */
    SQLHENV env;

    /** ODBC Connect. */
    SQLHDBC dbc;

    /** ODBC Statement. */
    SQLHSTMT stmt;
};

template<>
void SqlStringFunctionTestSuiteFixture::CheckSingleResult<std::string>(const char* request, const std::string& expected)
{
    SQLCHAR res[ODBC_BUFFER_SIZE] = { 0 };
    SQLLEN resLen = 0;

    CheckSingleResult0(request, SQL_C_CHAR, res, ODBC_BUFFER_SIZE, &resLen);

    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(res), static_cast<size_t>(resLen)), expected);
}

template<>
void SqlStringFunctionTestSuiteFixture::CheckSingleResult<int64_t>(const char* request, const int64_t& expected)
{
    CheckSingleResultNum0<int64_t>(request, expected, SQL_C_SBIGINT);
}

template<>
void SqlStringFunctionTestSuiteFixture::CheckSingleResult<int32_t>(const char* request, const int32_t& expected)
{
    CheckSingleResultNum0<int32_t>(request, expected, SQL_C_SLONG);
}

template<>
void SqlStringFunctionTestSuiteFixture::CheckSingleResult<int16_t>(const char* request, const int16_t& expected)
{
    CheckSingleResultNum0<int16_t>(request, expected, SQL_C_SSHORT);
}

template<>
void SqlStringFunctionTestSuiteFixture::CheckSingleResult<int8_t>(const char* request, const int8_t& expected)
{
    CheckSingleResultNum0<int8_t>(request, expected, SQL_C_STINYINT);
}

template<>
void SqlStringFunctionTestSuiteFixture::CheckSingleResult<float>(const char* request, const float& expected)
{
    SQLFLOAT res = 0;

    CheckSingleResult0(request, SQL_C_FLOAT, &res, 0, 0);

    BOOST_CHECK_CLOSE(static_cast<float>(res), expected, 1E-6f);
}

template<>
void SqlStringFunctionTestSuiteFixture::CheckSingleResult<double>(const char* request, const double& expected)
{
    SQLDOUBLE res = 0;

    CheckSingleResult0(request, SQL_C_DOUBLE, &res, 0, 0);

    BOOST_CHECK_CLOSE(static_cast<double>(res), expected, 1E-6);
}

BOOST_FIXTURE_TEST_SUITE(SqlStringFunctionTestSuite, SqlStringFunctionTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestStringFunctionAscii)
{
    TestType in;

    in.strField = "Hi";

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn ASCII(strField)} FROM TestType", static_cast<int32_t>('H'));
}

BOOST_AUTO_TEST_CASE(TestStringFunctionBitLength)
{
    TestType in;
    in.strField = "Lorem ipsum dolor";

    testCache.Put(1, in);

    CheckSingleResult<int64_t>("SELECT {fn BIT_LENGTH(strField)} FROM TestType", in.strField.size() * 16);
}

BOOST_AUTO_TEST_CASE(TestStringFunctionChar)
{
    TestType in;

    in.i32Field = static_cast<int32_t>('H');

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn CHAR(i32Field)} FROM TestType", "H");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionCharLength)
{
    TestType in;
    in.strField = "Lorem ipsum dolor";

    testCache.Put(1, in);

    CheckSingleResult<int64_t>("SELECT {fn CHAR_LENGTH(strField)} FROM TestType", in.strField.size());
}

BOOST_AUTO_TEST_CASE(TestStringFunctionCharacterLength)
{
    TestType in;
    in.strField = "Lorem ipsum dolor";

    testCache.Put(1, in);

    CheckSingleResult<int64_t>("SELECT {fn CHARACTER_LENGTH(strField)} FROM TestType", in.strField.size());
}

BOOST_AUTO_TEST_CASE(TestStringFunctionConcat)
{
    TestType in;
    in.strField = "Lorem ipsum dolor sit amet,";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn CONCAT(strField, \' consectetur adipiscing elit\')} FROM TestType",
        in.strField + " consectetur adipiscing elit");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionDifference)
{
    TestType in;
    in.strField = "Hello";

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn DIFFERENCE(strField, \'Hola!\')} FROM TestType", 4);
}

BOOST_AUTO_TEST_CASE(TestStringFunctionInsert)
{
    TestType in;
    in.strField = "Hello World!";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn INSERT(strField, 7, 5, \'Ignite\')} FROM TestType", "Hello Ignite!");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionLcase)
{
    TestType in;
    in.strField = "Hello World!";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn LCASE(strField)} FROM TestType", "hello world!");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionLeft)
{
    TestType in;
    in.strField = "Hello World!";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn LEFT(strField, 5)} FROM TestType", "Hello");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionLength)
{
    TestType in;
    in.strField = "Lorem ipsum dolor sit amet, consectetur adipiscing elit";

    testCache.Put(1, in);

    CheckSingleResult<int64_t>("SELECT {fn LENGTH(strField)} FROM TestType", in.strField.size());
}

BOOST_AUTO_TEST_CASE(TestStringFunctionLocate)
{
    TestType in;
    in.strField = "Lorem ipsum dolor sit amet, consectetur adipiscing elit";

    testCache.Put(1, in);

    CheckSingleResult<int64_t>("SELECT {fn LOCATE(\'ip\', strField)} FROM TestType", 7);
}

BOOST_AUTO_TEST_CASE(TestStringFunctionLocate2)
{
    TestType in;
    in.strField = "Lorem ipsum dolor sit amet, consectetur adipiscing elit";

    testCache.Put(1, in);

    CheckSingleResult<int64_t>("SELECT {fn LOCATE(\'ip\', strField, 10)} FROM TestType", 43);
}

BOOST_AUTO_TEST_CASE(TestStringFunctionLtrim)
{
    TestType in;
    in.strField = "    Lorem ipsum  ";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn LTRIM(strField)} FROM TestType", "Lorem ipsum  ");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionOctetLength)
{
    TestType in;
    in.strField = "Lorem ipsum dolor sit amet, consectetur adipiscing elit";

    testCache.Put(1, in);

    CheckSingleResult<int64_t>("SELECT {fn OCTET_LENGTH(strField)} FROM TestType", in.strField.size() * 2);
}

BOOST_AUTO_TEST_CASE(TestStringFunctionPosition)
{
    TestType in;
    in.strField = "Lorem ipsum dolor sit amet, consectetur adipiscing elit";

    testCache.Put(1, in);

    CheckSingleResult<int64_t>("SELECT {fn POSITION(\'sit\', strField)} FROM TestType", 19);
}

BOOST_AUTO_TEST_CASE(TestStringFunctionRepeat)
{
    TestType in;
    in.strField = "Test";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn REPEAT(strField,4)} FROM TestType", "TestTestTestTest");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionReplace)
{
    TestType in;
    in.strField = "Hello Ignite!";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn REPLACE(strField, \'Ignite\', \'World\')} FROM TestType", "Hello World!");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionRight)
{
    TestType in;
    in.strField = "Hello World!";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn RIGHT(strField, 6)} FROM TestType", "World!");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionRtrim)
{
    TestType in;
    in.strField = "    Lorem ipsum  ";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn RTRIM(strField)} FROM TestType", "    Lorem ipsum");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionSoundex)
{
    TestType in;
    in.strField = "Hello Ignite!";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn SOUNDEX(strField)} FROM TestType", "H425");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionSpace)
{
    CheckSingleResult<std::string>("SELECT {fn SPACE(10)}", "          ");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionSubstring)
{
    TestType in;
    in.strField = "Hello Ignite!";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn SUBSTRING(strField, 7, 6)} FROM TestType", "Ignite");
}

BOOST_AUTO_TEST_CASE(TestStringFunctionUcase)
{
    TestType in;
    in.strField = "Hello World!";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn UCASE(strField)} FROM TestType", "HELLO WORLD!");
}

BOOST_AUTO_TEST_SUITE_END()
