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

#ifdef _WIN32
#   include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

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

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;

using namespace boost::unit_test;

using ignite::impl::binary::BinaryUtils;

/** Read buffer size. */
enum { ODBC_BUFFER_SIZE = 1024 };

/**
 * Extract error message.
 *
 * @param handleType Type of the handle.
 * @param handle Handle.
 * @return Error message.
 */
std::string GetOdbcErrorMessage(SQLSMALLINT handleType, SQLHANDLE handle)
{
    SQLCHAR sqlstate[7] = {};
    SQLINTEGER nativeCode;

    SQLCHAR message[ODBC_BUFFER_SIZE];
    SQLSMALLINT reallen = 0;

    SQLGetDiagRec(handleType, handle, 1, sqlstate, &nativeCode, message, ODBC_BUFFER_SIZE, &reallen);

    return std::string(reinterpret_cast<char*>(sqlstate)) + ": " +
        std::string(reinterpret_cast<char*>(message), reallen);
}

/**
 * Test setup fixture.
 */
struct QueriesTestSuiteFixture 
{
    /**
     * Constructor.
     */
    QueriesTestSuiteFixture() : testCache(0), env(NULL), dbc(NULL), stmt(NULL)
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
    ~QueriesTestSuiteFixture()
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

    template<typename T>
    void CheckTwoRowsInt(SQLSMALLINT type)
    {
        SQLRETURN ret;

        TestType in1(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), BinaryUtils::MakeDateGmt(1987, 6, 5), BinaryUtils::MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));
        TestType in2(8, 7, 6, 5, "4", 3.0f, 2.0, false, Guid(1, 0), BinaryUtils::MakeDateGmt(1976, 1, 12), BinaryUtils::MakeTimestampGmt(1978, 8, 21, 23, 13, 45, 456));

        testCache.Put(1, in1);
        testCache.Put(2, in2);

        const size_t columnsCnt = 11;

        T columns[columnsCnt] = { 0 };

        // Binding columns.
        for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        {
            ret = SQLBindCol(stmt, i + 1, type, &columns[i], sizeof(columns[i]), 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
        }

        SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, "
            "floatField, doubleField, boolField, guidField, dateField, timestampField FROM TestType";

        ret = SQLExecDirect(stmt, request, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFetch(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(columns[0], 1);
        BOOST_CHECK_EQUAL(columns[1], 2);
        BOOST_CHECK_EQUAL(columns[2], 3);
        BOOST_CHECK_EQUAL(columns[3], 4);
        BOOST_CHECK_EQUAL(columns[4], 5);
        BOOST_CHECK_EQUAL(columns[5], 6);
        BOOST_CHECK_EQUAL(columns[6], 7);
        BOOST_CHECK_EQUAL(columns[7], 1);
        BOOST_CHECK_EQUAL(columns[8], 0);
        BOOST_CHECK_EQUAL(columns[9], 0);
        BOOST_CHECK_EQUAL(columns[10], 0);

        SQLLEN columnLens[columnsCnt] = { 0 };

        // Binding columns.
        for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        {
            ret = SQLBindCol(stmt, i + 1, type, &columns[i], sizeof(columns[i]), &columnLens[i]);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
        }

        ret = SQLFetch(stmt);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(columns[0], 8);
        BOOST_CHECK_EQUAL(columns[1], 7);
        BOOST_CHECK_EQUAL(columns[2], 6);
        BOOST_CHECK_EQUAL(columns[3], 5);
        BOOST_CHECK_EQUAL(columns[4], 4);
        BOOST_CHECK_EQUAL(columns[5], 3);
        BOOST_CHECK_EQUAL(columns[6], 2);
        BOOST_CHECK_EQUAL(columns[7], 0);
        BOOST_CHECK_EQUAL(columns[8], 0);
        BOOST_CHECK_EQUAL(columns[9], 0);
        BOOST_CHECK_EQUAL(columns[10], 0);

        BOOST_CHECK_EQUAL(columnLens[0], 0);
        BOOST_CHECK_EQUAL(columnLens[1], 0);
        BOOST_CHECK_EQUAL(columnLens[2], 0);
        BOOST_CHECK_EQUAL(columnLens[3], 0);
        BOOST_CHECK_EQUAL(columnLens[4], 0);
        BOOST_CHECK_EQUAL(columnLens[5], 0);
        BOOST_CHECK_EQUAL(columnLens[6], 0);
        BOOST_CHECK_EQUAL(columnLens[7], 0);
        BOOST_CHECK_EQUAL(columnLens[8], SQL_NO_TOTAL);
        BOOST_CHECK_EQUAL(columnLens[9], SQL_NO_TOTAL);
        BOOST_CHECK_EQUAL(columnLens[10], SQL_NO_TOTAL);

        ret = SQLFetch(stmt);
        BOOST_CHECK(ret == SQL_NO_DATA);
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

    template<typename T>
    void CheckSingleResult(const char* request, const T& expected)
    {
        BOOST_FAIL("Function is not defined for the type.");
    }

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
void QueriesTestSuiteFixture::CheckSingleResult<std::string>(const char* request, const std::string& expected)
{
    SQLCHAR res[ODBC_BUFFER_SIZE] = { 0 };
    SQLLEN resLen = 0;

    CheckSingleResult0(request, SQL_C_CHAR, res, ODBC_BUFFER_SIZE, &resLen);

    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(res), static_cast<size_t>(resLen)), expected);
}

template<>
void QueriesTestSuiteFixture::CheckSingleResult<int64_t>(const char* request, const int64_t& expected)
{
    CheckSingleResultNum0<int64_t>(request, expected, SQL_C_SBIGINT);
}

template<>
void QueriesTestSuiteFixture::CheckSingleResult<int32_t>(const char* request, const int32_t& expected)
{
    CheckSingleResultNum0<int32_t>(request, expected, SQL_C_SLONG);
}

template<>
void QueriesTestSuiteFixture::CheckSingleResult<int16_t>(const char* request, const int16_t& expected)
{
    CheckSingleResultNum0<int16_t>(request, expected, SQL_C_SSHORT);
}

template<>
void QueriesTestSuiteFixture::CheckSingleResult<int8_t>(const char* request, const int8_t& expected)
{
    CheckSingleResultNum0<int8_t>(request, expected, SQL_C_STINYINT);
}

template<>
void QueriesTestSuiteFixture::CheckSingleResult<float>(const char* request, const float& expected)
{
    SQLFLOAT res = 0;

    CheckSingleResult0(request, SQL_C_FLOAT, &res, 0, 0);

    BOOST_CHECK_CLOSE(static_cast<float>(res), expected, 1E-6f);
}

BOOST_FIXTURE_TEST_SUITE(QueriesTestSuite, QueriesTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestTwoRowsInt8)
{
    CheckTwoRowsInt<int8_t>(SQL_C_STINYINT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsUint8)
{
    CheckTwoRowsInt<uint8_t>(SQL_C_UTINYINT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsInt16)
{
    CheckTwoRowsInt<int16_t>(SQL_C_SSHORT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsUint16)
{
    CheckTwoRowsInt<uint16_t>(SQL_C_USHORT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsInt32)
{
    CheckTwoRowsInt<int32_t>(SQL_C_SLONG);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsUint32)
{
    CheckTwoRowsInt<uint32_t>(SQL_C_ULONG);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsInt64)
{
    CheckTwoRowsInt<int64_t>(SQL_C_SBIGINT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsUint64)
{
    CheckTwoRowsInt<uint64_t>(SQL_C_UBIGINT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsString)
{
    SQLRETURN ret;

    TestType in1(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), BinaryUtils::MakeDateGmt(1987, 6, 5), BinaryUtils::MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));
    TestType in2(8, 7, 6, 5, "4", 3.0f, 2.0, false, Guid(1, 0), BinaryUtils::MakeDateGmt(1976, 1, 12), BinaryUtils::MakeTimestampGmt(1978, 8, 21, 23, 13, 45, 999999999));

    testCache.Put(1, in1);
    testCache.Put(2, in2);

    const size_t columnsCnt = 11;

    SQLCHAR columns[columnsCnt][ODBC_BUFFER_SIZE] = { 0 };

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, "
        "floatField, doubleField, boolField, guidField, dateField, timestampField FROM TestType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[0])), "1");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[1])), "2");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[2])), "3");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[3])), "4");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[4])), "5");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[5])), "6");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[6])), "7");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[7])), "1");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[8])), "00000000-0000-0008-0000-000000000009");
    // Such format is used because Date returned as Timestamp.
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[9])), "1987-06-05 00:00:00");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[10])), "1998-12-27 01:02:03");

    SQLLEN columnLens[columnsCnt] = { 0 };

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, &columnLens[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[0])), "8");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[1])), "7");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[2])), "6");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[3])), "5");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[4])), "4");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[5])), "3");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[6])), "2");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[7])), "0");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[8])), "00000000-0000-0001-0000-000000000000");
    // Such format is used because Date returned as Timestamp.
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[9])), "1976-01-12 00:00:00");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[10])), "1978-08-21 23:13:45");

    BOOST_CHECK_EQUAL(columnLens[0], 1);
    BOOST_CHECK_EQUAL(columnLens[1], 1);
    BOOST_CHECK_EQUAL(columnLens[2], 1);
    BOOST_CHECK_EQUAL(columnLens[3], 1);
    BOOST_CHECK_EQUAL(columnLens[4], 1);
    BOOST_CHECK_EQUAL(columnLens[5], 1);
    BOOST_CHECK_EQUAL(columnLens[6], 1);
    BOOST_CHECK_EQUAL(columnLens[7], 1);
    BOOST_CHECK_EQUAL(columnLens[8], 36);
    BOOST_CHECK_EQUAL(columnLens[9], 19);
    BOOST_CHECK_EQUAL(columnLens[10], 19);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestOneRowString)
{
    SQLRETURN ret;

    TestType in(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), BinaryUtils::MakeDateGmt(1987, 6, 5), BinaryUtils::MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    testCache.Put(1, in);

    const size_t columnsCnt = 11;

    SQLCHAR columns[columnsCnt][ODBC_BUFFER_SIZE] = { 0 };

    SQLLEN columnLens[columnsCnt] = { 0 };

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, &columnLens[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, "
        "floatField, doubleField, boolField, guidField, dateField, timestampField FROM TestType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[0])), "1");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[1])), "2");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[2])), "3");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[3])), "4");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[4])), "5");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[5])), "6");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[6])), "7");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[7])), "1");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[8])), "00000000-0000-0008-0000-000000000009");
    // Such format is used because Date returned as Timestamp.
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[9])), "1987-06-05 00:00:00");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[10])), "1998-12-27 01:02:03");

    BOOST_CHECK_EQUAL(columnLens[0], 1);
    BOOST_CHECK_EQUAL(columnLens[1], 1);
    BOOST_CHECK_EQUAL(columnLens[2], 1);
    BOOST_CHECK_EQUAL(columnLens[3], 1);
    BOOST_CHECK_EQUAL(columnLens[4], 1);
    BOOST_CHECK_EQUAL(columnLens[5], 1);
    BOOST_CHECK_EQUAL(columnLens[6], 1);
    BOOST_CHECK_EQUAL(columnLens[7], 1);
    BOOST_CHECK_EQUAL(columnLens[8], 36);
    BOOST_CHECK_EQUAL(columnLens[9], 19);
    BOOST_CHECK_EQUAL(columnLens[10], 19);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestOneRowStringLen)
{
    SQLRETURN ret;

    TestType in(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), BinaryUtils::MakeDateGmt(1987, 6, 5), BinaryUtils::MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    testCache.Put(1, in);

    const size_t columnsCnt = 11;

    SQLLEN columnLens[columnsCnt] = { 0 };

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, 0, 0, &columnLens[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, "
        "floatField, doubleField, boolField, guidField, dateField, timestampField FROM TestType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(columnLens[0], 1);
    BOOST_CHECK_EQUAL(columnLens[1], 1);
    BOOST_CHECK_EQUAL(columnLens[2], 1);
    BOOST_CHECK_EQUAL(columnLens[3], 1);
    BOOST_CHECK_EQUAL(columnLens[4], 1);
    BOOST_CHECK_EQUAL(columnLens[5], 1);
    BOOST_CHECK_EQUAL(columnLens[6], 1);
    BOOST_CHECK_EQUAL(columnLens[7], 1);
    BOOST_CHECK_EQUAL(columnLens[8], 36);
    BOOST_CHECK_EQUAL(columnLens[9], 19);
    BOOST_CHECK_EQUAL(columnLens[10], 19);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestAggrFunctionLength)
{
    TestType in;
    in.strField = "Lorem ipsum dolor sit amet, consectetur adipiscing elit";

    testCache.Put(1, in);

    CheckSingleResult<int64_t>("SELECT {fn LENGTH(strField)} FROM TestType", in.strField.size());
}

BOOST_AUTO_TEST_CASE(TestAggrFunctionAvgInt)
{
    std::vector<TestType> in(3);

    in[0].i32Field = 43;
    in[1].i32Field = 311;
    in[2].i32Field = 7;

    int32_t avg = 0;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
    {
        testCache.Put(i, in[i]);

        avg += in[i].i32Field;
    }

    avg /= static_cast<int32_t>(in.size());

    CheckSingleResult<int64_t>("SELECT {fn AVG(i32Field)} FROM TestType", avg);
}

BOOST_AUTO_TEST_CASE(TestAggrFunctionAvgFloat)
{
    std::vector<TestType> in(3);

    in[0].floatField = 43.0;
    in[1].floatField = 311.0;
    in[2].floatField = 7.0;

    float avg = 0;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
    {
        testCache.Put(i, in[i]);

        avg += in[i].i32Field;
    }

    avg /= in.size();

    CheckSingleResult<float>("SELECT {fn AVG(floatField)} FROM TestType", avg);
}

BOOST_AUTO_TEST_CASE(TestAggrFunctionCount)
{
    std::vector<TestType> in(8);

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
        testCache.Put(i, in[i]);

    CheckSingleResult<int64_t>("SELECT {fn COUNT(*)} FROM TestType", in.size());
}

BOOST_AUTO_TEST_CASE(TestAggrFunctionMax)
{
    std::vector<TestType> in(4);

    in[0].i32Field = 121;
    in[1].i32Field = 17;
    in[2].i32Field = 314041;
    in[3].i32Field = 9410;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
        testCache.Put(i, in[i]);

    CheckSingleResult<int64_t>("SELECT {fn MAX(i32Field)} FROM TestType", in[2].i32Field);
}

BOOST_AUTO_TEST_CASE(TestAggrFunctionMin)
{
    std::vector<TestType> in(4);

    in[0].i32Field = 121;
    in[1].i32Field = 17;
    in[2].i32Field = 314041;
    in[3].i32Field = 9410;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
        testCache.Put(i, in[i]);

    CheckSingleResult<int64_t>("SELECT {fn MIN(i32Field)} FROM TestType", in[1].i32Field);
}

BOOST_AUTO_TEST_CASE(TestAggrFunctionSum)
{
    std::vector<TestType> in(4);

    in[0].i32Field = 121;
    in[1].i32Field = 17;
    in[2].i32Field = 314041;
    in[3].i32Field = 9410;

    int64_t sum = 0;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
    {
        testCache.Put(i, in[i]);

        sum += in[i].i32Field;
    }

    CheckSingleResult<int64_t>("SELECT {fn SUM(i32Field)} FROM TestType", sum);
}

BOOST_AUTO_TEST_SUITE_END()
