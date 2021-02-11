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
#include <algorithm>

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/common/fixed_size_array.h"
#include "ignite/ignition.h"
#include "ignite/impl/binary/binary_utils.h"
#include "ignite/binary/binary_object.h"

#include "teamcity/teamcity_messages.h"
#include "test_type.h"
#include "complex_type.h"
#include "test_utils.h"
#include "odbc_test_suite.h"

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;
using namespace ignite_test;
using namespace ignite::binary;
using namespace ignite::impl::binary;
using namespace ignite::impl::interop;

using namespace boost::unit_test;

using ignite::impl::binary::BinaryUtils;

/**
 * Test setup fixture.
 */
struct QueriesTestSuiteFixture : odbc::OdbcTestSuite
{
    /**
     * Constructor.
     */
    QueriesTestSuiteFixture() :
        cache1(0),
        cache2(0)
    {
        grid = StartPlatformNode("queries-test.xml", "NodeMain");

        cache1 = grid.GetCache<int64_t, TestType>("cache");
        cache2 = grid.GetCache<int64_t, ComplexType>("cache2");
    }

    /**
     * Destructor.
     */
    virtual ~QueriesTestSuiteFixture()
    {
        // No-op.
    }

    template<typename T>
    void CheckTwoRowsInt(SQLSMALLINT type)
    {
        Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

        SQLRETURN ret;

        TestType in1(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
            MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

        TestType in2(8, 7, 6, 5, "4", 3.0f, 2.0, false, Guid(1, 0), MakeDateGmt(1976, 1, 12),
            MakeTimeGmt(0, 8, 59), MakeTimestampGmt(1978, 8, 21, 23, 13, 45, 456));

        cache1.Put(1, in1);
        cache1.Put(2, in2);

        const SQLSMALLINT columnsCnt = 12;

        T columns[columnsCnt];

        std::memset(&columns, 0, sizeof(columns));

        // Binding columns.
        for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        {
            ret = SQLBindCol(stmt, i + 1, type, &columns[i], sizeof(columns[i]), 0);

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
        }

        char request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
            "doubleField, boolField, guidField, dateField, timeField, timestampField FROM TestType";

        ret = SQLExecDirect(stmt, reinterpret_cast<SQLCHAR*>(request), SQL_NTS);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

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
        BOOST_CHECK_EQUAL(columns[11], 0);

        SQLLEN columnLens[columnsCnt];

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
        BOOST_CHECK_EQUAL(columns[11], 0);

        BOOST_CHECK_EQUAL(columnLens[0], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[1], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[2], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[3], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[4], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[5], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[6], static_cast<SQLLEN>(sizeof(T)));
        BOOST_CHECK_EQUAL(columnLens[7], static_cast<SQLLEN>(sizeof(T)));

        ret = SQLFetch(stmt);
        BOOST_CHECK(ret == SQL_NO_DATA);
    }

    void CheckParamsNum(const std::string& req, SQLSMALLINT expectedParamsNum)
    {
        std::vector<SQLCHAR> req0(req.begin(), req.end());

        SQLRETURN ret = SQLPrepare(stmt, &req0[0], static_cast<SQLINTEGER>(req0.size()));

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQLSMALLINT paramsNum = -1;

        ret = SQLNumParams(stmt, &paramsNum);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(paramsNum, expectedParamsNum);
    }

    int CountRows(SQLHSTMT stmt)
    {
        int res = 0;

        SQLRETURN ret = SQL_SUCCESS;

        while (ret == SQL_SUCCESS)
        {
            ret = SQLFetch(stmt);

            if (ret == SQL_NO_DATA)
                break;

            if (!SQL_SUCCEEDED(ret))
                BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

            ++res;
        }

        return res;
    }

    static Ignite StartAdditionalNode(const char* name)
    {
        return StartPlatformNode("queries-test.xml", name);
    }

    /** Node started during the test. */
    Ignite grid;

    /** Frist cache instance. */
    Cache<int64_t, TestType> cache1;

    /** Second cache instance. */
    Cache<int64_t, ComplexType> cache2;
};

BOOST_FIXTURE_TEST_SUITE(QueriesTestSuite, QueriesTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestTwoRowsInt8)
{
    CheckTwoRowsInt<signed char>(SQL_C_STINYINT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsUint8)
{
    CheckTwoRowsInt<unsigned char>(SQL_C_UTINYINT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsInt16)
{
    CheckTwoRowsInt<signed short>(SQL_C_SSHORT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsUint16)
{
    CheckTwoRowsInt<unsigned short>(SQL_C_USHORT);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsInt32)
{
    CheckTwoRowsInt<SQLINTEGER>(SQL_C_SLONG);
}

BOOST_AUTO_TEST_CASE(TestTwoRowsUint32)
{
    CheckTwoRowsInt<SQLUINTEGER>(SQL_C_ULONG);
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
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    TestType in1(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    TestType in2(8, 7, 6, 5, "4", 3.0f, 2.0, false, Guid(1, 0), MakeDateGmt(1976, 1, 12),
        MakeTimeGmt(0, 8, 59), MakeTimestampGmt(1978, 8, 21, 23, 13, 45, 999999999));

    cache1.Put(1, in1);
    cache1.Put(2, in2);

    const SQLSMALLINT columnsCnt = 12;

    SQLCHAR columns[columnsCnt][ODBC_BUFFER_SIZE];

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, timeField, timestampField FROM TestType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

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
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[10])), "12:48:12");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[11])), "1998-12-27 01:02:03");

    SQLLEN columnLens[columnsCnt];

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
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[10])), "00:08:59");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[11])), "1978-08-21 23:13:45");

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
    BOOST_CHECK_EQUAL(columnLens[10], 8);
    BOOST_CHECK_EQUAL(columnLens[11], 19);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestOneRowString)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    TestType in(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    cache1.Put(1, in);

    const SQLSMALLINT columnsCnt = 12;

    SQLCHAR columns[columnsCnt][ODBC_BUFFER_SIZE];

    SQLLEN columnLens[columnsCnt];

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, &columnLens[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, CAST('12:48:12' AS TIME), timestampField FROM TestType";

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
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[10])), "12:48:12");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[11])), "1998-12-27 01:02:03");

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
    BOOST_CHECK_EQUAL(columnLens[10], 8);
    BOOST_CHECK_EQUAL(columnLens[11], 19);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestOneRowStringLen)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    TestType in(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    cache1.Put(1, in);

    const SQLSMALLINT columnsCnt = 12;

    SQLLEN columnLens[columnsCnt];

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, 0, 0, &columnLens[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, timeField, timestampField FROM TestType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

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
    BOOST_CHECK_EQUAL(columnLens[10], 8);
    BOOST_CHECK_EQUAL(columnLens[11], 19);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestOneRowObject)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache2");

    SQLRETURN ret;

    ComplexType obj;

    obj.i32Field = 123;
    obj.strField = "Some string";

    obj.objField.f1 = 54321;
    obj.objField.f2 = "Hello Ignite";

    cache2.Put(1, obj);

    int64_t column1 = 0;
    int8_t column2[ODBC_BUFFER_SIZE];
    char column3[ODBC_BUFFER_SIZE];

    SQLLEN column1Len = sizeof(column1);
    SQLLEN column2Len = sizeof(column2);
    SQLLEN column3Len = sizeof(column3);

    // Binding columns.
    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &column1, column1Len, &column1Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 2, SQL_C_BINARY, &column2, column2Len, &column2Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 3, SQL_C_CHAR, &column3, column3Len, &column3Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR request[] = "SELECT i32Field, objField, strField FROM ComplexType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(column1, obj.i32Field);
    BOOST_CHECK_EQUAL(column3, obj.strField);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestDataAtExecution)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    TestType in1(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    TestType in2(8, 7, 6, 5, "4", 3.0f, 2.0, false, Guid(1, 0), MakeDateGmt(1976, 1, 12),
        MakeTimeGmt(0, 8, 59), MakeTimestampGmt(1978, 8, 21, 23, 13, 45, 999999999));

    cache1.Put(1, in1);
    cache1.Put(2, in2);

    const SQLSMALLINT columnsCnt = 12;

    SQLLEN columnLens[columnsCnt];
    SQLCHAR columns[columnsCnt][ODBC_BUFFER_SIZE];

    // Binding columns.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_CHAR, &columns[i], ODBC_BUFFER_SIZE, &columnLens[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, guidField, dateField, timeField, timestampField FROM TestType "
        "WHERE i32Field = ? AND strField = ?";

    ret = SQLPrepare(stmt, request, SQL_NTS);

    SQLLEN ind1 = 1;
    SQLLEN ind2 = 2;

    SQLLEN len1 = SQL_DATA_AT_EXEC;
    SQLLEN len2 = SQL_LEN_DATA_AT_EXEC(static_cast<SQLLEN>(in1.strField.size()));

    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), &len1);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_VARCHAR, 100, 100, &ind2, sizeof(ind2), &len2);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    BOOST_REQUIRE_EQUAL(ret, SQL_NEED_DATA);

    void* oind;

    ret = SQLParamData(stmt, &oind);

    BOOST_REQUIRE_EQUAL(ret, SQL_NEED_DATA);

    if (oind == &ind1)
        ret = SQLPutData(stmt, &in1.i32Field, 0);
    else if (oind == &ind2)
        ret = SQLPutData(stmt, (SQLPOINTER)in1.strField.c_str(), (SQLLEN)in1.strField.size());
    else
        BOOST_FAIL("Unknown indicator value");

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLParamData(stmt, &oind);

    BOOST_REQUIRE_EQUAL(ret, SQL_NEED_DATA);

    if (oind == &ind1)
        ret = SQLPutData(stmt, &in1.i32Field, 0);
    else if (oind == &ind2)
        ret = SQLPutData(stmt, (SQLPOINTER)in1.strField.c_str(), (SQLLEN)in1.strField.size());
    else
        BOOST_FAIL("Unknown indicator value");

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLParamData(stmt, &oind);

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
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[10])), "12:48:12");
    BOOST_CHECK_EQUAL(std::string(reinterpret_cast<char*>(columns[11])), "1998-12-27 01:02:03");

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
    BOOST_CHECK_EQUAL(columnLens[10], 8);
    BOOST_CHECK_EQUAL(columnLens[11], 19);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestNullFields)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    TestType in(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    TestType inNull;

    inNull.allNulls = true;

    cache1.Put(1, in);
    cache1.Put(2, inNull);
    cache1.Put(3, in);

    const SQLSMALLINT columnsCnt = 11;

    SQLLEN columnLens[columnsCnt];

    int8_t i8Column;
    int16_t i16Column;
    int32_t i32Column;
    int64_t i64Column;
    char strColumn[ODBC_BUFFER_SIZE];
    float floatColumn;
    double doubleColumn;
    bool boolColumn;
    SQL_DATE_STRUCT dateColumn;
    SQL_TIME_STRUCT timeColumn;
    SQL_TIMESTAMP_STRUCT timestampColumn;

    // Binding columns.
    ret = SQLBindCol(stmt, 1, SQL_C_STINYINT, &i8Column, 0, &columnLens[0]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 2, SQL_C_SSHORT, &i16Column, 0, &columnLens[1]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 3, SQL_C_SLONG, &i32Column, 0, &columnLens[2]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 4, SQL_C_SBIGINT, &i64Column, 0, &columnLens[3]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 5, SQL_C_CHAR, &strColumn, ODBC_BUFFER_SIZE, &columnLens[4]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 6, SQL_C_FLOAT, &floatColumn, 0, &columnLens[5]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 7, SQL_C_DOUBLE, &doubleColumn, 0, &columnLens[6]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 8, SQL_C_BIT, &boolColumn, 0, &columnLens[7]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 9, SQL_C_TYPE_DATE, &dateColumn, 0, &columnLens[8]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 10, SQL_C_TYPE_TIME, &timeColumn, 0, &columnLens[9]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 11, SQL_C_TYPE_TIMESTAMP, &timestampColumn, 0, &columnLens[10]);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR request[] = "SELECT i8Field, i16Field, i32Field, i64Field, strField, floatField, "
        "doubleField, boolField, dateField, timeField, timestampField FROM TestType "
        "ORDER BY _key";

    ret = SQLExecDirect(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Fetching the first non-null row.
    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Checking that columns are not null.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        BOOST_CHECK_NE(columnLens[i], SQL_NULL_DATA);

    // Fetching null row.
    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Checking that columns are null.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        BOOST_CHECK_EQUAL(columnLens[i], SQL_NULL_DATA);

    // Fetching the last non-null row.
    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Checking that columns are not null.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
        BOOST_CHECK_NE(columnLens[i], SQL_NULL_DATA);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestDistributedJoins)
{
    MUTE_TEST_FOR_TEAMCITY;

    // Starting additional node.
    Ignite node1 = StartAdditionalNode("Node1");
    Ignite node2 = StartAdditionalNode("Node2");

    const int entriesNum = 1000;

    // Filling cache with data.
    for (int i = 0; i < entriesNum; ++i)
    {
        TestType entry;

        entry.i32Field = i;
        entry.i64Field = entriesNum - i - 1;

        cache1.Put(i, entry);
    }

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret;

    const SQLSMALLINT columnsCnt = 2;

    SQLBIGINT columns[columnsCnt];

    // Binding colums.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_SLONG, &columns[i], 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR request[] =
        "SELECT T0.i32Field, T1.i64Field FROM TestType AS T0 "
        "INNER JOIN TestType AS T1 "
        "ON (T0.i32Field = T1.i64Field)";

    ret = SQLExecDirect(stmt, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int rowsNum = CountRows(stmt);

    BOOST_CHECK_GT(rowsNum, 0);
    BOOST_CHECK_LT(rowsNum, entriesNum);

    Disconnect();

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;DISTRIBUTED_JOINS=true;");

    // Binding colums.
    for (SQLSMALLINT i = 0; i < columnsCnt; ++i)
    {
        ret = SQLBindCol(stmt, i + 1, SQL_C_SLONG, &columns[i], 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    ret = SQLExecDirect(stmt, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    rowsNum = CountRows(stmt);

    BOOST_CHECK_EQUAL(rowsNum, entriesNum);
}

BOOST_AUTO_TEST_CASE(TestInsertSelect)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int recordsNum = 100;

    // Inserting values.
    InsertTestStrings(recordsNum);

    int64_t key = 0;
    char strField[1024];
    SQLLEN strFieldLen = 0;

    // Binding columns.
    SQLRETURN ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &key, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Binding columns.
    ret = SQLBindCol(stmt, 2, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Just selecting everything to make sure everything is OK
    SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType ORDER BY _key";

    ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int selectedRecordsNum = 0;

    ret = SQL_SUCCESS;

    while (ret == SQL_SUCCESS)
    {
        ret = SQLFetch(stmt);

        if (ret == SQL_NO_DATA)
            break;

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        std::string expectedStr = GetTestString(selectedRecordsNum);
        int64_t expectedKey = selectedRecordsNum + 1;

        BOOST_CHECK_EQUAL(key, expectedKey);

        BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

        ++selectedRecordsNum;
    }

    BOOST_CHECK_EQUAL(recordsNum, selectedRecordsNum);
}

BOOST_AUTO_TEST_CASE(TestInsertUpdateSelect)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int recordsNum = 100;

    // Inserting values.
    InsertTestStrings(recordsNum);

    int64_t key = 0;
    char strField[1024];
    SQLLEN strFieldLen = 0;

    SQLCHAR updateReq[] = "UPDATE TestType SET strField = 'Updated value' WHERE _key = 42";

    SQLRETURN ret = SQLExecDirect(stmt, updateReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFreeStmt(stmt, SQL_CLOSE);

    // Binding columns.
    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &key, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Binding columns.
    ret = SQLBindCol(stmt, 2, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Just selecting everything to make sure everything is OK
    SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType ORDER BY _key";

    ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int selectedRecordsNum = 0;

    ret = SQL_SUCCESS;

    while (ret == SQL_SUCCESS)
    {
        ret = SQLFetch(stmt);

        if (ret == SQL_NO_DATA)
            break;

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        int64_t expectedKey = selectedRecordsNum + 1;
        std::string expectedStr;

        BOOST_CHECK_EQUAL(key, expectedKey);

        if (expectedKey == 42)
            expectedStr = "Updated value";
        else
            expectedStr = GetTestString(selectedRecordsNum);

        BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

        ++selectedRecordsNum;
    }

    BOOST_CHECK_EQUAL(recordsNum, selectedRecordsNum);
}

BOOST_AUTO_TEST_CASE(TestInsertDeleteSelect)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int recordsNum = 100;

    // Inserting values.
    InsertTestStrings(recordsNum);

    int64_t key = 0;
    char strField[1024];
    SQLLEN strFieldLen = 0;

    SQLCHAR updateReq[] = "DELETE FROM TestType WHERE (_key % 2) = 1";

    SQLRETURN ret = SQLExecDirect(stmt, updateReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFreeStmt(stmt, SQL_CLOSE);

    // Binding columns.
    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &key, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Binding columns.
    ret = SQLBindCol(stmt, 2, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Just selecting everything to make sure everything is OK
    SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType ORDER BY _key";

    ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int selectedRecordsNum = 0;

    ret = SQL_SUCCESS;

    while (ret == SQL_SUCCESS)
    {
        ret = SQLFetch(stmt);

        if (ret == SQL_NO_DATA)
            break;

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        int64_t expectedKey = (selectedRecordsNum + 1) * 2;
        std::string expectedStr = GetTestString(expectedKey - 1);

        BOOST_CHECK_EQUAL(key, expectedKey);
        BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

        ++selectedRecordsNum;
    }

    BOOST_CHECK_EQUAL(recordsNum / 2, selectedRecordsNum);
}

BOOST_AUTO_TEST_CASE(TestInsertMergeSelect)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int recordsNum = 100;

    // Inserting values.
    InsertTestStrings(recordsNum / 2);

    // Merging values.
    InsertTestStrings(recordsNum, true);

    int64_t key = 0;
    char strField[1024];
    SQLLEN strFieldLen = 0;

    // Binding columns.
    SQLRETURN ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &key, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Binding columns.
    ret = SQLBindCol(stmt, 2, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Just selecting everything to make sure everything is OK
    SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType ORDER BY _key";

    ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int selectedRecordsNum = 0;

    ret = SQL_SUCCESS;

    while (ret == SQL_SUCCESS)
    {
        ret = SQLFetch(stmt);

        if (ret == SQL_NO_DATA)
            break;

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        std::string expectedStr = GetTestString(selectedRecordsNum);
        int64_t expectedKey = selectedRecordsNum + 1;

        BOOST_CHECK_EQUAL(key, expectedKey);

        BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), expectedStr);

        ++selectedRecordsNum;
    }

    BOOST_CHECK_EQUAL(recordsNum, selectedRecordsNum);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect2)
{
    InsertBatchSelect(2);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect100)
{
    InsertBatchSelect(100);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect1000)
{
    InsertBatchSelect(1000);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect1023)
{
    InsertBatchSelect(1023);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect1024)
{
    InsertBatchSelect(1024);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect1025)
{
    InsertBatchSelect(1025);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect2000)
{
    InsertBatchSelect(2000);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect2047)
{
    InsertBatchSelect(2047);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect2048)
{
    InsertBatchSelect(2048);
}

BOOST_AUTO_TEST_CASE(TestInsertBatchSelect2049)
{
    InsertBatchSelect(2049);
}

BOOST_AUTO_TEST_CASE(TestNotFullInsertBatchSelect900)
{
    InsertNonFullBatchSelect(900, 42);
}

BOOST_AUTO_TEST_CASE(TestNotFullInsertBatchSelect1500)
{
    InsertNonFullBatchSelect(1500, 100);
}

BOOST_AUTO_TEST_CASE(TestNotFullInsertBatchSelect4500)
{
    InsertNonFullBatchSelect(4500, 1500);
}

BOOST_AUTO_TEST_CASE(TestNotFullInsertBatchSelect4096)
{
    InsertNonFullBatchSelect(4096, 1024);
}

template<size_t n, size_t k>
void CheckMeta(char columns[n][k], SQLLEN columnsLen[n])
{
    std::string catalog(columns[0], columnsLen[0]);
    std::string schema(columns[1], columnsLen[1]);
    std::string table(columns[2], columnsLen[2]);
    std::string tableType(columns[3], columnsLen[3]);

    BOOST_CHECK_EQUAL(catalog, std::string(""));
    BOOST_CHECK_EQUAL(tableType, std::string("TABLE"));
    BOOST_CHECK_EQUAL(columnsLen[4], SQL_NULL_DATA);

    if (schema == "\"cache\"")
    {
        BOOST_CHECK_EQUAL(table, std::string("TESTTYPE"));
    }
    else if (schema == "\"cache2\"")
    {
        BOOST_CHECK_EQUAL(table, std::string("COMPLEXTYPE"));
    }
    else
    {
        BOOST_FAIL("Unknown schema: " + schema);
    }
}

BOOST_AUTO_TEST_CASE(TestTablesMeta)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache2");

    SQLRETURN ret;

    enum { COLUMNS_NUM = 5 };

    // Five collumns: TABLE_CAT, TABLE_SCHEM, TABLE_NAME, TABLE_TYPE, REMARKS
    char columns[COLUMNS_NUM][ODBC_BUFFER_SIZE];
    SQLLEN columnsLen[COLUMNS_NUM];

    // Binding columns.
    for (size_t i = 0; i < COLUMNS_NUM; ++i)
    {
        columnsLen[i] = ODBC_BUFFER_SIZE;

        ret = SQLBindCol(stmt, static_cast<SQLSMALLINT>(i + 1), SQL_C_CHAR, columns[i], columnsLen[i], &columnsLen[i]);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }

    SQLCHAR catalogPattern[] = "";
    SQLCHAR schemaPattern[] = "";
    SQLCHAR tablePattern[] = "";
    SQLCHAR tableTypePattern[] = "";

    ret = SQLTables(stmt, catalogPattern, SQL_NTS, schemaPattern,
        SQL_NTS, tablePattern, SQL_NTS, tableTypePattern, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    CheckMeta<COLUMNS_NUM, ODBC_BUFFER_SIZE>(columns, columnsLen);

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    CheckMeta<COLUMNS_NUM, ODBC_BUFFER_SIZE>(columns, columnsLen);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

template<typename T>
void CheckObjectData(int8_t* data, int32_t len, T const& value)
{
    InteropUnpooledMemory mem(len);
    mem.Length(len);
    memcpy(mem.Data(), data, len);

    BinaryObject obj(BinaryObjectImpl::FromMemory(mem, 0, 0));

    T actual = obj.Deserialize<T>();

    BOOST_CHECK_EQUAL(value, actual);
}

BOOST_AUTO_TEST_CASE(TestKeyVal)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache2");

    SQLRETURN ret;

    ComplexType obj;

    obj.i32Field = 123;
    obj.strField = "Some string";

    obj.objField.f1 = 54321;
    obj.objField.f2 = "Hello Ignite";

    cache2.Put(1, obj);

    //_key
    int64_t column1 = 0;
    //_val
    int8_t column2[ODBC_BUFFER_SIZE];
    //k
    int64_t column3 = 0;
    //v
    int8_t column4[ODBC_BUFFER_SIZE];
    //i32Field
    int64_t column5 = 0;
    //objField
    int8_t column6[ODBC_BUFFER_SIZE];
    //strField
    char column7[ODBC_BUFFER_SIZE];

    SQLLEN column1Len = sizeof(column1);
    SQLLEN column2Len = sizeof(column2);
    SQLLEN column3Len = sizeof(column3);
    SQLLEN column4Len = sizeof(column4);
    SQLLEN column5Len = sizeof(column5);
    SQLLEN column6Len = sizeof(column6);
    SQLLEN column7Len = sizeof(column7);

    // Binding columns.
    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &column1, column1Len, &column1Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 2, SQL_C_BINARY, &column2, column2Len, &column2Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 3, SQL_C_SLONG, &column3, column3Len, &column3Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 4, SQL_C_BINARY, &column4, column4Len, &column4Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 5, SQL_C_SLONG, &column5, column5Len, &column5Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 6, SQL_C_BINARY, &column6, column6Len, &column6Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 7, SQL_C_CHAR, &column7, column7Len, &column7Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR request[] = "SELECT _key, _val, k, v, i32Field, objField, strField FROM ComplexType";

    ret = SQLExecDirect(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(column1, 1);

    CheckObjectData(column2, static_cast<int32_t>(column2Len), obj);

    BOOST_CHECK_EQUAL(column3, 1);

    CheckObjectData(column4, static_cast<int32_t>(column4Len), obj);

    BOOST_CHECK_EQUAL(column5, obj.i32Field);

    CheckObjectData(column6, static_cast<int32_t>(column6Len), obj.objField);

    BOOST_CHECK_EQUAL(column7, obj.strField);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);

    ret = SQLFreeStmt(stmt, SQL_CLOSE);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR requestStar[] = "SELECT _key, _val, * FROM ComplexType";

    ret = SQLExecDirect(stmt, requestStar, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(column1, 1);

    CheckObjectData(column2, static_cast<int32_t>(column2Len), obj);

    BOOST_CHECK_EQUAL(column3, 1);

    CheckObjectData(column4, static_cast<int32_t>(column4Len), obj);

    BOOST_CHECK_EQUAL(column5, obj.i32Field);

    CheckObjectData(column6, static_cast<int32_t>(column6Len), obj.objField);

    BOOST_CHECK_EQUAL(column7, obj.strField);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestParamsNum)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    CheckParamsNum("SELECT * FROM TestType", 0);
    CheckParamsNum("SELECT * FROM TestType WHERE _key=?", 1);
    CheckParamsNum("SELECT * FROM TestType WHERE _key=? AND _val=?", 2);
    CheckParamsNum("INSERT INTO TestType(_key, strField) VALUES(1, 'some')", 0);
    CheckParamsNum("INSERT INTO TestType(_key, strField) VALUES(?, ?)", 2);
}

BOOST_AUTO_TEST_CASE(TestExecuteAfterCursorClose)
{
    TestType in(1, 2, 3, 4, "5", 6.0f, 7.0, true, Guid(8, 9), MakeDateGmt(1987, 6, 5),
        MakeTimeGmt(12, 48, 12), MakeTimestampGmt(1998, 12, 27, 1, 2, 3, 456));

    cache1.Put(1, in);

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    int64_t key = 0;
    char strField[1024];
    SQLLEN strFieldLen = 0;

    // Binding columns.
    SQLRETURN ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &key, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Binding columns.
    ret = SQLBindCol(stmt, 2, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Just selecting everything to make sure everything is OK
    SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType";

    ret = SQLPrepare(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFreeStmt(stmt, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(key, 1);

    BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), "5");

    ret = SQLFetch(stmt);

    BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestCloseNonFullFetch)
{
    TestType in1;
    TestType in2;

    in1.strField = "str1";
    in2.strField = "str2";

    cache1.Put(1, in1);
    cache1.Put(2, in2);

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    int64_t key = 0;
    char strField[1024];
    SQLLEN strFieldLen = 0;

    // Binding columns.
    SQLRETURN ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &key, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Binding columns.
    ret = SQLBindCol(stmt, 2, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Just selecting everything to make sure everything is OK
    SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType ORDER BY _key";

    ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(key, 1);
    BOOST_CHECK_EQUAL(std::string(strField, strFieldLen), "str1");

    ret = SQLFreeStmt(stmt, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
}

BOOST_AUTO_TEST_CASE(TestBindNullParameter)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLLEN paramInd = SQL_NULL_DATA;

    // Binding NULL parameter.
    SQLRETURN ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_CHAR, SQL_CHAR, 100, 100, 0, 0, &paramInd);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Just selecting everything to make sure everything is OK
    SQLCHAR insertReq[] = "INSERT INTO TestType(_key, strField) VALUES(1, ?)";

    ret = SQLExecDirect(stmt, insertReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Unbindning parameter.
    ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Selecting inserted column to make sure that everything is OK
    SQLCHAR selectReq[] = "SELECT strField FROM TestType";

    char strField[1024];
    SQLLEN strFieldLen = 0;

    // Binding column.
    ret = SQLBindCol(stmt, 1, SQL_C_CHAR, &strField, sizeof(strField), &strFieldLen);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(strFieldLen, SQL_NULL_DATA);
}

BOOST_AUTO_TEST_CASE(TestErrorMessage)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    // Just selecting everything to make sure everything is OK
    SQLCHAR selectReq[] = "SELECT a FROM B";

    SQLRETURN ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);

    std::string error = GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt);
    std::string pattern = "42000: Table \"B\" not found; SQL statement:\nSELECT a FROM B";

    if (error.substr(0, pattern.size()) != pattern)
        BOOST_FAIL("'" + error + "' does not match '" + pattern + "'");
}

BOOST_AUTO_TEST_CASE(TestAffectedRows)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PAGE_SIZE=1024");

    const int recordsNum = 100;

    // Inserting values.
    InsertTestStrings(recordsNum);

    SQLCHAR updateReq[] = "UPDATE TestType SET strField = 'Updated value' WHERE _key > 20 AND _key < 40";

    SQLRETURN ret = SQLExecDirect(stmt, updateReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLLEN affected = 0;
    ret = SQLRowCount(stmt, &affected);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(affected, 19);

    ret = SQLFreeStmt(stmt, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    // Just selecting everything to make sure everything is OK
    SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType ORDER BY _key";

    ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    affected = -1;
    ret = SQLRowCount(stmt, &affected);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(affected, 1024);
}

BOOST_AUTO_TEST_CASE(TestAffectedRowsOnSelect)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PAGE_SIZE=123");

    const int recordsNum = 1000;

    // Inserting values.
    InsertTestStrings(recordsNum);

    // Just selecting everything to make sure everything is OK
    SQLCHAR selectReq[] = "SELECT _key, strField FROM TestType ORDER BY _key";

    SQLRETURN ret = SQLExecDirect(stmt, selectReq, sizeof(selectReq));

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    for (int i = 0; i < 200; ++i)
    {
        SQLLEN affected = -1;
        ret = SQLRowCount(stmt, &affected);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(affected, 123);

        ret = SQLFetch(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }
}

BOOST_AUTO_TEST_CASE(TestMultipleSelects)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int stmtCnt = 10;

    std::stringstream stream;
    for (int i = 0; i < stmtCnt; ++i)
        stream << "select " << i << "; ";

    stream << '\0';

    std::string query0 = stream.str();
    std::vector<SQLCHAR> query(query0.begin(), query0.end());

    SQLRETURN ret = SQLExecDirect(stmt, &query[0], SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    long res = 0;

    BOOST_TEST_CHECKPOINT("Binding column");
    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &res, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    for (long i = 0; i < stmtCnt; ++i)
    {
        ret = SQLFetch(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(res, i);

        ret = SQLFetch(stmt);

        BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);

        ret = SQLMoreResults(stmt);

        if (i < stmtCnt - 1 && !SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
        else if (i == stmtCnt - 1)
            BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);
    }
}

BOOST_AUTO_TEST_CASE(TestMultipleMixedStatements)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int stmtCnt = 10;

    std::stringstream stream;
    for (int i = 0; i < stmtCnt; ++i)
        stream << "select " << i << "; insert into TestType(_key) values(" << i << "); ";

    stream << '\0';

    std::string query0 = stream.str();
    std::vector<SQLCHAR> query(query0.begin(), query0.end());

    SQLRETURN ret = SQLExecDirect(stmt, &query[0], SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    long res = 0;

    BOOST_TEST_CHECKPOINT("Binding column");
    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &res, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    for (long i = 0; i < stmtCnt; ++i)
    {
        ret = SQLFetch(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(res, i);

        ret = SQLFetch(stmt);

        BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);

        ret = SQLMoreResults(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQLLEN affected = 0;
        ret = SQLRowCount(stmt, &affected);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(affected, 1);

        ret = SQLFetch(stmt);

        BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);

        ret = SQLMoreResults(stmt);

        if (i < stmtCnt - 1 && !SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
        else if (i == stmtCnt - 1)
            BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);
    }
}

BOOST_AUTO_TEST_CASE(TestMultipleMixedStatementsNoFetch)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    const int stmtCnt = 10;

    std::stringstream stream;
    for (int i = 0; i < stmtCnt; ++i)
        stream << "select " << i << "; insert into TestType(_key) values(" << i << "); ";

    stream << '\0';

    std::string query0 = stream.str();
    std::vector<SQLCHAR> query(query0.begin(), query0.end());

    SQLRETURN ret = SQLExecDirect(stmt, &query[0], SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    long res = 0;

    BOOST_TEST_CHECKPOINT("Binding column");
    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &res, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    for (long i = 0; i < stmtCnt; ++i)
    {
        ret = SQLMoreResults(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQLLEN affected = 0;
        ret = SQLRowCount(stmt, &affected);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(affected, 1);

        ret = SQLFetch(stmt);

        BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);

        ret = SQLMoreResults(stmt);

        if (i < stmtCnt - 1 && !SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
        else if (i == stmtCnt - 1)
            BOOST_CHECK_EQUAL(ret, SQL_NO_DATA);
    }
}

BOOST_AUTO_TEST_CASE(TestCloseAfterEmptyUpdate)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLCHAR query[] = "update TestType set strField='test' where _key=42";

    SQLRETURN ret = SQLExecDirect(stmt, &query[0], SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFreeStmt(stmt, SQL_CLOSE);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
}

BOOST_AUTO_TEST_CASE(TestLoginTimeout)
{
    Prepare();

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, reinterpret_cast<SQLPOINTER>(1), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    SQLCHAR connectStr[] = "DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache";

    SQLCHAR outstr[ODBC_BUFFER_SIZE];
    SQLSMALLINT outstrlen;

    // Connecting to ODBC server.
    ret = SQLDriverConnect(dbc, NULL, &connectStr[0], static_cast<SQLSMALLINT>(sizeof(connectStr)),
        outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_DBC, dbc));
}

BOOST_AUTO_TEST_CASE(TestLoginTimeoutFail)
{
    Prepare();

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    SQLCHAR connectStr[] = "DRIVER={Apache Ignite};ADDRESS=192.168.0.1:11120;SCHEMA=cache";

    SQLCHAR outstr[ODBC_BUFFER_SIZE];
    SQLSMALLINT outstrlen;

    // Connecting to ODBC server.
    ret = SQLDriverConnect(dbc, NULL, &connectStr[0], static_cast<SQLSMALLINT>(sizeof(connectStr)),
        outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

    if (SQL_SUCCEEDED(ret))
        BOOST_FAIL("Should timeout");
}

BOOST_AUTO_TEST_CASE(TestConnectionTimeoutQuery)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestStrings(10, false);
}

BOOST_AUTO_TEST_CASE(TestConnectionTimeoutBatch)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionTimeoutBoth)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestQueryTimeoutQuery)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    InsertTestStrings(10, false);
}

BOOST_AUTO_TEST_CASE(TestQueryTimeoutBatch)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestQueryTimeoutBoth)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestQueryAndConnectionTimeoutQuery)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(3), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestStrings(10, false);
}

BOOST_AUTO_TEST_CASE(TestQueryAndConnectionTimeoutBatch)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(3), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestQueryAndConnectionTimeoutBoth)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(3), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestSeveralInsertsWithoutClosing)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLCHAR request[] = "INSERT INTO TestType(_key, i32Field) VALUES(?, ?)";

    SQLRETURN ret = SQLPrepare(stmt, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int64_t key = 0;
    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SBIGINT, SQL_BIGINT, 0, 0, &key, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int32_t data = 0;
    ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 0, 0, &data, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    for (int32_t i = 0; i < 10; ++i)
    {
        key = i;
        data = i * 10;

        ret = SQLExecute(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }
}

BOOST_AUTO_TEST_CASE(TestManyCursors)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    for (int32_t i = 0; i < 1000; ++i)
    {
        SQLCHAR req[] = "SELECT 1";

        SQLRETURN ret = SQLExecDirect(stmt, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFreeStmt(stmt, SQL_CLOSE);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }
}

BOOST_AUTO_TEST_CASE(TestManyCursors2)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    for (int32_t i = 0; i < 1000; ++i)
    {
        ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQLCHAR req[] = "SELECT 1";

        ret = SQLExecDirect(stmt, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        int32_t res = 0;
        SQLLEN resLen = 0;
        ret = SQLBindCol(stmt, 1, SQL_INTEGER, &res, 0, &resLen);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFetch(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_REQUIRE_EQUAL(res, 1);

        ret = SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        stmt = NULL;
    }
}

BOOST_AUTO_TEST_CASE(TestManyCursorsTwoSelects1)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    for (int32_t i = 0; i < 1000; ++i)
    {
        SQLCHAR req[] = "SELECT 1; SELECT 2";

        SQLRETURN ret = SQLExecDirect(stmt, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFreeStmt(stmt, SQL_CLOSE);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }
}

BOOST_AUTO_TEST_CASE(TestManyCursorsTwoSelects2)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    for (int32_t i = 0; i < 1000; ++i)
    {
        SQLCHAR req[] = "SELECT 1; SELECT 2;";

        SQLRETURN ret = SQLExecDirect(stmt, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLMoreResults(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFreeStmt(stmt, SQL_CLOSE);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }
}

BOOST_AUTO_TEST_CASE(TestManyCursorsSelectMerge1)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    for (int32_t i = 0; i < 1000; ++i)
    {
        SQLCHAR req[] = "SELECT 1; MERGE into TestType(_key) values(2)";

        SQLRETURN ret = SQLExecDirect(stmt, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFreeStmt(stmt, SQL_CLOSE);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }
}

BOOST_AUTO_TEST_CASE(TestManyCursorsSelectMerge2)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    for (int32_t i = 0; i < 1000; ++i)
    {
        SQLCHAR req[] = "SELECT 1; MERGE into TestType(_key) values(2)";

        SQLRETURN ret = SQLExecDirect(stmt, req, SQL_NTS);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLMoreResults(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFreeStmt(stmt, SQL_CLOSE);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
    }
}

BOOST_AUTO_TEST_SUITE_END()
