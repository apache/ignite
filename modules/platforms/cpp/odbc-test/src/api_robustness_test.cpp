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
#include "test_utils.h"

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;
using namespace ignite_test;

using namespace boost::unit_test;

using ignite::impl::binary::BinaryUtils;

/**
 * Test setup fixture.
 */
struct ApiRobustnessTestSuiteFixture
{
    void Prepare()
    {
        // Allocate an environment handle
        SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

        BOOST_REQUIRE(env != NULL);

        // We want ODBC 3 support
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

        // Allocate a connection handle
        SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

        BOOST_REQUIRE(dbc != NULL);
    }

    /**
     * Establish connection to node.
     *
     * @param connectStr Connection string.
     */
    void Connect(const std::string& connectStr)
    {
        Prepare();

        // Connect string
        std::vector<SQLCHAR> connectStr0;

        connectStr0.reserve(connectStr.size() + 1);
        std::copy(connectStr.begin(), connectStr.end(), std::back_inserter(connectStr0));

        SQLCHAR outstr[ODBC_BUFFER_SIZE];
        SQLSMALLINT outstrlen;

        // Connecting to ODBC server.
        SQLRETURN ret = SQLDriverConnect(dbc, NULL, &connectStr0[0], static_cast<SQLSMALLINT>(connectStr0.size()),
            outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

        ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

        // Allocate a statement handle
        SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

        BOOST_REQUIRE(stmt != NULL);
    }

    void Disconnect()
    {
        // Releasing statement handle.
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);

        // Disconneting from the server.
        SQLDisconnect(dbc);

        // Releasing allocated handles.
        SQLFreeHandle(SQL_HANDLE_DBC, dbc);
        SQLFreeHandle(SQL_HANDLE_ENV, env);
    }

    static Ignite StartAdditionalNode(const char* name)
    {
        return StartNode("queries-test-noodbc.xml", name);
    }

    /**
     * Constructor.
     */
    ApiRobustnessTestSuiteFixture() :
        testCache(0),
        env(NULL),
        dbc(NULL),
        stmt(NULL)
    {
        grid = StartNode("queries-test.xml", "NodeMain");

        testCache = grid.GetCache<int64_t, TestType>("cache");
    }

    /**
     * Check that SQLFetchScroll does not crash with unsupported orientation.
     *
     * @param orientation Fetch orientation.
     */
    void CheckFetchScrollUnsupportedOrientation(SQLUSMALLINT orientation)
    {
        Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;CACHE=cache");

        SQLRETURN ret;

        const int64_t recordsNum = 100;

        for (int i = 0; i < recordsNum; ++i)
        {
            TestType val;

            val.i32Field = i * 10;

            testCache.Put(i, val);
        }

        int32_t i32Field = -1;

        // Binding column.
        ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &i32Field, 0, 0);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        SQLCHAR request[] = "SELECT i32Field FROM TestType ORDER BY _key";

        ret = SQLExecDirect(stmt, request, SQL_NTS);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFetchScroll(stmt, SQL_FETCH_NEXT, 0);
        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        BOOST_CHECK_EQUAL(i32Field, 0);

        ret = SQLFetchScroll(stmt, orientation, 0);

        // Operation is not supported. However, there should be no crash.
        BOOST_CHECK(ret == SQL_ERROR);

        CheckSQLStatementDiagnosticError("HY106");
    }

    void CheckSQLDiagnosticError(int16_t handleType, SQLHANDLE handle, const std::string& expectSqlState)
    {
        SQLCHAR state[ODBC_BUFFER_SIZE];
        SQLINTEGER nativeError = 0;
        SQLCHAR message[ODBC_BUFFER_SIZE];
        SQLSMALLINT messageLen = 0;

        SQLRETURN ret = SQLGetDiagRec(handleType, handle, 1, state, &nativeError, message, sizeof(message), &messageLen);

        const std::string sqlState = reinterpret_cast<char*>(state);
        BOOST_REQUIRE_EQUAL(ret, SQL_SUCCESS);
        BOOST_REQUIRE_EQUAL(sqlState, expectSqlState);
        BOOST_REQUIRE(messageLen > 0);
    }

    void CheckSQLStatementDiagnosticError(const std::string& expectSqlState)
    {
        CheckSQLDiagnosticError(SQL_HANDLE_STMT, stmt, expectSqlState);
    }

    void CheckSQLConnectionDiagnosticError(const std::string& expectSqlState)
    {
        CheckSQLDiagnosticError(SQL_HANDLE_DBC, dbc, expectSqlState);
    }

    /**
     * Destructor.
     */
    ~ApiRobustnessTestSuiteFixture()
    {
        Disconnect();

        Ignition::StopAll(true);
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

SQLSMALLINT unsupportedC[] = {
        SQL_C_INTERVAL_YEAR,
        SQL_C_INTERVAL_MONTH,
        SQL_C_INTERVAL_DAY,
        SQL_C_INTERVAL_HOUR,
        SQL_C_INTERVAL_MINUTE,
        SQL_C_INTERVAL_SECOND,
        SQL_C_INTERVAL_YEAR_TO_MONTH,
        SQL_C_INTERVAL_DAY_TO_HOUR,
        SQL_C_INTERVAL_DAY_TO_MINUTE,
        SQL_C_INTERVAL_DAY_TO_SECOND,
        SQL_C_INTERVAL_HOUR_TO_MINUTE,
        SQL_C_INTERVAL_HOUR_TO_SECOND,
        SQL_C_INTERVAL_MINUTE_TO_SECOND
    };

SQLSMALLINT unsupportedSql[] = {
        SQL_WVARCHAR,
        SQL_WLONGVARCHAR,
        SQL_REAL,
        SQL_NUMERIC,
        SQL_TYPE_TIME,
        SQL_INTERVAL_MONTH,
        SQL_INTERVAL_YEAR,
        SQL_INTERVAL_YEAR_TO_MONTH,
        SQL_INTERVAL_DAY,
        SQL_INTERVAL_HOUR,
        SQL_INTERVAL_MINUTE,
        SQL_INTERVAL_SECOND,
        SQL_INTERVAL_DAY_TO_HOUR,
        SQL_INTERVAL_DAY_TO_MINUTE,
        SQL_INTERVAL_DAY_TO_SECOND,
        SQL_INTERVAL_HOUR_TO_MINUTE,
        SQL_INTERVAL_HOUR_TO_SECOND,
        SQL_INTERVAL_MINUTE_TO_SECOND
    };

BOOST_FIXTURE_TEST_SUITE(ApiRobustnessTestSuite, ApiRobustnessTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestSQLDriverConnect)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Prepare();

    SQLCHAR connectStr[] = "DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache";

    SQLCHAR outStr[ODBC_BUFFER_SIZE];
    SQLSMALLINT outStrLen;

    // Normal connect.
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, connectStr, sizeof(connectStr),
        outStr, sizeof(outStr), &outStrLen, SQL_DRIVER_COMPLETE);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLDisconnect(dbc);

    // Null out string resulting length.
    SQLDriverConnect(dbc, NULL, connectStr, sizeof(connectStr), outStr, sizeof(outStr), 0, SQL_DRIVER_COMPLETE);

    SQLDisconnect(dbc);

    // Null out string buffer length.
    SQLDriverConnect(dbc, NULL, connectStr, sizeof(connectStr), outStr, 0, &outStrLen, SQL_DRIVER_COMPLETE);

    SQLDisconnect(dbc);

    // Null out string.
    SQLDriverConnect(dbc, NULL, connectStr, sizeof(connectStr), 0, sizeof(outStr), &outStrLen, SQL_DRIVER_COMPLETE);

    SQLDisconnect(dbc);

    // Null all.
    SQLDriverConnect(dbc, NULL, connectStr, sizeof(connectStr), 0, 0, 0, SQL_DRIVER_COMPLETE);

    SQLDisconnect(dbc);
}

BOOST_AUTO_TEST_CASE(TestSQLConnect)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLSMALLINT resLen = 0;

    // Everyting is ok.
    SQLRETURN ret = SQLGetInfo(dbc, SQL_DRIVER_NAME, buffer, ODBC_BUFFER_SIZE, &resLen);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    // Resulting length is null.
    SQLGetInfo(dbc, SQL_DRIVER_NAME, buffer, ODBC_BUFFER_SIZE, 0);

    // Buffer length is null.
    SQLGetInfo(dbc, SQL_DRIVER_NAME, buffer, 0, &resLen);

    // Buffer is null.
    SQLGetInfo(dbc, SQL_DRIVER_NAME, 0, ODBC_BUFFER_SIZE, &resLen);

    // Unknown info.
    SQLGetInfo(dbc, -1, buffer, ODBC_BUFFER_SIZE, &resLen);

    // All nulls.
    SQLGetInfo(dbc, SQL_DRIVER_NAME, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLPrepare)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    // Everyting is ok.
    SQLRETURN ret = SQLPrepare(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLCloseCursor(stmt);

    // Value length is null.
    SQLPrepare(stmt, sql, 0);

    SQLCloseCursor(stmt);

    // Value is null.
    SQLPrepare(stmt, 0, sizeof(sql));

    SQLCloseCursor(stmt);

    // All nulls.
    SQLPrepare(stmt, 0, 0);

    SQLCloseCursor(stmt);
}

BOOST_AUTO_TEST_CASE(TestSQLExecDirect)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    // Everyting is ok.
    SQLRETURN ret = SQLExecDirect(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLCloseCursor(stmt);

    // Value length is null.
    SQLExecDirect(stmt, sql, 0);

    SQLCloseCursor(stmt);

    // Value is null.
    SQLExecDirect(stmt, 0, sizeof(sql));

    SQLCloseCursor(stmt);

    // All nulls.
    SQLExecDirect(stmt, 0, 0);

    SQLCloseCursor(stmt);
}

BOOST_AUTO_TEST_CASE(TestSQLExtendedFetch)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    for (int i = 0; i < 100; ++i)
    {
        TestType obj;

        obj.strField = LexicalCast<std::string>(i);

        testCache.Put(i, obj);
    }

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    SQLRETURN ret = SQLExecDirect(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLULEN rowCount;
    SQLUSMALLINT rowStatus[16];

    // Everyting is ok.
    ret = SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, rowStatus);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Row count is null.
    SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, 0, rowStatus);

    // Row statuses is null.
    SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, &rowCount, 0);

    // All nulls.
    SQLExtendedFetch(stmt, SQL_FETCH_NEXT, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLNumResultCols)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    for (int i = 0; i < 100; ++i)
    {
        TestType obj;

        obj.strField = LexicalCast<std::string>(i);

        testCache.Put(i, obj);
    }

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    SQLRETURN ret = SQLExecDirect(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLSMALLINT columnCount;

    // Everyting is ok.
    ret = SQLNumResultCols(stmt, &columnCount);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Column count is null.
    SQLNumResultCols(stmt, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLTables)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR catalogName[] = "";
    SQLCHAR schemaName[] = "";
    SQLCHAR tableName[] = "";
    SQLCHAR tableType[] = "";

    // Everithing is ok.
    SQLRETURN ret = SQLTables(stmt, catalogName, sizeof(catalogName), schemaName,
        sizeof(schemaName), tableName, sizeof(tableName), tableType, sizeof(tableType));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Sizes are nulls.
    SQLTables(dbc, catalogName, 0, schemaName, 0, tableName, 0, tableType, 0);

    // Values are nulls.
    SQLTables(dbc, 0, sizeof(catalogName), 0, sizeof(schemaName), 0, sizeof(tableName), 0, sizeof(tableType));

    // All nulls.
    SQLTables(dbc, 0, 0, 0, 0, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLColumns)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR catalogName[] = "";
    SQLCHAR schemaName[] = "";
    SQLCHAR tableName[] = "";
    SQLCHAR columnName[] = "";

    // Everithing is ok.
    SQLRETURN ret = SQLColumns(stmt, catalogName, sizeof(catalogName), schemaName,
        sizeof(schemaName), tableName, sizeof(tableName), columnName, sizeof(columnName));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Sizes are nulls.
    SQLColumns(dbc, catalogName, 0, schemaName, 0, tableName, 0, columnName, 0);

    // Values are nulls.
    SQLColumns(dbc, 0, sizeof(catalogName), 0, sizeof(schemaName), 0, sizeof(tableName), 0, sizeof(columnName));

    // All nulls.
    SQLColumns(dbc, 0, 0, 0, 0, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLBindCol)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLINTEGER ind1;
    SQLLEN len1 = 0;

    // Everithing is ok.
    SQLRETURN ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &ind1, sizeof(ind1), &len1);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    //Unsupported data types
    for(int i = 0; i < sizeof(unsupportedC)/sizeof(unsupportedC[0]); ++i)
    {
        ret = SQLBindCol(stmt, 1, unsupportedC[i], &ind1, sizeof(ind1), &len1);
        BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
        CheckSQLStatementDiagnosticError("HY003");
    }

    // Size is negative.
    ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &ind1, -1, &len1);
    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
    CheckSQLStatementDiagnosticError("HY090");

    // Size is null.
    SQLBindCol(stmt, 1, SQL_C_SLONG, &ind1, 0, &len1);

    // Res size is null.
    SQLBindCol(stmt, 2, SQL_C_SLONG, &ind1, sizeof(ind1), 0);

    // Value is null.
    SQLBindCol(stmt, 3, SQL_C_SLONG, 0, sizeof(ind1), &len1);

    // All nulls.
    SQLBindCol(stmt, 4, SQL_C_SLONG, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLBindParameter)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLINTEGER ind1;
    SQLLEN len1 = 0;

    // Everithing is ok.
    SQLRETURN ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT,
        SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), &len1);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    //Unsupported parameter type : output
    SQLBindParameter(stmt, 2, SQL_PARAM_OUTPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), &len1);
    CheckSQLStatementDiagnosticError("HY105");

    //Unsupported parameter type : input/output
    SQLBindParameter(stmt, 2, SQL_PARAM_INPUT_OUTPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), &len1);
    CheckSQLStatementDiagnosticError("HY105");


    //Unsupported data types
    for(int i = 0; i < sizeof(unsupportedSql)/sizeof(unsupportedSql[0]); ++i)
    {
        ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG, unsupportedSql[i], 100, 100, &ind1, sizeof(ind1), &len1);
        BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
        CheckSQLStatementDiagnosticError("HYC00");
    }


    // Size is null.
    SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, 0, &len1);

    // Res size is null.
    SQLBindParameter(stmt, 3, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, &ind1, sizeof(ind1), 0);

    // Value is null.
    SQLBindParameter(stmt, 4, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, 0, sizeof(ind1), &len1);

    // All nulls.
    SQLBindParameter(stmt, 5, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_INTEGER, 100, 100, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLNativeSql)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";
    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLINTEGER resLen = 0;

    // Everithing is ok.
    SQLRETURN ret = SQLNativeSql(dbc, sql, sizeof(sql), buffer, sizeof(buffer), &resLen);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Value size is null.
    SQLNativeSql(dbc, sql, 0, buffer, sizeof(buffer), &resLen);

    // Buffer size is null.
    SQLNativeSql(dbc, sql, sizeof(sql), buffer, 0, &resLen);

    // Res size is null.
    SQLNativeSql(dbc, sql, sizeof(sql), buffer, sizeof(buffer), 0);

    // Value is null.
    SQLNativeSql(dbc, sql, 0, buffer, sizeof(buffer), &resLen);

    // Buffer is null.
    SQLNativeSql(dbc, sql, sizeof(sql), 0, sizeof(buffer), &resLen);

    // All nulls.
    SQLNativeSql(dbc, sql, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLColAttribute)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    SQLRETURN ret = SQLExecDirect(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLSMALLINT resLen = 0;
    SQLLEN numericAttr = 0;

    // Everithing is ok. Character attribute.
    ret = SQLColAttribute(stmt, 1, SQL_COLUMN_TABLE_NAME, buffer, sizeof(buffer), &resLen, &numericAttr);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    // Everithing is ok. Numeric attribute.
    ret = SQLColAttribute(stmt, 1, SQL_DESC_COUNT, buffer, sizeof(buffer), &resLen, &numericAttr);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLColAttribute(stmt, 1, SQL_COLUMN_TABLE_NAME, buffer, sizeof(buffer), &resLen, 0);
    SQLColAttribute(stmt, 1, SQL_COLUMN_TABLE_NAME, buffer, sizeof(buffer), 0, &numericAttr);
    SQLColAttribute(stmt, 1, SQL_COLUMN_TABLE_NAME, buffer, 0, &resLen, &numericAttr);
    SQLColAttribute(stmt, 1, SQL_COLUMN_TABLE_NAME, 0, sizeof(buffer), &resLen, &numericAttr);
    SQLColAttribute(stmt, 1, SQL_COLUMN_TABLE_NAME, 0, 0, 0, 0);

    SQLColAttribute(stmt, 1, SQL_DESC_COUNT, buffer, sizeof(buffer), &resLen, 0);
    SQLColAttribute(stmt, 1, SQL_DESC_COUNT, buffer, sizeof(buffer), 0, &numericAttr);
    SQLColAttribute(stmt, 1, SQL_DESC_COUNT, buffer, 0, &resLen, &numericAttr);
    SQLColAttribute(stmt, 1, SQL_DESC_COUNT, 0, sizeof(buffer), &resLen, &numericAttr);
    SQLColAttribute(stmt, 1, SQL_DESC_COUNT, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLDescribeCol)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    SQLRETURN ret = SQLExecDirect(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLCHAR columnName[ODBC_BUFFER_SIZE];
    SQLSMALLINT columnNameLen = 0;
    SQLSMALLINT dataType = 0;
    SQLULEN columnSize = 0;
    SQLSMALLINT decimalDigits = 0;
    SQLSMALLINT nullable = 0;

    // Everithing is ok.
    ret = SQLDescribeCol(stmt, 1, columnName, sizeof(columnName),
        &columnNameLen, &dataType, &columnSize, &decimalDigits, &nullable);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLDescribeCol(stmt, 1, 0, sizeof(columnName), &columnNameLen, &dataType, &columnSize, &decimalDigits, &nullable);
    SQLDescribeCol(stmt, 1, columnName, 0, &columnNameLen, &dataType, &columnSize, &decimalDigits, &nullable);
    SQLDescribeCol(stmt, 1, columnName, sizeof(columnName), 0, &dataType, &columnSize, &decimalDigits, &nullable);
    SQLDescribeCol(stmt, 1, columnName, sizeof(columnName), &columnNameLen, 0, &columnSize, &decimalDigits, &nullable);
    SQLDescribeCol(stmt, 1, columnName, sizeof(columnName), &columnNameLen, &dataType, 0, &decimalDigits, &nullable);
    SQLDescribeCol(stmt, 1, columnName, sizeof(columnName), &columnNameLen, &dataType, &columnSize, 0, &nullable);
    SQLDescribeCol(stmt, 1, columnName, sizeof(columnName), &columnNameLen, &dataType, &columnSize, &decimalDigits, 0);
    SQLDescribeCol(stmt, 1, 0, 0, 0, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLRowCount)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    SQLRETURN ret = SQLExecDirect(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLLEN rows = 0;

    // Everithing is ok.
    ret = SQLRowCount(stmt, &rows);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLRowCount(stmt, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLForeignKeys)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR catalogName[] = "";
    SQLCHAR schemaName[] = "cache";
    SQLCHAR tableName[] = "TestType";

    // Everithing is ok.
    SQLRETURN ret = SQLForeignKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName),
        tableName, sizeof(tableName), catalogName, sizeof(catalogName),
        schemaName, sizeof(schemaName), tableName, sizeof(tableName));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, 0, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName),
        catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName));

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, catalogName, 0, schemaName, sizeof(schemaName), tableName, sizeof(tableName),
        catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName));

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, catalogName, sizeof(catalogName), 0, sizeof(schemaName), tableName, sizeof(tableName),
        catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName));

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, catalogName, sizeof(catalogName), schemaName, 0, tableName, sizeof(tableName),
        catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName));

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), 0, sizeof(tableName),
        catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName));

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, 0, catalogName,
        sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName));

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName),
        0, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName));

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName),
        catalogName, 0, schemaName, sizeof(schemaName), tableName, sizeof(tableName));

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName),
        catalogName, sizeof(catalogName), 0, sizeof(schemaName), tableName, sizeof(tableName));

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName),
        catalogName, sizeof(catalogName), schemaName, 0, tableName, sizeof(tableName));

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName),
        catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), 0, sizeof(tableName));

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName),
        catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, 0);

    SQLCloseCursor(stmt);

    SQLForeignKeys(stmt, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    SQLCloseCursor(stmt);
}

BOOST_AUTO_TEST_CASE(TestSQLGetStmtAttr)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLINTEGER resLen = 0;

    // Everithing is ok.
    SQLRETURN ret = SQLGetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, buffer, sizeof(buffer), &resLen);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLGetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, 0, sizeof(buffer), &resLen);
    SQLGetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, buffer, 0, &resLen);
    SQLGetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, buffer, sizeof(buffer), 0);
    SQLGetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLSetStmtAttr)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLULEN val = 1;

    // Everithing is ok.
    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, reinterpret_cast<SQLPOINTER>(val), sizeof(val));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, 0, sizeof(val));
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, reinterpret_cast<SQLPOINTER>(val), 0);
    SQLSetStmtAttr(stmt, SQL_ATTR_ROW_ARRAY_SIZE, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLPrimaryKeys)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR catalogName[] = "";
    SQLCHAR schemaName[] = "cache";
    SQLCHAR tableName[] = "TestType";

    // Everithing is ok.
    SQLRETURN ret = SQLPrimaryKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName),
        tableName, sizeof(tableName));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLPrimaryKeys(stmt, 0, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, sizeof(tableName));
    SQLPrimaryKeys(stmt, catalogName, 0, schemaName, sizeof(schemaName), tableName, sizeof(tableName));
    SQLPrimaryKeys(stmt, catalogName, sizeof(catalogName), 0, sizeof(schemaName), tableName, sizeof(tableName));
    SQLPrimaryKeys(stmt, catalogName, sizeof(catalogName), schemaName, 0, tableName, sizeof(tableName));
    SQLPrimaryKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), 0, sizeof(tableName));
    SQLPrimaryKeys(stmt, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName), tableName, 0);
    SQLPrimaryKeys(stmt, 0, 0, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLNumParams)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    // Everyting is ok.
    SQLRETURN ret = SQLPrepare(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLSMALLINT params;

    // Everithing is ok.
    ret = SQLNumParams(stmt, &params);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLNumParams(stmt, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLGetDiagField)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    // Should fail.
    SQLRETURN ret = SQLGetTypeInfo(stmt, SQL_INTERVAL_MONTH);

    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLSMALLINT resLen = 0;

    // Everithing is ok
    ret = SQLGetDiagField(SQL_HANDLE_STMT, stmt, 1, SQL_DIAG_MESSAGE_TEXT, buffer, sizeof(buffer), &resLen);

    BOOST_REQUIRE_EQUAL(ret, SQL_SUCCESS);

    SQLGetDiagField(SQL_HANDLE_STMT, stmt, 1, SQL_DIAG_MESSAGE_TEXT, 0, sizeof(buffer), &resLen);
    SQLGetDiagField(SQL_HANDLE_STMT, stmt, 1, SQL_DIAG_MESSAGE_TEXT, buffer, 0, &resLen);
    SQLGetDiagField(SQL_HANDLE_STMT, stmt, 1, SQL_DIAG_MESSAGE_TEXT, buffer, sizeof(buffer), 0);
    SQLGetDiagField(SQL_HANDLE_STMT, stmt, 1, SQL_DIAG_MESSAGE_TEXT, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLGetDiagRec)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    // Should fail.
    SQLRETURN ret = SQLGetTypeInfo(stmt, SQL_INTERVAL_MONTH);

    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);

    SQLCHAR state[ODBC_BUFFER_SIZE];
    SQLINTEGER nativeError = 0;
    SQLCHAR message[ODBC_BUFFER_SIZE];
    SQLSMALLINT messageLen = 0;

    // Everithing is ok
    ret = SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, state, &nativeError, message, sizeof(message), &messageLen);

    BOOST_REQUIRE_EQUAL(ret, SQL_SUCCESS);

    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, 0, &nativeError, message, sizeof(message), &messageLen);
    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, state, 0, message, sizeof(message), &messageLen);
    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, state, &nativeError, 0, sizeof(message), &messageLen);
    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, state, &nativeError, message, 0, &messageLen);
    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, state, &nativeError, message, sizeof(message), 0);
    SQLGetDiagRec(SQL_HANDLE_STMT, stmt, 1, 0, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLGetData)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    for (int i = 0; i < 100; ++i)
    {
        TestType obj;

        obj.strField = LexicalCast<std::string>(i);

        testCache.Put(i, obj);
    }

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR sql[] = "SELECT strField FROM TestType";

    SQLRETURN ret = SQLExecDirect(stmt, sql, sizeof(sql));

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLFetch(stmt);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLLEN resLen = 0;

    // Everything is ok.
    ret = SQLGetData(stmt, 1, SQL_C_CHAR, buffer, sizeof(buffer), &resLen);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLFetch(stmt);

    SQLGetData(stmt, 1, SQL_C_CHAR, 0, sizeof(buffer), &resLen);

    SQLFetch(stmt);

    SQLGetData(stmt, 1, SQL_C_CHAR, buffer, 0, &resLen);

    SQLFetch(stmt);

    SQLGetData(stmt, 1, SQL_C_CHAR, buffer, sizeof(buffer), 0);

    SQLFetch(stmt);

    SQLGetData(stmt, 1, SQL_C_CHAR, 0, 0, 0);

    SQLFetch(stmt);
}

BOOST_AUTO_TEST_CASE(TestSQLGetEnvAttr)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR buffer[ODBC_BUFFER_SIZE];
    SQLINTEGER resLen = 0;

    // Everything is ok.
    SQLRETURN ret = SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, buffer, sizeof(buffer), &resLen);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, 0, sizeof(buffer), &resLen);
    SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, buffer, 0, &resLen);
    SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, buffer, sizeof(buffer), 0);
    SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLSpecialColumns)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR catalogName[] = "";
    SQLCHAR schemaName[] = "cache";
    SQLCHAR tableName[] = "TestType";

    // Everything is ok.
    SQLRETURN ret = SQLSpecialColumns(stmt, SQL_BEST_ROWID, catalogName, sizeof(catalogName),
        schemaName, sizeof(schemaName), tableName, sizeof(tableName), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    SQLCloseCursor(stmt);

    SQLSpecialColumns(stmt, SQL_BEST_ROWID, 0, sizeof(catalogName), schemaName, sizeof(schemaName), tableName,
        sizeof(tableName), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(stmt);

    SQLSpecialColumns(stmt, SQL_BEST_ROWID, catalogName, 0, schemaName, sizeof(schemaName), tableName,
        sizeof(tableName), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(stmt);

    SQLSpecialColumns(stmt, SQL_BEST_ROWID, catalogName, sizeof(catalogName), 0, sizeof(schemaName), tableName,
        sizeof(tableName), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(stmt);

    SQLSpecialColumns(stmt, SQL_BEST_ROWID, catalogName, sizeof(catalogName), schemaName, 0, tableName,
        sizeof(tableName), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(stmt);

    SQLSpecialColumns(stmt, SQL_BEST_ROWID, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName),
        0, sizeof(tableName), SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(stmt);

    SQLSpecialColumns(stmt, SQL_BEST_ROWID, catalogName, sizeof(catalogName), schemaName, sizeof(schemaName),
        tableName, 0, SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(stmt);

    SQLSpecialColumns(stmt, SQL_BEST_ROWID, 0, 0, 0, 0, 0, 0, SQL_SCOPE_CURROW, SQL_NO_NULLS);

    SQLCloseCursor(stmt);
}

BOOST_AUTO_TEST_CASE(TestFetchScrollLast)
{
    CheckFetchScrollUnsupportedOrientation(SQL_FETCH_LAST);
}

BOOST_AUTO_TEST_CASE(TestFetchScrollPrior)
{
    CheckFetchScrollUnsupportedOrientation(SQL_FETCH_PRIOR);
}

BOOST_AUTO_TEST_CASE(TestFetchScrollFirst)
{
    CheckFetchScrollUnsupportedOrientation(SQL_FETCH_FIRST);
}

BOOST_AUTO_TEST_CASE(TestSQLError)
{
    // There are no checks because we do not really care what is the result of these
    // calls as long as they do not cause segmentation fault.

    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLCHAR state[6] = { 0 };
    SQLINTEGER nativeCode = 0;
    SQLCHAR message[ODBC_BUFFER_SIZE] = { 0 };
    SQLSMALLINT messageLen = 0;

    // Everything is ok.
    SQLRETURN ret = SQLError(env, 0, 0, state, &nativeCode, message, sizeof(message), &messageLen);

    if (ret != SQL_SUCCESS && ret != SQL_NO_DATA)
        BOOST_FAIL("Unexpected error");

    ret = SQLError(0, dbc, 0, state, &nativeCode, message, sizeof(message), &messageLen);

    if (ret != SQL_SUCCESS && ret != SQL_NO_DATA)
        BOOST_FAIL("Unexpected error");

    ret = SQLError(0, 0, stmt, state, &nativeCode, message, sizeof(message), &messageLen);

    if (ret != SQL_SUCCESS && ret != SQL_NO_DATA)
        BOOST_FAIL("Unexpected error");

    SQLError(0, 0, 0, state, &nativeCode, message, sizeof(message), &messageLen);

    SQLError(0, 0, stmt, 0, &nativeCode, message, sizeof(message), &messageLen);

    SQLError(0, 0, stmt, state, 0, message, sizeof(message), &messageLen);

    SQLError(0, 0, stmt, state, &nativeCode, 0, sizeof(message), &messageLen);

    SQLError(0, 0, stmt, state, &nativeCode, message, 0, &messageLen);

    SQLError(0, 0, stmt, state, &nativeCode, message, sizeof(message), 0);

    SQLError(0, 0, stmt, 0, 0, 0, 0, 0);

    SQLError(0, 0, 0, 0, 0, 0, 0, 0);
}

BOOST_AUTO_TEST_CASE(TestSQLDiagnosticRecords)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;cache=cache");

    SQLHANDLE hnd;
    SQLRETURN ret;

    ret = SQLAllocHandle(SQL_HANDLE_DESC, dbc, &hnd);
    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
    CheckSQLConnectionDiagnosticError("IM001");

    ret = SQLFreeStmt(stmt, 4);
    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
    CheckSQLStatementDiagnosticError("HY092");
}

BOOST_AUTO_TEST_SUITE_END()
