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

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include "ignite/ignition.h"

#include "ignite/common/fixed_size_array.h"
#include "ignite/impl/binary/binary_utils.h"

#include "test_type.h"
#include "complex_type.h"
#include "test_utils.h"

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;
using namespace ignite_test;
using namespace ignite::binary;
using namespace ignite::impl::binary;
using namespace ignite::impl::interop;

using namespace boost::unit_test;

/**
 * Test setup fixture.
 */
struct MetaQueriesTestSuiteFixture 
{
    /**
     * Establish connection to node.
     *
     * @param connectStr Connection string.
     */
    void Connect(const std::string& connectStr)
    {
        // Allocate an environment handle
        SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

        BOOST_REQUIRE(env != NULL);

        // We want ODBC 3 support
        SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, reinterpret_cast<void*>(SQL_OV_ODBC3), 0);

        // Allocate a connection handle
        SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);

        BOOST_REQUIRE(dbc != NULL);

        // Connect string
        std::vector<SQLCHAR> connectStr0;

        connectStr0.reserve(connectStr.size() + 1);
        std::copy(connectStr.begin(), connectStr.end(), std::back_inserter(connectStr0));

        SQLCHAR outstr[ODBC_BUFFER_SIZE];
        SQLSMALLINT outstrlen;

        // Connecting to ODBC server.
        SQLRETURN ret = SQLDriverConnect(dbc, NULL, &connectStr0[0], static_cast<SQLSMALLINT>(connectStr0.size()),
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
     * Disconnect.
     */
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

    /**
     * Start additional node with the specified name.
     *
     * @param name Node name.
     */
    static Ignite StartAdditionalNode(const char* name)
    {
#ifdef IGNITE_TESTS_32
        return StartNode("queries-test-noodbc-32.xml", name);
#else
        return StartNode("queries-test-noodbc.xml", name);
#endif
    }

    /**
     * Checks single row result set for correct work with SQLGetData.
     *
     * @param stmt Statement.
     */
    void CheckSingleRowResultSetWithGetData(SQLHSTMT stmt)
    {
        SQLRETURN ret = SQLFetch(stmt);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        char buf[1024];
        SQLLEN bufLen = sizeof(buf);

        ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &bufLen);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        ret = SQLFetch(stmt);

        BOOST_REQUIRE_EQUAL(ret, SQL_NO_DATA);

        ret = SQLGetData(stmt, 1, SQL_C_CHAR, buf, sizeof(buf), &bufLen);

        BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
        BOOST_CHECK_EQUAL(GetOdbcErrorState(SQL_HANDLE_STMT, stmt), "24000");
    }

    /**
     * Check string column.
     *
     * @param stmt Statement.
     * @param colId Column ID to check.
     * @param value Expected value.
     */
    void CheckStringColumn(SQLHSTMT stmt, int colId, const std::string& value)
    {
        char buf[1024];
        SQLLEN bufLen = sizeof(buf);

        SQLRETURN ret = SQLGetData(stmt, colId, SQL_C_CHAR, buf, sizeof(buf), &bufLen);

        if (!SQL_SUCCEEDED(ret))
            BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

        if (bufLen <= 0)
            BOOST_CHECK(value.empty());
        else
            BOOST_CHECK_EQUAL(std::string(buf, static_cast<size_t>(bufLen)), value);
    }

    /**
     * Constructor.
     */
    MetaQueriesTestSuiteFixture() :
        cache1(0),
        cache2(0),
        env(NULL),
        dbc(NULL),
        stmt(NULL)
    {
#ifdef IGNITE_TESTS_32
        grid = StartNode("queries-test-32.xml", "NodeMain");
#else
        grid = StartNode("queries-test.xml", "NodeMain");
#endif

        cache1 = grid.GetCache<int64_t, TestType>("cache");
        cache2 = grid.GetCache<int64_t, ComplexType>("cache2");
    }

    /**
     * Destructor.
     */
    ~MetaQueriesTestSuiteFixture()
    {
        Disconnect();

        Ignition::StopAll(true);
    }

    /** Node started during the test. */
    Ignite grid;

    /** Frist cache instance. */
    Cache<int64_t, TestType> cache1;

    /** Second cache instance. */
    Cache<int64_t, ComplexType> cache2;

    /** ODBC Environment. */
    SQLHENV env;

    /** ODBC Connect. */
    SQLHDBC dbc;

    /** ODBC Statement. */
    SQLHSTMT stmt;
};

BOOST_FIXTURE_TEST_SUITE(MetaQueriesTestSuite, MetaQueriesTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestGetTypeInfoAllTypes)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLGetTypeInfo(stmt, SQL_ALL_TYPES);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
}

BOOST_AUTO_TEST_CASE(TestColAttributesColumnLength)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLCHAR req[] = "select strField from TestType";
    SQLExecDirect(stmt, req, SQL_NTS);

    SQLLEN intVal;
    SQLCHAR strBuf[1024];
    SQLSMALLINT strLen;

    SQLRETURN ret = SQLColAttribute(stmt, 1, SQL_COLUMN_LENGTH, strBuf, sizeof(strBuf), &strLen, &intVal);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
}

BOOST_AUTO_TEST_CASE(TestColAttributesColumnPresicion)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLCHAR req[] = "select strField from TestType";
    SQLExecDirect(stmt, req, SQL_NTS);

    SQLLEN intVal;
    SQLCHAR strBuf[1024];
    SQLSMALLINT strLen;

    SQLRETURN ret = SQLColAttribute(stmt, 1, SQL_COLUMN_PRECISION, strBuf, sizeof(strBuf), &strLen, &intVal);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
}

BOOST_AUTO_TEST_CASE(TestColAttributesColumnScale)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLCHAR req[] = "select strField from TestType";
    SQLExecDirect(stmt, req, SQL_NTS);

    SQLLEN intVal;
    SQLCHAR strBuf[1024];
    SQLSMALLINT strLen;

    SQLRETURN ret = SQLColAttribute(stmt, 1, SQL_COLUMN_SCALE, strBuf, sizeof(strBuf), &strLen, &intVal);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));
}

BOOST_AUTO_TEST_CASE(TestGetDataWithGetTypeInfo)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLRETURN ret = SQLGetTypeInfo(stmt, SQL_VARCHAR);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    CheckSingleRowResultSetWithGetData(stmt);
}

BOOST_AUTO_TEST_CASE(TestGetDataWithTables)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLCHAR empty[] = "";
    SQLCHAR table[] = "TestType";

    SQLRETURN ret = SQLTables(stmt, empty, SQL_NTS, empty, SQL_NTS, table, SQL_NTS, empty, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    CheckSingleRowResultSetWithGetData(stmt);
}

BOOST_AUTO_TEST_CASE(TestGetDataWithColumns)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLCHAR empty[] = "";
    SQLCHAR table[] = "TestType";
    SQLCHAR column[] = "strField";

    SQLRETURN ret = SQLColumns(stmt, empty, SQL_NTS, empty, SQL_NTS, table, SQL_NTS, column, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    CheckSingleRowResultSetWithGetData(stmt);
}

BOOST_AUTO_TEST_CASE(TestGetDataWithSelectQuery)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLCHAR insertReq[] = "insert into TestType(_key, strField) VALUES(1, 'Lorem ipsum')";
    SQLRETURN ret = SQLExecDirect(stmt, insertReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR selectReq[] = "select strField from TestType";
    ret = SQLExecDirect(stmt, selectReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    CheckSingleRowResultSetWithGetData(stmt);
}

BOOST_AUTO_TEST_CASE(TestGetInfoScrollOptions)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLUINTEGER val = 0;
    SQLRETURN ret = SQLGetInfo(dbc, SQL_SCROLL_OPTIONS, &val, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_DBC, dbc));

    BOOST_CHECK_NE(val, 0);
}

BOOST_AUTO_TEST_CASE(TestDdlTablesMeta)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=PUBLIC");

    SQLCHAR createTable[] = "create table TestTable(id int primary key, testColumn varchar)";
    SQLRETURN ret = SQLExecDirect(stmt, createTable, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR empty[] = "";
    SQLCHAR table[] = "TestTable";

    ret = SQLTables(stmt, empty, SQL_NTS, empty, SQL_NTS, table, SQL_NTS, empty, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    CheckStringColumn(stmt, 1, "");
    CheckStringColumn(stmt, 2, "\"PUBLIC\"");
    CheckStringColumn(stmt, 3, "TESTTABLE");
    CheckStringColumn(stmt, 4, "TABLE");

    ret = SQLFetch(stmt);

    BOOST_REQUIRE_EQUAL(ret, SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestDdlColumnsMeta)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=PUBLIC");

    SQLCHAR createTable[] = "create table TestTable(id int primary key, testColumn varchar)";
    SQLRETURN ret = SQLExecDirect(stmt, createTable, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR empty[] = "";
    SQLCHAR table[] = "TestTable";

    ret = SQLColumns(stmt, empty, SQL_NTS, empty, SQL_NTS, table, SQL_NTS, empty, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    CheckStringColumn(stmt, 1, "");
    CheckStringColumn(stmt, 2, "\"PUBLIC\"");
    CheckStringColumn(stmt, 3, "TESTTABLE");
    CheckStringColumn(stmt, 4, "ID");

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    CheckStringColumn(stmt, 1, "");
    CheckStringColumn(stmt, 2, "\"PUBLIC\"");
    CheckStringColumn(stmt, 3, "TESTTABLE");
    CheckStringColumn(stmt, 4, "TESTCOLUMN");

    ret = SQLFetch(stmt);

    BOOST_REQUIRE_EQUAL(ret, SQL_NO_DATA);
}

BOOST_AUTO_TEST_SUITE_END()
