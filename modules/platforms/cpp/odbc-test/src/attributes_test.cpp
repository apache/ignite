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
struct AttributesTestSuiteFixture
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

    /**
     * Constructor.
     */
    AttributesTestSuiteFixture() :
        env(NULL),
        dbc(NULL),
        stmt(NULL)
    {
        const char* config = NULL;

#ifdef IGNITE_TESTS_32
          config = "queries-test-32.xml";
#else
          config = "queries-test.xml";
#endif

        grid = StartNode(config, "NodeMain");
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

    void CheckSQLConnectionDiagnosticError(const std::string& expectSqlState)
    {
        CheckSQLDiagnosticError(SQL_HANDLE_DBC, dbc, expectSqlState);
    }

    /**
     * Destructor.
     */
    ~AttributesTestSuiteFixture()
    {
        Disconnect();

        Ignition::StopAll(true);
    }

    /** Node started during the test. */
    Ignite grid;

    /** ODBC Environment. */
    SQLHENV env;

    /** ODBC Connect. */
    SQLHDBC dbc;

    /** ODBC Statement. */
    SQLHSTMT stmt;
};

BOOST_FIXTURE_TEST_SUITE(AttributesTestSuite, AttributesTestSuiteFixture)

BOOST_AUTO_TEST_CASE(ConnectionAttributeConnectionDeadGet)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLUINTEGER dead = SQL_CD_TRUE;
    SQLRETURN ret;

    ret = SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_DEAD, &dead, 0, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    BOOST_REQUIRE_EQUAL(dead, SQL_CD_FALSE);
}

BOOST_AUTO_TEST_CASE(ConnectionAttributeConnectionDeadSet)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLUINTEGER dead = SQL_CD_TRUE;
    SQLRETURN ret;

    ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_DEAD, &dead, 0);

    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);

    // According to https://docs.microsoft.com/en-us/sql/odbc/reference/syntax/sqlsetconnectattr-function#diagnostics
    CheckSQLConnectionDiagnosticError("HY092");
}

BOOST_AUTO_TEST_CASE(StatementAttributeQueryTimeout)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLULEN timeout = -1;
    SQLRETURN ret = SQLGetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, &timeout, 0, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);
    BOOST_REQUIRE_EQUAL(timeout, 0);

    ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(7), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    timeout = -1;

    ret = SQLGetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, &timeout, 0, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);
    BOOST_REQUIRE_EQUAL(timeout, 7);
}

BOOST_AUTO_TEST_CASE(ConnetionAttributeConnectionTimeout)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLUINTEGER timeout = -1;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, 0, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);
    BOOST_REQUIRE_EQUAL(timeout, 0);

    ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(42), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    timeout = -1;

    ret = SQLGetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, &timeout, 0, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);
    BOOST_REQUIRE_EQUAL(timeout, 42);
}

BOOST_AUTO_TEST_SUITE_END()
