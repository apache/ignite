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

#include <boost/test/unit_test.hpp>

#include <ignite/network/socket_client.h>

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/impl/binary/binary_utils.h"

#include "ignite/odbc/connection.h"

#include "test_type.h"
#include "test_utils.h"
#include "odbc_test_suite.h"

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
struct AttributesTestSuiteFixture : odbc::OdbcTestSuite
{
    /**
     * Constructor.
     */
    AttributesTestSuiteFixture()
    {
        grid = StartPlatformNode("queries-test.xml", "NodeMain");
    }

    /**
     * Destructor.
     */
    ~AttributesTestSuiteFixture()
    {
        Ignition::StopAll(true);
    }

    /** Node started during the test. */
    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(AttributesTestSuite, AttributesTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestLegacyConnection)
{
    Connect("DRIVER={Apache Ignite};SERVER=127.0.0.1;PORT=11110;SCHEMA=cache");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionProtocolVersion_2_1_0)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PROTOCOL_VERSION=2.1.0");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionProtocolVersion_2_1_5)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PROTOCOL_VERSION=2.1.5");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionProtocolVersion_2_3_0)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PROTOCOL_VERSION=2.3.0");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionProtocolVersion_2_3_2)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PROTOCOL_VERSION=2.3.2");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionProtocolVersion_2_5_0)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PROTOCOL_VERSION=2.5.0");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionProtocolVersion_2_7_0)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PROTOCOL_VERSION=2.7.0");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionProtocolVersion_2_8_0)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache;PROTOCOL_VERSION=2.8.0");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionRangeBegin)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110..11115;SCHEMA=cache");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionRangeEnd)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11108..11110;SCHEMA=cache");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionRangeMiddle)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11108..11115;SCHEMA=cache");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionMultipleAddresses)
{
    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:4242,127.0.0.1:11109..11115,127.0.0.1;SCHEMA=cache");

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

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

BOOST_AUTO_TEST_CASE(ConnectionAttributeConnectionTimeout)
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

BOOST_AUTO_TEST_CASE(ConnectionAttributeLoginTimeout)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLUINTEGER timeout = -1;
    SQLRETURN ret = SQLGetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, &timeout, 0, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);
    BOOST_REQUIRE_EQUAL(timeout, (SQLUINTEGER) odbc::Connection::DEFAULT_CONNECT_TIMEOUT);

    ret = SQLSetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, reinterpret_cast<SQLPOINTER>(42), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    timeout = -1;

    ret = SQLGetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, &timeout, 0, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);
    BOOST_REQUIRE_EQUAL(timeout, 42);
}

/**
 * Check that environment returns expected version of ODBC standard.
 *
 * 1. Start node.
 * 2. Establish connection using ODBC driver.
 * 3. Get current ODBC version from env handle.
 * 4. Check that version is of the expected value.
 */
BOOST_AUTO_TEST_CASE(TestSQLGetEnvAttrDriverVersion)
{
    Connect("DRIVER={Apache Ignite};address=127.0.0.1:11110;schema=cache");

    SQLINTEGER version;
    SQLRETURN ret = SQLGetEnvAttr(env, SQL_ATTR_ODBC_VERSION, &version, 0, 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_ENV, env);

    BOOST_CHECK_EQUAL(version, SQL_OV_ODBC3);
}

BOOST_AUTO_TEST_SUITE_END()
