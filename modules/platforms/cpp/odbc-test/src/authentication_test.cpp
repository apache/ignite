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

#include "ignite/odbc/system/odbc_constants.h"

#include <vector>
#include <string>

#include <boost/regex.hpp>
#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "test_utils.h"
#include "odbc_test_suite.h"
#include "test_type.h"

using namespace ignite;
using namespace ignite::common;
using namespace ignite_test;

using namespace boost::unit_test;

namespace
{
    /** Default login. */
    const std::string defaultUser = "ignite";

    /** Default pass. */
    const std::string defaultPass = "ignite";
}

/**
 * Test setup fixture.
 */
struct AuthenticationTestSuiteFixture : odbc::OdbcTestSuite
{
    static Ignite StartAdditionalNode(const char* name)
    {
        return StartPlatformNode("queries-auth.xml", name);
    }

    /**
     * Constructor.
     */
    AuthenticationTestSuiteFixture() :
        OdbcTestSuite()
    {
        ClearLfs();

        grid = StartAdditionalNode("NodeMain");

        grid.SetActive(true);
    }

    /**
     * Destructor.
     */
    virtual ~AuthenticationTestSuiteFixture()
    {
        // No-op.
    }

    /**
     * Create connection string.
     *
     * @param user User.
     * @param pass Password.
     * @return Connection string.
     */
    static std::string MakeConnectionString(const std::string& user, const std::string& pass)
    {
        std::stringstream connectString;

        connectString <<
            "DRIVER={Apache Ignite};"
            "ADDRESS=127.0.0.1:11110;"
            "SCHEMA=cache;"
            "USER=" << user << ";"
            "PASSWORD=" << pass << ";";

        return connectString.str();
    }

    /**
     * Create default connection string.
     * @return Default connection string.
     */
    static std::string MakeDefaultConnectionString()
    {
        return MakeConnectionString(defaultUser, defaultPass);
    }

    /** Node started during the test. */
    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(AuthenticationTestSuite, AuthenticationTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestConnectionDefaultAuthSuccess)
{
    Connect(MakeDefaultConnectionString());

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

/**
 * Check that connection with UID and PWD arguments established successfully.
 *
 * 1. Start test node with configured authentication.
 * 2. Establish connection using UID and PWD arguments. Check that it established successfully.
 * 3. Check that connection can be used successfully for SQL insert and select operations.
 */
BOOST_AUTO_TEST_CASE(TestConnectionLegacyAuthSuccess)
{
    std::stringstream comp;

    comp <<
        "DRIVER={Apache Ignite};"
        "ADDRESS=127.0.0.1:11110;"
        "SCHEMA=cache;"
        "UID=" << defaultUser << ";"
        "PWD=" << defaultPass << ";";

    std::string connStr = comp.str();

    Connect(connStr);

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

/**
 * Check that connection with UID, USER, PWD and PASSWORD arguments established successfully.
 *
 * 1. Start test node with configured authentication.
 * 2. Establish connection using UID, USER, PWD and PASSWORD arguments. Check that it established successfully.
 * 3. Check that connection returns warning that password and user arguments duplicated.
 * 4. Check that connection can be used successfully for SQL insert and select operations.
 */
BOOST_AUTO_TEST_CASE(TestConnectionBothAuthSuccess)
{
    std::stringstream comp;

    comp <<
        "DRIVER={Apache Ignite};"
        "ADDRESS=127.0.0.1:11110;"
        "SCHEMA=cache;"
        "UID=" << defaultUser << ";"
        "PWD=" << defaultPass << ";"
        "USER=" << defaultUser << ";"
        "PASSWORD=" << defaultPass << ";";

    std::string connStr = comp.str();

    Prepare();

    // Connect string
    std::vector<SQLCHAR> connectStr0(connStr.begin(), connStr.end());

    SQLCHAR outstr[ODBC_BUFFER_SIZE];
    SQLSMALLINT outstrlen;

    // Connecting to ODBC server.
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, &connectStr0[0], static_cast<SQLSMALLINT>(connectStr0.size()),
        outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_DBC, dbc));

    BOOST_CHECK_EQUAL(ret, SQL_SUCCESS_WITH_INFO);

    std::string message = GetOdbcErrorMessage(SQL_HANDLE_DBC, dbc);

    BOOST_CHECK(!message.empty());

    BOOST_CHECK(message.find("01S02") != std::string::npos);
    BOOST_CHECK(message.find("Re-writing PASSWORD (have you specified it several times?") != std::string::npos);

    message = GetOdbcErrorMessage(SQL_HANDLE_DBC, dbc, 2);

    BOOST_CHECK(!message.empty());

    BOOST_CHECK(message.find("01S02") != std::string::npos);
    BOOST_CHECK(message.find("Re-writing USER (have you specified it several times?") != std::string::npos);

    // Allocate a statement handle
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);

    BOOST_REQUIRE(stmt != NULL);

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionAuthReject)
{
    std::string state = ExpectConnectionReject(MakeConnectionString("unknown", "unknown"));

    BOOST_CHECK_EQUAL(std::string("08004"), state);
}

BOOST_AUTO_TEST_CASE(TestConnectionUserOperationsQuery)
{
    Connect(MakeDefaultConnectionString());

    SQLRETURN ret = ExecQuery("CREATE USER \"test\" WITH PASSWORD 'somePass'");

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    Disconnect();

    Connect(MakeConnectionString("test", "somePass"));

    ret = ExecQuery("ALTER USER \"test\" WITH PASSWORD 'somePass42'");

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    Disconnect();

    Connect(MakeConnectionString("test", "somePass42"));

    Disconnect();

    Connect(MakeDefaultConnectionString());

    ret = ExecQuery("DROP USER \"test\"");

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    Disconnect();

    std::string state = ExpectConnectionReject(MakeConnectionString("test", "somePass"));

    BOOST_CHECK_EQUAL(std::string("08004"), state);
}

BOOST_AUTO_TEST_SUITE_END()
