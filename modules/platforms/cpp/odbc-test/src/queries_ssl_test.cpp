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
#include <sstream>

#include <boost/regex.hpp>
#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

#include "test_utils.h"
#include "odbc_test_suite.h"

using namespace ignite;
using namespace ignite::common;
using namespace ignite_test;

using namespace boost::unit_test;

/**
 * Test setup fixture.
 */
struct SslQueriesTestSuiteFixture : odbc::OdbcTestSuite
{
    static Ignite StartAdditionalNode(const char* name)
    {
        return StartPlatformNode("queries-ssl.xml", name);
    }

    /**
     * Constructor.
     */
    SslQueriesTestSuiteFixture() :
        OdbcTestSuite()
    {
        grid = StartAdditionalNode("NodeMain");
    }

    /**
     * Destructor.
     */
    virtual ~SslQueriesTestSuiteFixture()
    {
        // No-op.
    }

    /**
     * Create default connection string.
     * @return Default connection string.
     */
    std::string MakeDefaultConnectionString()
    {
        std::string cfgDirPath = GetTestConfigDir();

        std::stringstream connectString;

        connectString <<
            "DRIVER={Apache Ignite};"
            "ADDRESS=127.0.0.1:11110;"
            "SCHEMA=cache;"
            "SSL_MODE=require;"
            "SSL_KEY_FILE=" << cfgDirPath << Fs << "ssl" << Fs << "client_full.pem;"
            "SSL_CERT_FILE=" << cfgDirPath << Fs << "ssl" << Fs << "client_full.pem;"
            "SSL_CA_FILE=" << cfgDirPath << Fs << "ssl" << Fs << "ca.pem;";

        return connectString.str();
    }

    /** Node started during the test. */
    Ignite grid;
};

BOOST_FIXTURE_TEST_SUITE(SslQueriesTestSuite, SslQueriesTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestConnectionSslSuccess)
{
    Connect(MakeDefaultConnectionString());

    InsertTestStrings(10, false);
    InsertTestBatch(11, 2000, 1989);
}

BOOST_AUTO_TEST_CASE(TestConnectionSslReject)
{
    std::string cfgDirPath = GetTestConfigDir();

    std::stringstream connectString;

    connectString <<
        "DRIVER={Apache Ignite};"
        "ADDRESS=127.0.0.1:11110;"
        "SCHEMA=cache;"
        "SSL_MODE=require;"
        "SSL_KEY_FILE=" << cfgDirPath << Fs << "ssl" << Fs << "client_unknown.pem;"
        "SSL_CERT_FILE=" << cfgDirPath << Fs << "ssl" << Fs << "client_unknown.pem;"
        "SSL_CA_FILE=" << cfgDirPath << Fs << "ssl" << Fs << "ca.pem;";

    Prepare();

    // Connect string
    std::string str = connectString.str();
    std::vector<SQLCHAR> connectStr0(str.begin(), str.end());

    SQLCHAR outstr[ODBC_BUFFER_SIZE];
    SQLSMALLINT outstrlen;

    // Connecting to ODBC server.
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, &connectStr0[0], static_cast<SQLSMALLINT>(connectStr0.size()),
        outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

    // Checking that there is an error.
    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);

    // Checking that error is the connection error.
    std::set<std::string> codes;
    codes.insert(std::string("08001")); // Client unable to establish connection.
    codes.insert(std::string("08S01")); // Communication link failure. Returned on newest UnixODBC.
    BOOST_CHECK_EQUAL(codes.count(GetOdbcErrorState(SQL_HANDLE_DBC, dbc)), 1);
}

BOOST_AUTO_TEST_CASE(TestLoginTimeout)
{
    Prepare();

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_LOGIN_TIMEOUT, reinterpret_cast<SQLPOINTER>(1), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    std::string connectStr0 = MakeDefaultConnectionString();
    std::vector<SQLCHAR> connectStr(connectStr0.begin(), connectStr0.end());

    SQLCHAR outstr[ODBC_BUFFER_SIZE];
    SQLSMALLINT outstrlen;

    // Connecting to ODBC server.
    ret = SQLDriverConnect(dbc, NULL, &connectStr[0], static_cast<SQLSMALLINT>(connectStr.size()),
        outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_DBC, dbc));
}

BOOST_AUTO_TEST_CASE(TestConnectionTimeoutQuery)
{
    Connect(MakeDefaultConnectionString());

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestStrings(10, false);
}

BOOST_AUTO_TEST_CASE(TestConnectionTimeoutBatch)
{
    Connect(MakeDefaultConnectionString());

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestConnectionTimeoutBoth)
{
    Connect(MakeDefaultConnectionString());

    SQLRETURN ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestQueryTimeoutQuery)
{
    Connect(MakeDefaultConnectionString());

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    InsertTestStrings(10, false);
}

BOOST_AUTO_TEST_CASE(TestQueryTimeoutBatch)
{
    Connect(MakeDefaultConnectionString());

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestQueryTimeoutBoth)
{
    Connect(MakeDefaultConnectionString());

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestQueryAndConnectionTimeoutQuery)
{
    Connect(MakeDefaultConnectionString());

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(3), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestStrings(10, false);
}

BOOST_AUTO_TEST_CASE(TestQueryAndConnectionTimeoutBatch)
{
    Connect(MakeDefaultConnectionString());

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(3), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_CASE(TestQueryAndConnectionTimeoutBoth)
{
    Connect(MakeDefaultConnectionString());

    SQLRETURN ret = SQLSetStmtAttr(stmt, SQL_ATTR_QUERY_TIMEOUT, reinterpret_cast<SQLPOINTER>(5), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_STMT, stmt);

    ret = SQLSetConnectAttr(dbc, SQL_ATTR_CONNECTION_TIMEOUT, reinterpret_cast<SQLPOINTER>(3), 0);

    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);

    InsertTestStrings(10, false);
    InsertTestBatch(11, 20, 9);
}

BOOST_AUTO_TEST_SUITE_END()
