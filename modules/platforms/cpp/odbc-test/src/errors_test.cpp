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

#include <boost/regex.hpp>
#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/common/fixed_size_array.h"
#include "ignite/ignition.h"
#include "ignite/impl/binary/binary_utils.h"
#include "ignite/binary/binary_object.h"

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
struct ErrorTestSuiteFixture : odbc::OdbcTestSuite
{
    static Ignite StartAdditionalNode(const char* name)
    {
        return StartPlatformNode("queries-test.xml", name);
    }

    /**
     * Constructor.
     */
    ErrorTestSuiteFixture() :
        OdbcTestSuite()
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    ~ErrorTestSuiteFixture()
    {
        // No-op.
    }
};

BOOST_FIXTURE_TEST_SUITE(ErrorTestSuite, ErrorTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestConnectFail)
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
    SQLCHAR connectStr[] = "DRIVER={Apache Ignite};ADDRESS=127.0.0.1:1111;SCHEMA=cache";

    SQLCHAR outstr[ODBC_BUFFER_SIZE];
    SQLSMALLINT outstrlen;

    // Connecting to ODBC server.
    SQLRETURN ret = SQLDriverConnect(dbc, NULL, connectStr, SQL_NTS,
        outstr, sizeof(outstr), &outstrlen, SQL_DRIVER_COMPLETE);

    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
    BOOST_CHECK_EQUAL(GetOdbcErrorState(SQL_HANDLE_DBC, dbc), "08001");
}

BOOST_AUTO_TEST_CASE(TestDuplicateKey)
{
    StartAdditionalNode("Node1");

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLCHAR insertReq[] = "INSERT INTO TestType(_key, strField) VALUES(1, 'some')";

    SQLRETURN ret;

    ret = SQLExecDirect(stmt, insertReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecDirect(stmt, insertReq, SQL_NTS);

    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
    BOOST_CHECK_EQUAL(GetOdbcErrorState(SQL_HANDLE_STMT, stmt), "23000");
}

BOOST_AUTO_TEST_CASE(TestUpdateKey)
{
    StartAdditionalNode("Node1");

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=cache");

    SQLCHAR insertReq[] = "INSERT INTO TestType(_key, strField) VALUES(1, 'some')";

    SQLRETURN ret;

    ret = SQLExecDirect(stmt, insertReq, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR updateReq[] = "UPDATE TestType SET _key=2 WHERE _key=1";

    ret = SQLExecDirect(stmt, updateReq, SQL_NTS);

    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
    BOOST_CHECK_EQUAL(GetOdbcErrorState(SQL_HANDLE_STMT, stmt), "42000");
}

BOOST_AUTO_TEST_CASE(TestTableNotFound)
{
    StartAdditionalNode("Node1");

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=PUBLIC");

    SQLCHAR req[] = "DROP TABLE Nonexisting";

    SQLRETURN ret;

    ret = SQLExecDirect(stmt, req, SQL_NTS);

    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
    BOOST_CHECK_EQUAL(GetOdbcErrorState(SQL_HANDLE_STMT, stmt), "42S02");
}

BOOST_AUTO_TEST_CASE(TestIndexNotFound)
{
    StartAdditionalNode("Node1");

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=PUBLIC");

    SQLCHAR req[] = "DROP INDEX Nonexisting";

    SQLRETURN ret;

    ret = SQLExecDirect(stmt, req, SQL_NTS);

    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
    BOOST_CHECK_EQUAL(GetOdbcErrorState(SQL_HANDLE_STMT, stmt), "42S12");
}

BOOST_AUTO_TEST_CASE(TestSyntaxError)
{
    StartAdditionalNode("Node1");

    Connect("DRIVER={Apache Ignite};ADDRESS=127.0.0.1:11110;SCHEMA=PUBLIC");

    SQLCHAR req[] = "INSERT INTO TestType(_key, fasf) VALUES(1, 'some')";

    SQLRETURN ret;

    ret = SQLExecDirect(stmt, req, SQL_NTS);

    BOOST_REQUIRE_EQUAL(ret, SQL_ERROR);
    BOOST_CHECK_EQUAL(GetOdbcErrorState(SQL_HANDLE_STMT, stmt), "42000");
}

BOOST_AUTO_TEST_SUITE_END()
