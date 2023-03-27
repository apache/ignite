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
#include "test_type.h"
#include "test_utils.h"
#include "odbc_test_suite.h"

using namespace ignite;
using namespace ignite_test;

using namespace boost::unit_test;

/**
 * Test setup fixture.
 */
struct TypesTestSuiteFixture : odbc::OdbcTestSuite
{
    /**
     * Constructor.
     */
    TypesTestSuiteFixture() :
        cache1(0)
    {
        node = StartPlatformNode("queries-test.xml", "NodeMain");

        cache1 = node.GetCache<int64_t, TestType>("cache");
    }

    /**
     * Destructor.
     */
    virtual ~TypesTestSuiteFixture()
    {
        // No-op.
    }

    /** Node started during the test. */
    Ignite node;

    /** Cache instance. */
    cache::Cache<int64_t, TestType> cache1;
};

BOOST_FIXTURE_TEST_SUITE(TypesTestSuite, TypesTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestZeroDecimal)
{
    Connect("DRIVER={Apache Ignite};SERVER=127.0.0.1;PORT=11110;SCHEMA=PUBLIC");

    SQLCHAR ddl[] = "CREATE TABLE IF NOT EXISTS TestTable "
        "(RecId varchar PRIMARY KEY, RecValue DECIMAL(4,2))"
        "WITH \"template=replicated, cache_name=TestTable_Cache\";";

    SQLRETURN ret = SQLExecDirect(stmt, ddl, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR dml[] = "INSERT INTO TestTable (RecId, RecValue) VALUES ('1', ?)";

    ret = SQLPrepare(stmt, dml, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQL_NUMERIC_STRUCT num;

    memset(&num, 0, sizeof(num));

    num.sign = 1;
    num.precision = 1;

    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_NUMERIC, SQL_DECIMAL, 0, 0, &num, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFreeStmt(stmt, SQL_RESET_PARAMS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQL_NUMERIC_STRUCT num0;
    SQLLEN num0Len = static_cast<SQLLEN>(sizeof(num0));

    // Filling data to avoid acidental equality
    memset(&num0, 0xFF, sizeof(num0));

    ret = SQLBindCol(stmt, 1, SQL_C_NUMERIC, &num0, sizeof(num0), &num0Len);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR qry[] = "SELECT RecValue FROM TestTable WHERE RecId = '1'";

    ret = SQLExecDirect(stmt, qry, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_EQUAL(num.precision, num0.precision);
    BOOST_CHECK_EQUAL(num.scale, num0.scale);
    BOOST_CHECK_EQUAL(num.sign, num0.sign);

    BOOST_CHECK_EQUAL(num.val[0], num0.val[0]);
    BOOST_CHECK_EQUAL(num.val[1], num0.val[1]);
    BOOST_CHECK_EQUAL(num.val[2], num0.val[2]);
    BOOST_CHECK_EQUAL(num.val[3], num0.val[3]);
    BOOST_CHECK_EQUAL(num.val[4], num0.val[4]);
    BOOST_CHECK_EQUAL(num.val[5], num0.val[5]);
    BOOST_CHECK_EQUAL(num.val[6], num0.val[6]);
    BOOST_CHECK_EQUAL(num.val[7], num0.val[7]);
    BOOST_CHECK_EQUAL(num.val[8], num0.val[8]);
    BOOST_CHECK_EQUAL(num.val[9], num0.val[9]);
    BOOST_CHECK_EQUAL(num.val[10], num0.val[10]);
    BOOST_CHECK_EQUAL(num.val[11], num0.val[11]);
    BOOST_CHECK_EQUAL(num.val[12], num0.val[12]);
    BOOST_CHECK_EQUAL(num.val[13], num0.val[13]);
    BOOST_CHECK_EQUAL(num.val[14], num0.val[14]);
    BOOST_CHECK_EQUAL(num.val[15], num0.val[15]);
}

BOOST_AUTO_TEST_SUITE_END()
