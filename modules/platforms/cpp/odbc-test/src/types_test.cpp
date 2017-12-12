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
        node = StartTestNode("queries-test.xml", "NodeMain");

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

    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_NUMERIC, SQL_DECIMAL, 0, 0, &num, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));


}

BOOST_AUTO_TEST_SUITE_END()
