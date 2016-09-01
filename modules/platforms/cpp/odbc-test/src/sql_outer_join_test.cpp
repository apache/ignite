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

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include "sql_test_suite_fixture.h"
#include "test_utils.h"

using namespace ignite;

using namespace boost::unit_test;

BOOST_FIXTURE_TEST_SUITE(SqlOuterJoinTestSuite, ignite::SqlTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestOuterJoinLeft)
{
    TestType in1;
    TestType in2;

    in1.i32Field = 20;
    in2.i32Field = 30;

    in1.i16Field = 40;
    in2.i16Field = 20;

    testCache.Put(1, in1);
    testCache.Put(2, in2);

    SQLINTEGER columns[2];
    SQLLEN columnsLen[2];

    SQLRETURN ret = SQLBindCol(stmt, 1, SQL_C_SLONG, &columns[0], 0, &columnsLen[0]);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLBindCol(stmt, 2, SQL_C_SLONG, &columns[1], 0, &columnsLen[1]);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR request[] = "SELECT T1.i32Field, T2.i16Field FROM "
        "{oj TestType T1 LEFT OUTER JOIN TestType T2 ON T1.i32Field = T2.i16Field}";

    ret = SQLExecDirect(stmt, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_NE(columnsLen[0], SQL_NULL_DATA);
    BOOST_CHECK_EQUAL(columns[0], 20);

    BOOST_CHECK_NE(columnsLen[1], SQL_NULL_DATA);
    BOOST_CHECK_EQUAL(columns[1], 20);

    ret = SQLFetch(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_CHECK_NE(columnsLen[0], SQL_NULL_DATA);
    BOOST_CHECK_EQUAL(columns[0], 30);

    BOOST_CHECK_EQUAL(columnsLen[1], SQL_NULL_DATA);

    ret = SQLFetch(stmt);
    BOOST_CHECK(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_SUITE_END()
