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
using namespace ignite_test;

using namespace boost::unit_test;

BOOST_FIXTURE_TEST_SUITE(SqlTypesTestSuite, ignite::SqlTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestGuidTrivial)
{
    CheckSingleResult<std::string>("SELECT {guid '04CC382A-0B82-F520-08D0-07A0620C0004'}",
        "04cc382a-0b82-f520-08d0-07a0620c0004");

    CheckSingleResult<std::string>("SELECT {guid '63802467-9f4a-4f71-8fc8-cf2d99a28ddf'}",
        "63802467-9f4a-4f71-8fc8-cf2d99a28ddf");
}

BOOST_AUTO_TEST_CASE(TestGuidEqualsToColumn)
{
    TestType in1;
    TestType in2;

    in1.guidField = Guid(0x638024679f4a4f71, 0x8fc8cf2d99a28ddf);
    in2.guidField = Guid(0x04cc382a0b82f520, 0x08d007a0620c0004);

    in1.i32Field = 1;
    in2.i32Field = 2;

    testCache.Put(1, in1);
    testCache.Put(2, in2);

    CheckSingleResult<int32_t>(
        "SELECT i32Field FROM TestType WHERE guidField = {guid '04cc382a-0b82-f520-08d0-07a0620c0004'}", in2.i32Field);
}

BOOST_AUTO_TEST_CASE(TestByteArraySelect)
{
    TestType in;
    const int8_t data[] = { 'A','B','C','D','E','F','G','H','I','J' };
    in.i8ArrayField.assign(data, data + sizeof(data)/sizeof(data[0]));
    testCache.Put(1, in);

    TestType out = testCache.Get(1);

    BOOST_REQUIRE(in.i8ArrayField.size() == out.i8ArrayField.size());

    BOOST_REQUIRE_EQUAL_COLLECTIONS(in.i8ArrayField.begin(), in.i8ArrayField.end(), out.i8ArrayField.begin(), out.i8ArrayField.end());

    CheckSingleResult<std::vector<int8_t> >("SELECT i8ArrayField FROM TestType", in.i8ArrayField);
}

BOOST_AUTO_TEST_CASE(TestByteArrayParam)
{
    SQLRETURN ret;

    TestType in;
    in.i8Field = 101;

    const int8_t data[] = { 'A','B','C','D','E','F','G','H','I','J' };
    in.i8ArrayField.assign(data, data + sizeof(data) / sizeof(data[0]));

    testCache.Put(1, in);

    SQLLEN colLen = 0;
    SQLCHAR colData = 0;

    ret = SQLBindCol(stmt, 1, SQL_C_TINYINT, &colData, sizeof(colData), &colLen);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLCHAR request[] = "SELECT i8Field FROM TestType WHERE i8ArrayField = ?";

    ret = SQLPrepare(stmt, request, SQL_NTS);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    std::vector<int8_t> paramData(in.i8ArrayField);
    SQLLEN paramLen = paramData.size();
    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY, paramData.size(), 0, &paramData[0], paramData.size(), &paramLen);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLFetch(stmt);
    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    BOOST_REQUIRE_EQUAL(colData, in.i8Field);
    BOOST_REQUIRE_EQUAL(colLen, sizeof(colData));

    ret = SQLFetch(stmt);
    BOOST_REQUIRE(ret == SQL_NO_DATA);
}

BOOST_AUTO_TEST_CASE(TestByteArrayParamInsert)
{
    SQLRETURN ret;

    const int8_t data[] = { 'A','B','C','D','E','F','G','H','I','J' };
    std::vector<int8_t> paramData(data, data + sizeof(data) / sizeof(data[0]));
    SQLCHAR request[] = "INSERT INTO TestType(_key, i8ArrayField) VALUES(?, ?)";;

    ret = SQLPrepare(stmt, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int64_t key = 1;
    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQLLEN paramLen = paramData.size();

    ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_BINARY, SQL_VARBINARY, paramData.size(), 0, &paramData[0], paramData.size(), &paramLen);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    TestType out = testCache.Get(key);
    BOOST_REQUIRE_EQUAL_COLLECTIONS(out.i8ArrayField.begin(), out.i8ArrayField.end(), paramData.begin(), paramData.end());
}

BOOST_AUTO_TEST_CASE(TestByteParamInsert)
{
    SQLRETURN ret;

    SQLCHAR request[] = "INSERT INTO TestType(_key, i8Field) VALUES(?, ?)";;

    ret = SQLPrepare(stmt, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int64_t key = 1;
    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int8_t data = 2;
    ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_TINYINT, SQL_TINYINT, 0, 0, &data, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    TestType out = testCache.Get(key);
    BOOST_REQUIRE_EQUAL(out.i8Field, data);
}

BOOST_AUTO_TEST_CASE(TestTimestampSelect)
{
    TestType in1;
    in1.i32Field = 1;
    in1.timestampField = common::MakeTimestampGmt(2017, 1, 13, 19, 54, 01, 987654321);

    testCache.Put(1, in1);

    CheckSingleResult<int32_t>(
        "SELECT i32Field FROM TestType WHERE timestampField = '2017-01-13 19:54:01.987654321'", in1.i32Field);

    CheckSingleResult<Timestamp>(
        "SELECT timestampField FROM TestType WHERE i32Field = 1", in1.timestampField);
}

BOOST_AUTO_TEST_CASE(TestTimestampInsert)
{
    SQLRETURN ret;

    SQLCHAR request[] = "INSERT INTO TestType(_key, timestampField) VALUES(?, ?)";

    ret = SQLPrepare(stmt, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int64_t key = 1;
    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQL_TIMESTAMP_STRUCT data;
    data.year = 2017;
    data.month = 1;
    data.day = 13;
    data.hour = 19;
    data.minute = 54;
    data.second = 1;
    data.fraction = 987654321;

    using ignite::impl::binary::BinaryUtils;
    Timestamp expected = common::MakeTimestampGmt(data.year, data.month, data.day, data.hour,
        data.minute, data.second, data.fraction);

    SQLLEN lenInd = sizeof(data);
    ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_TIMESTAMP, SQL_TIMESTAMP, sizeof(data), 0, &data, sizeof(data), &lenInd);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    TestType out = testCache.Get(key);

    BOOST_REQUIRE_EQUAL(out.timestampField.GetSeconds(), expected.GetSeconds());
    BOOST_REQUIRE_EQUAL(out.timestampField.GetSecondFraction(), expected.GetSecondFraction());
}

BOOST_AUTO_TEST_CASE(TestTimeSelect)
{
    SQL_TIME_STRUCT ts;
    ts.hour = 19;
    ts.minute = 54;
    ts.second = 1;

    TestType in1;
    in1.i32Field = 1;
    in1.timestampField = common::MakeTimestampGmt(2017, 1, 13, ts.hour, ts.minute, ts.second);

    testCache.Put(1, in1);

    CheckSingleResult<SQL_TIME_STRUCT>(
        "SELECT CAST(timestampField AS TIME) FROM TestType WHERE i32Field = 1", ts);

    CheckSingleResult<int32_t>(
        "SELECT i32Field FROM TestType WHERE CAST(timestampField AS TIME) = '19:54:01'", in1.i32Field);
}

BOOST_AUTO_TEST_CASE(TestTimeInsertToTimestamp)
{
    SQLRETURN ret;

    SQLCHAR request[] = "INSERT INTO TestType(_key, timestampField) VALUES(?, ?)";

    ret = SQLPrepare(stmt, request, SQL_NTS);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    int64_t key = 1;
    ret = SQLBindParameter(stmt, 1, SQL_PARAM_INPUT, SQL_C_SLONG, SQL_BIGINT, 0, 0, &key, 0, 0);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    SQL_TIME_STRUCT data = { 0 };
    data.hour = 19;
    data.minute = 54;
    data.second = 1;

    using ignite::impl::binary::BinaryUtils;
    Timestamp expected = common::MakeTimestampGmt(1970, 1, 1, data.hour, data.minute, data.second, 0);

    SQLLEN lenInd = sizeof(data);
    ret = SQLBindParameter(stmt, 2, SQL_PARAM_INPUT, SQL_C_TIME, SQL_TIMESTAMP, sizeof(data), 0, &data, sizeof(data), &lenInd);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    ret = SQLExecute(stmt);

    if (!SQL_SUCCEEDED(ret))
        BOOST_FAIL(GetOdbcErrorMessage(SQL_HANDLE_STMT, stmt));

    TestType out = testCache.Get(key);

    BOOST_REQUIRE_EQUAL(out.timestampField.GetSeconds(), expected.GetSeconds());
    BOOST_REQUIRE_EQUAL(out.timestampField.GetSecondFraction(), expected.GetSecondFraction());
}

BOOST_AUTO_TEST_SUITE_END()
