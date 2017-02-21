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
#include "ignite/common/decimal.h"
#include "ignite/common/utils.h"
#include "sql_test_suite_fixture.h"
#include "test_utils.h"

using namespace ignite;
using namespace ignite::common;
using namespace boost::unit_test;

BOOST_FIXTURE_TEST_SUITE(SqlEscConvertFunctionTestSuite, ignite::SqlTestSuiteFixture)

int CheckConnectionInfo(HDBC dbc, int infoType)
{
    SQLUINTEGER mask = 0;
    SQLRETURN ret = SQLGetInfo(dbc, infoType, &mask, sizeof(mask), 0);
    ODBC_FAIL_ON_ERROR(ret, SQL_HANDLE_DBC, dbc);
    return mask;
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionGetInfo)
{
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_FUNCTIONS) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_BIGINT) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_BINARY) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_BIT) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_CHAR) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_DATE) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_DECIMAL) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_DOUBLE) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_FLOAT) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_INTEGER) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_LONGVARCHAR) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_NUMERIC) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_REAL) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_SMALLINT) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_TIME) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_TIMESTAMP) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_TINYINT) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_VARBINARY) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_VARCHAR) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_LONGVARBINARY) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_WCHAR) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_WLONGVARCHAR) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_WVARCHAR) != 0);
    BOOST_REQUIRE(CheckConnectionInfo(dbc, SQL_CONVERT_GUID) != 0);
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionInt64)
{
    CheckSingleResult<int64_t>("SELECT {fn CONVERT(72623859790382856, SQL_BIGINT)}", 72623859790382856);
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionInt32)
{
    CheckSingleResult<int32_t>("SELECT {fn CONVERT(1234567890, SQL_INTEGER)}", 1234567890);
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionInt16)
{
    CheckSingleResult<int16_t>("SELECT {fn CONVERT(12345, SQL_SMALLINT)}", 12345);
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionInt8)
{
    CheckSingleResult<int8_t>("SELECT {fn CONVERT(123, SQL_TINYINT)}", 123);
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionByteArray)
{
    int32_t value = ToBigEndian(123456);

    std::vector<int8_t> val;
    val.assign((const int8_t*)&value, (const int8_t*)&value+sizeof(value));

    CheckSingleResult<std::vector<int8_t> >("SELECT {fn CONVERT(123456, SQL_BINARY(4))}", val);
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionBool)
{
    CheckSingleResult<bool>("SELECT {fn CONVERT(1, SQL_BIT)}", true);
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionString)
{
    CheckSingleResult<std::string>("SELECT {fn CONVERT(123, SQL_VARCHAR(10))}", "123");
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionDecimal)
{
    CheckSingleResult<Decimal>("SELECT {fn CONVERT(-1.25, SQL_DECIMAL(5,2))}", Decimal("-1.25"));
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionDouble)
{
    CheckSingleResult<double>("SELECT CAST(-1.25 AS DOUBLE)", -1.25);
    CheckSingleResult<double>("SELECT CONVERT(-1.25, DOUBLE)", -1.25);
    CheckSingleResult<double>("SELECT {fn CONVERT(-1.25, SQL_DOUBLE)}", -1.25);
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionFloat)
{
    CheckSingleResult<float>("SELECT CAST(-1.25 AS REAL)", -1.25);
    CheckSingleResult<float>("SELECT CONVERT(-1.25, REAL)", -1.25);
    CheckSingleResult<float>("SELECT CAST(-1.25 AS FLOAT4)", -1.25);
    CheckSingleResult<float>("SELECT CONVERT(-1.25, FLOAT4)", -1.25);
    CheckSingleResult<float>("SELECT {fn CONVERT(-1.25, SQL_REAL)}", -1.25);
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionGuid)
{
    //no support for binding as GUID because we report v3.0 to DM, thus fallback to string binding for now
    CheckSingleResult<std::string>("SELECT {fn CONVERT({guid '04cc382a-0b82-f520-08d0-07a0620c0004'}, SQL_GUID)}", "04cc382a-0b82-f520-08d0-07a0620c0004");
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionDate)
{
    using ignite::impl::binary::BinaryUtils;
    Date date = common::MakeDateGmt(1983, 3, 14);
    CheckSingleResult<Date>("SELECT {fn CONVERT('1983-03-14', SQL_DATE)}", date);
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionTime)
{
    SQL_TIME_STRUCT exp;
    exp.hour = 13;
    exp.minute = 20;
    exp.second = 15;
    CheckSingleResult<SQL_TIME_STRUCT>("SELECT {fn CONVERT('13:20:15', SQL_TIME)}", exp);
}

BOOST_AUTO_TEST_CASE(TestEscConvertFunctionTimestamp)
{
    using ignite::impl::binary::BinaryUtils;
    Timestamp ts = common::MakeTimestampGmt(1983, 3, 14, 13, 20, 15, 999999999);
    CheckSingleResult<Timestamp>("SELECT {fn CONVERT('1983-03-14 13:20:15.999999999', SQL_TIMESTAMP)}", ts);
}

BOOST_AUTO_TEST_SUITE_END()
