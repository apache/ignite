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

#include <ignite/guid.h>
#include <ignite/odbc/decimal.h>
#include <ignite/odbc/app/application_data_buffer.h>

#define FLOAT_PRECISION 0.0000001f

using namespace ignite;
using namespace ignite::odbc;
using namespace ignite::odbc::app;
using namespace ignite::odbc::type_traits;

BOOST_AUTO_TEST_SUITE(ApplicationDataBufferTestSuite)

BOOST_AUTO_TEST_CASE(TestPutIntToString)
{
    char buffer[1024];
    SqlLen reslen;
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_CHAR, buffer, sizeof(buffer), &reslen, &offset);

    appBuf.PutInt8(12);
    BOOST_REQUIRE(!strcmp(buffer, "12"));
    BOOST_REQUIRE(reslen == strlen("12"));

    appBuf.PutInt8(-12);
    BOOST_REQUIRE(!strcmp(buffer, "-12"));
    BOOST_REQUIRE(reslen == strlen("-12"));

    appBuf.PutInt16(9876);
    BOOST_REQUIRE(!strcmp(buffer, "9876"));
    BOOST_REQUIRE(reslen == strlen("9876"));

    appBuf.PutInt16(-9876);
    BOOST_REQUIRE(!strcmp(buffer, "-9876"));
    BOOST_REQUIRE(reslen == strlen("-9876"));

    appBuf.PutInt32(1234567);
    BOOST_REQUIRE(!strcmp(buffer, "1234567"));
    BOOST_REQUIRE(reslen == strlen("1234567"));

    appBuf.PutInt32(-1234567);
    BOOST_REQUIRE(!strcmp(buffer, "-1234567"));
    BOOST_REQUIRE(reslen == strlen("-1234567"));
}

BOOST_AUTO_TEST_CASE(TestPutFloatToString)
{
    char buffer[1024];
    SqlLen reslen;
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_CHAR, buffer, sizeof(buffer), &reslen, &offset);

    appBuf.PutFloat(12.42f);
    BOOST_REQUIRE(!strcmp(buffer, "12.42"));
    BOOST_REQUIRE(reslen == strlen("12.42"));

    appBuf.PutFloat(-12.42f);
    BOOST_REQUIRE(!strcmp(buffer, "-12.42"));
    BOOST_REQUIRE(reslen == strlen("-12.42"));

    appBuf.PutDouble(1000.21);
    BOOST_REQUIRE(!strcmp(buffer, "1000.21"));
    BOOST_REQUIRE(reslen == strlen("1000.21"));

    appBuf.PutDouble(-1000.21);
    BOOST_REQUIRE(!strcmp(buffer, "-1000.21"));
    BOOST_REQUIRE(reslen == strlen("-1000.21"));
}

BOOST_AUTO_TEST_CASE(TestPutGuidToString)
{
    char buffer[1024];
    SqlLen reslen;
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_CHAR, buffer, sizeof(buffer), &reslen, &offset);

    ignite::Guid guid(0x1da1ef8f39ff4d62ULL, 0x8b72e8e9f3371801ULL);

    appBuf.PutGuid(guid);

    BOOST_REQUIRE(!strcmp(buffer, "1da1ef8f-39ff-4d62-8b72-e8e9f3371801"));
    BOOST_REQUIRE(reslen == strlen("1da1ef8f-39ff-4d62-8b72-e8e9f3371801"));
}

BOOST_AUTO_TEST_CASE(TestPutBinaryToString)
{
    char buffer[1024];
    SqlLen reslen;
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_CHAR, buffer, sizeof(buffer), &reslen, &offset);

    uint8_t binary[] = { 0x21, 0x84, 0xF4, 0xDC, 0x01, 0x00, 0xFF, 0xF0 };

    appBuf.PutBinaryData(binary, sizeof(binary));

    BOOST_REQUIRE(!strcmp(buffer, "2184f4dc0100fff0"));
    BOOST_REQUIRE(reslen == strlen("2184f4dc0100fff0"));
}

BOOST_AUTO_TEST_CASE(TestPutStringToString)
{
    char buffer[1024];
    SqlLen reslen;
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_CHAR, buffer, sizeof(buffer), &reslen, &offset);

    std::string testString("Test string");

    appBuf.PutString(testString);

    BOOST_REQUIRE(!strcmp(buffer, testString.c_str()));
    BOOST_REQUIRE(reslen == testString.size());
}

BOOST_AUTO_TEST_CASE(TestPutStringToWstring)
{
    wchar_t buffer[1024];
    SqlLen reslen;
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_WCHAR, buffer, sizeof(buffer), &reslen, &offset);

    std::string testString("Test string");

    appBuf.PutString(testString);
    BOOST_REQUIRE(!wcscmp(buffer, L"Test string"));
}

BOOST_AUTO_TEST_CASE(TestPutStringToLong)
{
    long numBuf;
    SqlLen reslen;
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_SIGNED_LONG, &numBuf, sizeof(numBuf), &reslen, &offset);

    appBuf.PutString("424242424");
    BOOST_REQUIRE(numBuf == 424242424L);

    appBuf.PutString("-424242424");
    BOOST_REQUIRE(numBuf == -424242424L);
}

BOOST_AUTO_TEST_CASE(TestPutStringToTiny)
{
    int8_t numBuf;
    SqlLen reslen;
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_SIGNED_TINYINT, &numBuf, sizeof(numBuf), &reslen, &offset);

    appBuf.PutString("12");
    BOOST_REQUIRE(numBuf == 12);

    appBuf.PutString("-12");
    BOOST_REQUIRE(numBuf == -12);
}

BOOST_AUTO_TEST_CASE(TestPutStringToFloat)
{
    float numBuf;
    SqlLen reslen;
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_FLOAT, &numBuf, sizeof(numBuf), &reslen, &offset);

    appBuf.PutString("12.21");
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, 12.21, FLOAT_PRECISION);

    appBuf.PutString("-12.21");
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, -12.21, FLOAT_PRECISION);
}

BOOST_AUTO_TEST_CASE(TestPutIntToFloat)
{
    float numBuf;
    SqlLen reslen;
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_FLOAT, &numBuf, sizeof(numBuf), &reslen, &offset);

    appBuf.PutInt8(5);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, 5.0, FLOAT_PRECISION);

    appBuf.PutInt8(-5);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, -5.0, FLOAT_PRECISION);

    appBuf.PutInt16(4242);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, 4242.0, FLOAT_PRECISION);

    appBuf.PutInt16(-4242);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, -4242.0, FLOAT_PRECISION);

    appBuf.PutInt32(1234567);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, 1234567.0, FLOAT_PRECISION);

    appBuf.PutInt32(-1234567);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, -1234567.0, FLOAT_PRECISION);
}

BOOST_AUTO_TEST_CASE(TestPutFloatToShort)
{
    short numBuf;
    SqlLen reslen;
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_SIGNED_SHORT, &numBuf, sizeof(numBuf), &reslen, &offset);

    appBuf.PutDouble(5.42);
    BOOST_REQUIRE(numBuf == 5);

    appBuf.PutDouble(-5.42);
    BOOST_REQUIRE(numBuf == -5.0);

    appBuf.PutFloat(42.99f);
    BOOST_REQUIRE(numBuf == 42);

    appBuf.PutFloat(-42.99f);
    BOOST_REQUIRE(numBuf == -42);
}

BOOST_AUTO_TEST_CASE(TestPutDecimalToDouble)
{
    double numBuf;
    SqlLen reslen;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_DOUBLE, &numBuf, sizeof(numBuf), &reslen, 0);

    Decimal decimal;

    BOOST_REQUIRE_CLOSE_FRACTION(static_cast<double>(decimal), 0.0, FLOAT_PRECISION);

    appBuf.PutDecimal(decimal);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, 0.0, FLOAT_PRECISION);

    int8_t mag1[] = { 1, 0 };

    decimal = Decimal(0, mag1, sizeof(mag1));

    appBuf.PutDecimal(decimal);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, 256.0, FLOAT_PRECISION);

    int8_t mag2[] = { 2, 23 };

    decimal = Decimal(1 | 0x80000000, mag2, sizeof(mag2));

    appBuf.PutDecimal(decimal);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, -53.5, FLOAT_PRECISION);
}

BOOST_AUTO_TEST_CASE(TestPutDecimalToLong)
{
    long numBuf;
    SqlLen reslen;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_SIGNED_LONG, &numBuf, sizeof(numBuf), &reslen, 0);

    Decimal decimal;

    appBuf.PutDecimal(decimal);
    BOOST_REQUIRE(numBuf == 0);

    int8_t mag1[] = { 1, 0 };

    decimal = Decimal(0, mag1, sizeof(mag1));

    appBuf.PutDecimal(decimal);
    BOOST_REQUIRE(numBuf == 256);

    int8_t mag2[] = { 2, 23 };

    decimal = Decimal(1 | 0x80000000, mag2, sizeof(mag2));

    appBuf.PutDecimal(decimal);
    BOOST_REQUIRE(numBuf == -53);
}

BOOST_AUTO_TEST_CASE(TestPutDecimalToString)
{
    char strBuf[64];
    SqlLen reslen;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_CHAR, &strBuf, sizeof(strBuf), &reslen, 0);

    Decimal decimal;

    appBuf.PutDecimal(decimal);
    BOOST_REQUIRE(std::string(strBuf, reslen) == "0");

    int8_t mag1[] = { 1, 0 };

    decimal = Decimal(0, mag1, sizeof(mag1));

    appBuf.PutDecimal(decimal);
    BOOST_REQUIRE(std::string(strBuf, reslen) == "256");

    int8_t mag2[] = { 2, 23 };

    decimal = Decimal(1 | 0x80000000, mag2, sizeof(mag2));

    appBuf.PutDecimal(decimal);
    BOOST_REQUIRE(std::string(strBuf, reslen) == "-53.5");
}

BOOST_AUTO_TEST_CASE(TestGetStringFromLong)
{
    long numBuf = 42;
    SqlLen reslen = sizeof(numBuf);
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_SIGNED_LONG, &numBuf, reslen, &reslen, &offset);

    std::string res = appBuf.GetString(32);

    BOOST_REQUIRE(res == "42");

    numBuf = -77;

    res = appBuf.GetString(32);

    BOOST_REQUIRE(res == "-77");
}

BOOST_AUTO_TEST_CASE(TestGetStringFromDouble)
{
    double numBuf = 43.36;
    SqlLen reslen = sizeof(numBuf);
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_DOUBLE, &numBuf, reslen, &reslen, &offset);

    std::string res = appBuf.GetString(32);

    BOOST_REQUIRE(res == "43.36");

    numBuf = -58.91;

    res = appBuf.GetString(32);

    BOOST_REQUIRE(res == "-58.91");
}

BOOST_AUTO_TEST_CASE(TestGetStringFromString)
{
    char buf[] = "Some data 32d2d5hs";
    SqlLen reslen = sizeof(buf);
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_CHAR, &buf, reslen, &reslen, &offset);

    std::string res = appBuf.GetString(reslen);

    BOOST_REQUIRE(res.compare(buf));
}

BOOST_AUTO_TEST_CASE(TestGetFloatFromUshort)
{
    unsigned short numBuf = 7162;
    SqlLen reslen = sizeof(numBuf);
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_UNSIGNED_SHORT, &numBuf, reslen, &reslen, &offset);

    float resFloat = appBuf.GetFloat();

    BOOST_REQUIRE_CLOSE_FRACTION(resFloat, 7162.0f, FLOAT_PRECISION);

    double resDouble = appBuf.GetDouble();

    BOOST_REQUIRE_CLOSE_FRACTION(resDouble, 7162.0, FLOAT_PRECISION);
}

BOOST_AUTO_TEST_CASE(TestGetFloatFromString)
{
    char buf[] = "28.562";
    SqlLen reslen = sizeof(buf);
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_CHAR, &buf, reslen, &reslen, &offset);

    float resFloat = appBuf.GetFloat();

    BOOST_REQUIRE_CLOSE_FRACTION(resFloat, 28.562f, FLOAT_PRECISION);

    double resDouble = appBuf.GetDouble();

    BOOST_REQUIRE_CLOSE_FRACTION(resDouble, 28.562, FLOAT_PRECISION);
}

BOOST_AUTO_TEST_CASE(TestGetFloatFromFloat)
{
    float buf = 207.49f;
    SqlLen reslen = sizeof(buf);
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_FLOAT, &buf, reslen, &reslen, &offset);

    float resFloat = appBuf.GetFloat();

    BOOST_REQUIRE_CLOSE_FRACTION(resFloat, 207.49f, FLOAT_PRECISION);

    double resDouble = appBuf.GetDouble();

    BOOST_REQUIRE_CLOSE_FRACTION(resDouble, 207.49, FLOAT_PRECISION);
}

BOOST_AUTO_TEST_CASE(TestGetFloatFromDouble)
{
    double buf = 893.162;
    SqlLen reslen = sizeof(buf);
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_DOUBLE, &buf, reslen, &reslen, &offset);

    float resFloat = appBuf.GetFloat();

    BOOST_REQUIRE_CLOSE_FRACTION(resFloat, 893.162f, FLOAT_PRECISION);

    double resDouble = appBuf.GetDouble();

    BOOST_REQUIRE_CLOSE_FRACTION(resDouble, 893.162, FLOAT_PRECISION);
}

BOOST_AUTO_TEST_CASE(TestGetIntFromString)
{
    char buf[] = "39";
    SqlLen reslen = sizeof(buf);
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_CHAR, &buf, reslen, &reslen, &offset);

    int64_t resInt64 = appBuf.GetInt64();

    BOOST_REQUIRE(resInt64 == 39);

    int32_t resInt32 = appBuf.GetInt32();

    BOOST_REQUIRE(resInt32 == 39);

    int16_t resInt16 = appBuf.GetInt16();

    BOOST_REQUIRE(resInt16 == 39);

    int8_t resInt8 = appBuf.GetInt8();

    BOOST_REQUIRE(resInt8 == 39);
}

BOOST_AUTO_TEST_CASE(TestGetIntFromFloat)
{
    float buf = -107.49f;
    SqlLen reslen = sizeof(buf);
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_FLOAT, &buf, reslen, &reslen, &offset);

    int64_t resInt64 = appBuf.GetInt64();

    BOOST_REQUIRE(resInt64 == -107);

    int32_t resInt32 = appBuf.GetInt32();

    BOOST_REQUIRE(resInt32 == -107);

    int16_t resInt16 = appBuf.GetInt16();

    BOOST_REQUIRE(resInt16 == -107);

    int8_t resInt8 = appBuf.GetInt8();

    BOOST_REQUIRE(resInt8 == -107);
}

BOOST_AUTO_TEST_CASE(TestGetIntFromDouble)
{
    double buf = 42.97f;
    SqlLen reslen = sizeof(buf);
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_DOUBLE, &buf, reslen, &reslen, &offset);

    int64_t resInt64 = appBuf.GetInt64();

    BOOST_REQUIRE(resInt64 == 42);

    int32_t resInt32 = appBuf.GetInt32();

    BOOST_REQUIRE(resInt32 == 42);

    int16_t resInt16 = appBuf.GetInt16();

    BOOST_REQUIRE(resInt16 == 42);

    int8_t resInt8 = appBuf.GetInt8();

    BOOST_REQUIRE(resInt8 == 42);
}

BOOST_AUTO_TEST_CASE(TestGetIntFromBigint)
{
    uint64_t buf = 19;
    SqlLen reslen = sizeof(buf);
    size_t* offset = 0;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_UNSIGNED_BIGINT, &buf, reslen, &reslen, &offset);

    int64_t resInt64 = appBuf.GetInt64();

    BOOST_REQUIRE(resInt64 == 19);

    int32_t resInt32 = appBuf.GetInt32();

    BOOST_REQUIRE(resInt32 == 19);

    int16_t resInt16 = appBuf.GetInt16();

    BOOST_REQUIRE(resInt16 == 19);

    int8_t resInt8 = appBuf.GetInt8();

    BOOST_REQUIRE(resInt8 == 19);
}

BOOST_AUTO_TEST_CASE(TestGetIntWithOffset)
{
    struct TestStruct
    {
        uint64_t val;
        SqlLen reslen;
    };

    TestStruct buf[2] = {
        { 12, sizeof(uint64_t) },
        { 42, sizeof(uint64_t) }
    };

    size_t offset = 0;
    size_t* offsetPtr = &offset;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_UNSIGNED_BIGINT, &buf[0].val, sizeof(buf[0].val), &buf[0].reslen, &offsetPtr);

    int64_t val = appBuf.GetInt64();

    BOOST_REQUIRE(val == 12);

    offset += sizeof(TestStruct);

    val = appBuf.GetInt64();

    BOOST_REQUIRE(val == 42);

    offsetPtr = 0;

    val = appBuf.GetInt64();

    BOOST_REQUIRE(val == 12);
}

BOOST_AUTO_TEST_CASE(TestSetStringWithOffset)
{
    struct TestStruct
    {
        char val[64];
        SqlLen reslen;
    };

    TestStruct buf[2] = {
        { "", 0 },
        { "", 0 }
    };

    size_t offset = 0;
    size_t* offsetPtr = &offset;

    ApplicationDataBuffer appBuf(IGNITE_ODBC_C_TYPE_CHAR, &buf[0].val, sizeof(buf[0].val), &buf[0].reslen, &offsetPtr);

    appBuf.PutString("Hello Ignite!");

    std::string res(buf[0].val, buf[0].reslen);

    BOOST_REQUIRE(buf[0].reslen == strlen("Hello Ignite!"));
    BOOST_REQUIRE(res == "Hello Ignite!");
    BOOST_REQUIRE(res.size() == strlen("Hello Ignite!"));

    offset += sizeof(TestStruct);

    appBuf.PutString("Hello with offset!");

    res.assign(buf[0].val, buf[0].reslen);

    BOOST_REQUIRE(res == "Hello Ignite!");
    BOOST_REQUIRE(res.size() == strlen("Hello Ignite!"));
    BOOST_REQUIRE(buf[0].reslen == strlen("Hello Ignite!"));

    res.assign(buf[1].val, buf[1].reslen);

    BOOST_REQUIRE(res == "Hello with offset!");
    BOOST_REQUIRE(res.size() == strlen("Hello with offset!"));
    BOOST_REQUIRE(buf[1].reslen == strlen("Hello with offset!"));
}

BOOST_AUTO_TEST_SUITE_END()