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
#include <ignite/odbc/app/application_data_buffer.h>

using namespace ignite::odbc::app;
using namespace ignite::odbc::type_traits;

BOOST_AUTO_TEST_SUITE(ApplicationDataBufferTestSuite)

BOOST_AUTO_TEST_CASE(TestIntToString)
{
    char buffer[1024];
    int64_t reslen;

    ApplicationDataBuffer appBuf(IGNITE_SQL_TYPE_CHAR, buffer, sizeof(buffer), &reslen);

    appBuf.PutInt8(12);
    BOOST_REQUIRE(!strcmp(buffer, "12"));
    BOOST_REQUIRE(reslen = sizeof("12"));

    appBuf.PutInt8(-12);
    BOOST_REQUIRE(!strcmp(buffer, "-12"));
    BOOST_REQUIRE(reslen = sizeof("-12"));

    appBuf.PutInt16(9876);
    BOOST_REQUIRE(!strcmp(buffer, "9876"));
    BOOST_REQUIRE(reslen = sizeof("9876"));

    appBuf.PutInt16(-9876);
    BOOST_REQUIRE(!strcmp(buffer, "-9876"));
    BOOST_REQUIRE(reslen = sizeof("-9876"));

    appBuf.PutInt32(1234567);
    BOOST_REQUIRE(!strcmp(buffer, "1234567"));
    BOOST_REQUIRE(reslen = sizeof("1234567"));

    appBuf.PutInt32(-1234567);
    BOOST_REQUIRE(!strcmp(buffer, "-1234567"));
    BOOST_REQUIRE(reslen = sizeof("-1234567"));
}

BOOST_AUTO_TEST_CASE(TestFloatToString)
{
    char buffer[1024];
    int64_t reslen;

    ApplicationDataBuffer appBuf(IGNITE_SQL_TYPE_CHAR, buffer, sizeof(buffer), &reslen);

    appBuf.PutFloat(12.42f);
    BOOST_REQUIRE(!strcmp(buffer, "12.42"));
    BOOST_REQUIRE(reslen = sizeof("12.42"));

    appBuf.PutFloat(-12.42f);
    BOOST_REQUIRE(!strcmp(buffer, "-12.42"));
    BOOST_REQUIRE(reslen = sizeof("-12.42"));

    appBuf.PutDouble(1000.21);
    BOOST_REQUIRE(!strcmp(buffer, "1000.21"));
    BOOST_REQUIRE(reslen = sizeof("1000.21"));

    appBuf.PutDouble(-1000.21);
    BOOST_REQUIRE(!strcmp(buffer, "-1000.21"));
    BOOST_REQUIRE(reslen = sizeof("-1000.21"));
}

BOOST_AUTO_TEST_CASE(TestGuidToString)
{
    char buffer[1024];
    int64_t reslen;

    ApplicationDataBuffer appBuf(IGNITE_SQL_TYPE_CHAR, buffer, sizeof(buffer), &reslen);

    ignite::Guid guid(0x1da1ef8f39ff4d62ULL, 0x8b72e8e9f3371801ULL);

    appBuf.PutGuid(guid);

    BOOST_REQUIRE(!strcmp(buffer, "1da1ef8f-39ff-4d62-8b72-e8e9f3371801"));
    BOOST_REQUIRE(reslen = sizeof("1da1ef8f-39ff-4d62-8b72-e8e9f3371801"));
}

BOOST_AUTO_TEST_CASE(TestBinaryToString)
{
    char buffer[1024];
    int64_t reslen;

    ApplicationDataBuffer appBuf(IGNITE_SQL_TYPE_CHAR, buffer, sizeof(buffer), &reslen);

    int8_t binary[] = { 's', 'o', 'm', 'e', ' ', 'd', 'a', 't', 'a', '\0' };

    appBuf.PutBinaryData(binary, sizeof(binary));

    BOOST_REQUIRE(!strcmp(buffer, "some data"));
    BOOST_REQUIRE(reslen = sizeof("some data"));
}

BOOST_AUTO_TEST_CASE(TestStringToString)
{
    char buffer[1024];
    int64_t reslen;

    ApplicationDataBuffer appBuf(IGNITE_SQL_TYPE_CHAR, buffer, sizeof(buffer), &reslen);

    std::string testString("Test string");

    appBuf.PutString(testString);

    BOOST_REQUIRE(!strcmp(buffer, testString.c_str()));
    BOOST_REQUIRE(reslen = testString.size() + 1);
}

BOOST_AUTO_TEST_CASE(TestStringToWstring)
{
    wchar_t buffer[1024];
    int64_t reslen;

    ApplicationDataBuffer appBuf(IGNITE_SQL_TYPE_WCHAR, buffer, sizeof(buffer), &reslen);

    std::string testString("Test string");

    appBuf.PutString(testString);
    BOOST_REQUIRE(!wcscmp(buffer, L"Test string"));
}

BOOST_AUTO_TEST_CASE(TestStringToLong)
{
    long numBuf;
    int64_t reslen;

    ApplicationDataBuffer appBuf(IGNITE_SQL_TYPE_SIGNED_LONG, &numBuf, sizeof(numBuf), &reslen);

    appBuf.PutString("424242424");
    BOOST_REQUIRE(numBuf == 424242424L);

    appBuf.PutString("-424242424");
    BOOST_REQUIRE(numBuf == -424242424L);
}

BOOST_AUTO_TEST_CASE(TestStringToTiny)
{
    int8_t numBuf;
    int64_t reslen;

    ApplicationDataBuffer appBuf(IGNITE_SQL_TYPE_SIGNED_TINYINT, &numBuf, sizeof(numBuf), &reslen);

    appBuf.PutString("12");
    BOOST_REQUIRE(numBuf == 12);

    appBuf.PutString("-12");
    BOOST_REQUIRE(numBuf == -12);
}

BOOST_AUTO_TEST_CASE(TestStringToFloat)
{
    float numBuf;
    int64_t reslen;

    ApplicationDataBuffer appBuf(IGNITE_SQL_TYPE_FLOAT, &numBuf, sizeof(numBuf), &reslen);

    appBuf.PutString("12.21");
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, 12.21, 0.0000001);

    appBuf.PutString("-12.21");
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, -12.21, 0.0000001);
}

BOOST_AUTO_TEST_CASE(TestIntToFloat)
{
    float numBuf;
    int64_t reslen;

    ApplicationDataBuffer appBuf(IGNITE_SQL_TYPE_FLOAT, &numBuf, sizeof(numBuf), &reslen);

    appBuf.PutInt8(5);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, 5.0, 0.0000001);

    appBuf.PutInt8(-5);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, -5.0, 0.0000001);

    appBuf.PutInt16(4242);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, 4242.0, 0.0000001);

    appBuf.PutInt16(-4242);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, -4242.0, 0.0000001);

    appBuf.PutInt32(1234567);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, 1234567.0, 0.0000001);

    appBuf.PutInt32(-1234567);
    BOOST_REQUIRE_CLOSE_FRACTION(numBuf, -1234567.0, 0.0000001);
}

BOOST_AUTO_TEST_CASE(TestFloatToShort)
{
    short numBuf;
    int64_t reslen;

    ApplicationDataBuffer appBuf(IGNITE_SQL_TYPE_SIGNED_SHORT, &numBuf, sizeof(numBuf), &reslen);

    appBuf.PutDouble(5.42);
    BOOST_REQUIRE(numBuf == 5);

    appBuf.PutDouble(-5.42);
    BOOST_REQUIRE(numBuf == -5.0);

    appBuf.PutFloat(42.99f);
    BOOST_REQUIRE(numBuf == 42);

    appBuf.PutFloat(-42.99f);
    BOOST_REQUIRE(numBuf == -42);
}


BOOST_AUTO_TEST_SUITE_END()