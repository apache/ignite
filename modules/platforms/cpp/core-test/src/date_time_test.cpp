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

#include <boost/test/unit_test.hpp>

#include <ignite/time.h>
#include <ignite/date.h>
#include <ignite/timestamp.h>

#include <ignite/common/utils.h>

#include <ignite/test_utils.h>

using namespace ignite;
using namespace cache;
using namespace boost::unit_test;

/**
 * Check operators for type.
 * All args should refer to a different instances.
 * Also, val1 should be less then val2 and equeal val3.
 */
template<typename T>
void CheckOperators(const T& val1, const T& val2, const T& val3)
{
    BOOST_CHECK(&val1 != &val2);
    BOOST_CHECK(&val1 != &val3);
    BOOST_CHECK(&val2 != &val3);

    BOOST_CHECK(val1 == val1);
    BOOST_CHECK(val2 == val2);

    BOOST_CHECK(val1 == val3);
    BOOST_CHECK(val3 == val1);

    BOOST_CHECK(val1 != val2);
    BOOST_CHECK(val2 != val1);
    BOOST_CHECK(val3 != val2);
    BOOST_CHECK(val2 != val3);

    BOOST_CHECK(val1 < val2);
    BOOST_CHECK(val3 < val2);

    BOOST_CHECK(val1 <= val2);
    BOOST_CHECK(val3 <= val2);
    BOOST_CHECK(val1 <= val1);
    BOOST_CHECK(val2 <= val2);
    BOOST_CHECK(val1 <= val3);
    BOOST_CHECK(val3 <= val1);

    BOOST_CHECK(val2 > val1);
    BOOST_CHECK(val2 > val3);

    BOOST_CHECK(val2 >= val1);
    BOOST_CHECK(val2 >= val3);
    BOOST_CHECK(val1 >= val1);
    BOOST_CHECK(val2 >= val2);
    BOOST_CHECK(val1 >= val3);
    BOOST_CHECK(val3 >= val1);
}

void CheckTime(int hour, int mins, int sec)
{
    Time time = common::MakeTimeGmt(hour, mins, sec);
    tm res;

    common::TimeToCTm(time, res);

    BOOST_CHECK_EQUAL(res.tm_hour, hour);
    BOOST_CHECK_EQUAL(res.tm_min, mins);
    BOOST_CHECK_EQUAL(res.tm_sec, sec);
}

void CheckDate(int year, int mon, int day)
{
    Date date = common::MakeDateGmt(year, mon, day);
    tm res;

    common::DateToCTm(date, res);

    BOOST_CHECK_EQUAL(res.tm_year + 1900, year);
    BOOST_CHECK_EQUAL(res.tm_mon + 1, mon);
    BOOST_CHECK_EQUAL(res.tm_mday, day);
}

void CheckTimestamp(int year, int mon, int day, int hour, int mins, int sec, int ns)
{
    Timestamp ts = common::MakeTimestampGmt(year, mon, day, hour, mins, sec, ns);
    tm res;

    common::TimestampToCTm(ts, res);

    BOOST_CHECK_EQUAL(res.tm_year + 1900, year);
    BOOST_CHECK_EQUAL(res.tm_mon + 1, mon);
    BOOST_CHECK_EQUAL(res.tm_mday, day);
    BOOST_CHECK_EQUAL(res.tm_hour, hour);
    BOOST_CHECK_EQUAL(res.tm_min, mins);
    BOOST_CHECK_EQUAL(res.tm_sec, sec);
}

BOOST_AUTO_TEST_SUITE(DateTimeTestSuite)

BOOST_AUTO_TEST_CASE(TimeOperators1)
{
    Time val1(1);
    Time val2(2);
    Time val3(1);

    CheckOperators(val1, val2, val3);
}

BOOST_AUTO_TEST_CASE(TimeOperators2)
{
    Time val1(154362);
    Time val2(val1.GetMilliseconds() + 42363);
    Time val3(val1);

    CheckOperators(val1, val2, val3);
}

BOOST_AUTO_TEST_CASE(DateOperators1)
{
    Date val1(1);
    Date val2(2);
    Date val3(1);

    CheckOperators(val1, val2, val3);
}

BOOST_AUTO_TEST_CASE(DateOperators2)
{
    Date val1(154362);
    Date val2(val1.GetMilliseconds() + 42363);
    Date val3(val1);

    CheckOperators(val1, val2, val3);
}

BOOST_AUTO_TEST_CASE(TimestampOperators1)
{
    Timestamp val1(1);
    Timestamp val2(2);
    Timestamp val3(1);

    CheckOperators(val1, val2, val3);
}

BOOST_AUTO_TEST_CASE(TimestampOperators2)
{
    Timestamp val1(154362);
    Timestamp val2(val1.GetMilliseconds() + 42363);
    Timestamp val3(val1);

    CheckOperators(val1, val2, val3);
}

BOOST_AUTO_TEST_CASE(TimestampOperators3)
{
    Timestamp val1(42, 1);
    Timestamp val2(42, 2);
    Timestamp val3(42, 1);

    CheckOperators(val1, val2, val3);
}

BOOST_AUTO_TEST_CASE(TimestampOperators4)
{
    Timestamp val1(42, 154362);
    Timestamp val2(42, val1.GetSecondFraction() + 42363);
    Timestamp val3(42, val1.GetSecondFraction());

    CheckOperators(val1, val2, val3);
}

BOOST_AUTO_TEST_CASE(TimestampOperators5)
{
    Timestamp val1(154362, 154362);
    Timestamp val2(val1.GetMilliseconds() + 42363, val1.GetSecondFraction() + 42363);
    Timestamp val3(val1);

    CheckOperators(val1, val2, val3);
}

BOOST_AUTO_TEST_CASE(MakeTime)
{
    using namespace common;

    BOOST_CHECK_EQUAL(MakeTimeGmt(0, 0, 0).GetMilliseconds(), 0);
    BOOST_CHECK_EQUAL(MakeTimeGmt(23, 59, 59).GetMilliseconds(), 86399000);
    BOOST_CHECK_EQUAL(MakeTimeGmt(14, 23, 47).GetMilliseconds(), 51827000);
    BOOST_CHECK_EQUAL(MakeTimeGmt(0, 24, 12).GetMilliseconds(), 1452000);
    BOOST_CHECK_EQUAL(MakeTimeGmt(8, 0, 0).GetMilliseconds(), 28800000);

    BOOST_CHECK_EQUAL(MakeTimeGmt(0, 0, 0).GetSeconds(), 0);
    BOOST_CHECK_EQUAL(MakeTimeGmt(23, 59, 59).GetSeconds(), 86399);
    BOOST_CHECK_EQUAL(MakeTimeGmt(14, 23, 47).GetSeconds(), 51827);
    BOOST_CHECK_EQUAL(MakeTimeGmt(0, 24, 12).GetSeconds(), 1452);
    BOOST_CHECK_EQUAL(MakeTimeGmt(8, 0, 0).GetSeconds(), 28800);
}

BOOST_AUTO_TEST_CASE(MakeDate)
{
    using namespace common;

    BOOST_CHECK_EQUAL(MakeDateGmt(1970, 1, 1).GetMilliseconds(), 0);
    BOOST_CHECK_EQUAL(MakeDateGmt(2000, 12, 31).GetMilliseconds(), 978220800000);
    BOOST_CHECK_EQUAL(MakeDateGmt(2017, 3, 20).GetMilliseconds(), 1489968000000);

    BOOST_CHECK_EQUAL(MakeDateGmt(1970, 1, 1).GetSeconds(), 0);
    BOOST_CHECK_EQUAL(MakeDateGmt(2000, 12, 31).GetSeconds(), 978220800);
    BOOST_CHECK_EQUAL(MakeDateGmt(2017, 3, 20).GetSeconds(), 1489968000);
}

BOOST_AUTO_TEST_CASE(MakeTimestamp)
{
    using namespace common;

    BOOST_CHECK_EQUAL(MakeTimestampGmt(1970, 1, 1, 0, 0, 0, 0).GetMilliseconds(), 0);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2000, 12, 31, 23, 59, 59, 999999999).GetMilliseconds(), 978307199999);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2001, 9, 9, 1, 46, 39, 999999999).GetMilliseconds(), 999999999999);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2017, 3, 20, 18, 43, 19, 170038645).GetMilliseconds(), 1490035399170);

    BOOST_CHECK_EQUAL(MakeTimestampGmt(1970, 1, 1, 0, 0, 0, 0).GetSeconds(), 0);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2000, 12, 31, 23, 59, 59, 999999999).GetSeconds(), 978307199);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2001, 9, 9, 1, 46, 39, 999999999).GetSeconds(), 999999999);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2017, 3, 20, 18, 43, 19, 170038645).GetSeconds(), 1490035399);

    BOOST_CHECK_EQUAL(MakeTimestampGmt(1970, 1, 1, 0, 0, 0, 0).GetSecondFraction(), 0);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2000, 12, 31, 23, 59, 59, 999999999).GetSecondFraction(), 999999999);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2001, 9, 9, 1, 46, 39, 999999999).GetSecondFraction(), 999999999);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2017, 3, 20, 18, 43, 19, 170038645).GetSecondFraction(), 170038645);

    BOOST_CHECK_EQUAL(MakeTimestampGmt(1970, 1, 1, 0, 0, 0, 0).GetDate().GetMilliseconds(), 0);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2000, 12, 31, 23, 59, 59, 999999999).GetDate().GetMilliseconds(), 978307199999);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2001, 9, 9, 1, 46, 39, 999999999).GetDate().GetMilliseconds(), 999999999999);
    BOOST_CHECK_EQUAL(MakeTimestampGmt(2017, 3, 20, 18, 43, 19, 170038645).GetDate().GetMilliseconds(), 1490035399170);
}

BOOST_AUTO_TEST_CASE(CastTimeToTm)
{
    CheckTime(21, 8, 5);
    CheckTime(12, 41, 11);
    CheckTime(1, 28, 18);
    CheckTime(8, 12, 59);
    CheckTime(17, 52, 31);
    CheckTime(21, 56, 21);
}

BOOST_AUTO_TEST_CASE(CastDateToTm)
{
    CheckDate(2024, 8, 5);
    CheckDate(1987, 1, 1);
    CheckDate(1999, 2, 18);
    CheckDate(1997, 12, 9);
    CheckDate(2007, 12, 31);
    CheckDate(2001, 6, 21);
}

BOOST_AUTO_TEST_CASE(CastTimestampToTm)
{
    CheckTimestamp(1970, 1, 1, 0, 0, 0, 0);
    CheckTimestamp(2000, 12, 31, 23, 59, 59, 999999999);
    CheckTimestamp(2001, 9, 9, 1, 46, 39, 999999999);
    CheckTimestamp(2017, 3, 20, 18, 43, 19, 170038645);
    CheckTimestamp(2007, 12, 31, 19, 24, 44, 894375963);
    CheckTimestamp(2001, 6, 21, 13, 53, 2, 25346547);
}

BOOST_AUTO_TEST_SUITE_END()
