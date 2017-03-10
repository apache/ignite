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

using namespace ignite;

using namespace boost::unit_test;

BOOST_FIXTURE_TEST_SUITE(SqlDateTimeFunctionTestSuite, ignite::SqlTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestCurrentDate)
{
    CheckSingleResult<Date>("SELECT {fn CURRENT_DATE()}");
}

BOOST_AUTO_TEST_CASE(TestCurdate)
{
    CheckSingleResult<Date>("SELECT {fn CURDATE()}");
}

BOOST_AUTO_TEST_CASE(TestCurrentTime)
{
    CheckSingleResult<Timestamp>("SELECT {fn CURRENT_TIME()}");
}

BOOST_AUTO_TEST_CASE(TestCurtime)
{
    CheckSingleResult<Timestamp>("SELECT {fn CURTIME()}");
}

BOOST_AUTO_TEST_CASE(TestCurrentTimestamp)
{
    CheckSingleResult<Timestamp>("SELECT {fn CURRENT_TIMESTAMP()}");
}

BOOST_AUTO_TEST_CASE(TestDayname)
{
    TestType in;

    in.dateField = common::MakeDateGmt(2016, 8, 29);

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn DAYNAME(dateField)} FROM TestType", "Monday");
}

BOOST_AUTO_TEST_CASE(TestDayofmonth)
{
    TestType in;

    in.dateField = common::MakeDateGmt(2016, 8, 29);

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn DAYOFMONTH(dateField)} FROM TestType", 29);
    CheckSingleResult<int32_t>("SELECT {fn DAY_OF_MONTH(dateField)} FROM TestType", 29);
}

BOOST_AUTO_TEST_CASE(TestDayofweek)
{
    TestType in;

    in.dateField = common::MakeDateGmt(2016, 8, 29);

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn DAYOFWEEK(dateField)} FROM TestType", 2);
    CheckSingleResult<int32_t>("SELECT {fn DAY_OF_WEEK(dateField)} FROM TestType", 2);
}

BOOST_AUTO_TEST_CASE(TestDayofyear)
{
    TestType in;

    in.dateField = common::MakeDateGmt(2016, 8, 29);

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn DAYOFYEAR(dateField)} FROM TestType", 242);
    CheckSingleResult<int32_t>("SELECT {fn DAY_OF_YEAR(dateField)} FROM TestType", 242);
}

BOOST_AUTO_TEST_CASE(TestExtract)
{
    TestType in;

    in.timestampField = common::MakeTimestampGmt(2016, 2, 24, 13, 45, 23, 580695103);

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn EXTRACT(YEAR FROM timestampField)} FROM TestType", 2016);
    CheckSingleResult<int32_t>("SELECT {fn EXTRACT(MONTH FROM timestampField)} FROM TestType", 2);
    CheckSingleResult<int32_t>("SELECT {fn EXTRACT(DAY FROM timestampField)} FROM TestType", 24);
    CheckSingleResult<int32_t>("SELECT {fn EXTRACT(HOUR FROM timestampField)} FROM TestType", 13);
    CheckSingleResult<int32_t>("SELECT {fn EXTRACT(MINUTE FROM timestampField)} FROM TestType", 45);
    CheckSingleResult<int32_t>("SELECT {fn EXTRACT(SECOND FROM timestampField)} FROM TestType", 23);
}

BOOST_AUTO_TEST_CASE(TestHour)
{
    TestType in;

    in.timestampField = common::MakeTimestampGmt(2016, 2, 24, 13, 45, 23, 580695103);

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn HOUR(timestampField)} FROM TestType", 13);
}

BOOST_AUTO_TEST_CASE(TestMinute)
{
    TestType in;

    in.timestampField = common::MakeTimestampGmt(2016, 2, 24, 13, 45, 23, 580695103);

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn MINUTE(timestampField)} FROM TestType", 45);
}

BOOST_AUTO_TEST_CASE(TestMonth)
{
    TestType in;

    in.timestampField = common::MakeTimestampGmt(2016, 2, 24, 13, 45, 23, 580695103);

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn MONTH(timestampField)} FROM TestType", 2);
}

BOOST_AUTO_TEST_CASE(TestMonthname)
{
    TestType in;

    in.timestampField = common::MakeTimestampGmt(2016, 2, 24, 13, 45, 23, 580695103);

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT {fn MONTHNAME(timestampField)} FROM TestType", "February");
}

BOOST_AUTO_TEST_CASE(TestNow)
{
    CheckSingleResult<Timestamp>("SELECT {fn NOW()}");
}

BOOST_AUTO_TEST_CASE(TestQuarter)
{
    TestType in;

    in.timestampField = common::MakeTimestampGmt(2016, 2, 24, 13, 45, 23, 580695103);

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn QUARTER(timestampField)} FROM TestType", 1);
}

BOOST_AUTO_TEST_CASE(TestSecond)
{
    TestType in;

    in.timestampField = common::MakeTimestampGmt(2016, 2, 24, 13, 45, 23, 580695103);

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn SECOND(timestampField)} FROM TestType", 23);
}

BOOST_AUTO_TEST_CASE(TestWeek)
{
    TestType in;

    in.timestampField = common::MakeTimestampGmt(2016, 2, 24, 13, 45, 23, 580695103);

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn WEEK(timestampField)} FROM TestType", 9);
}

BOOST_AUTO_TEST_CASE(TestYear)
{
    TestType in;

    in.timestampField = common::MakeTimestampGmt(2016, 2, 24, 13, 45, 23, 580695103);

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn YEAR(timestampField)} FROM TestType", 2016);
}

BOOST_AUTO_TEST_SUITE_END()
