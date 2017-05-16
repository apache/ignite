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

#define _USE_MATH_DEFINES

#ifdef _WIN32
#   include <windows.h>
#endif

#include <sql.h>
#include <sqlext.h>

#include <cmath>

#include <vector>
#include <string>

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include "ignite/ignite.h"
#include "ignite/ignition.h"
#include "ignite/impl/binary/binary_utils.h"

#include "test_type.h"
#include "test_utils.h"
#include "sql_test_suite_fixture.h"

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;
using namespace ignite_test;

using namespace boost::unit_test;

using ignite::impl::binary::BinaryUtils;

BOOST_FIXTURE_TEST_SUITE(SqlAggregateFunctionTestSuite, ignite::SqlTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestAggregateFunctionAvgInt)
{
    std::vector<TestType> in(3);

    in[0].i32Field = 43;
    in[1].i32Field = 311;
    in[2].i32Field = 7;

    int32_t avg = 0;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
    {
        testCache.Put(i, in[i]);

        avg += in[i].i32Field;
    }

    avg /= static_cast<int32_t>(in.size());

    CheckSingleResult<int64_t>("SELECT {fn AVG(i32Field)} FROM TestType", avg);
}

BOOST_AUTO_TEST_CASE(TestAggregateFunctionAvgIntDistinct)
{
    std::vector<TestType> in(3);

    in[0].i32Field = 43;
    in[1].i32Field = 311;
    in[2].i32Field = 7;

    int32_t avg = 0;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
    {
        testCache.Put(i, in[i]);

        avg += in[i].i32Field;
    }

    avg /= static_cast<int32_t>(in.size());

    testCache.Put(in.size() + 10, in[0]);

    CheckSingleResult<int64_t>("SELECT {fn AVG(DISTINCT i32Field)} FROM TestType", avg);
}

BOOST_AUTO_TEST_CASE(TestAggregateFunctionAvgFloat)
{
    std::vector<TestType> in(3);

    in[0].floatField = 43.0;
    in[1].floatField = 311.0;
    in[2].floatField = 7.0;

    float avg = 0;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
    {
        testCache.Put(i, in[i]);

        avg += in[i].floatField;
    }

    avg /= in.size();

    CheckSingleResult<float>("SELECT {fn AVG(floatField)} FROM TestType", avg);
}

BOOST_AUTO_TEST_CASE(TestAggregateFunctionAvgFloatDistinct)
{
    std::vector<TestType> in(3);

    in[0].floatField = 43.0;
    in[1].floatField = 311.0;
    in[2].floatField = 7.0;

    float avg = 0;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
    {
        testCache.Put(i, in[i]);

        avg += in[i].floatField;
    }

    avg /= in.size();

    testCache.Put(in.size() + 10, in[0]);

    CheckSingleResult<float>("SELECT {fn AVG(DISTINCT floatField)} FROM TestType", avg);
}

BOOST_AUTO_TEST_CASE(TestAggregateFunctionCount)
{
    std::vector<TestType> in(8);

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
        testCache.Put(i, in[i]);

    CheckSingleResult<int64_t>("SELECT {fn COUNT(*)} FROM TestType", in.size());
}

BOOST_AUTO_TEST_CASE(TestAggregateFunctionCountDistinct)
{
    std::vector<TestType> in(8);

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
    {
        in[i].i32Field = i;

        testCache.Put(i, in[i]);
    }

    testCache.Put(in.size() + 10, in[0]);

    CheckSingleResult<int64_t>("SELECT {fn COUNT(DISTINCT i32Field)} FROM TestType", in.size());
}

BOOST_AUTO_TEST_CASE(TestAggregateFunctionMax)
{
    std::vector<TestType> in(4);

    in[0].i32Field = 121;
    in[1].i32Field = 17;
    in[2].i32Field = 314041;
    in[3].i32Field = 9410;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
        testCache.Put(i, in[i]);

    CheckSingleResult<int64_t>("SELECT {fn MAX(i32Field)} FROM TestType", in[2].i32Field);
}

BOOST_AUTO_TEST_CASE(TestAggregateFunctionMin)
{
    std::vector<TestType> in(4);

    in[0].i32Field = 121;
    in[1].i32Field = 17;
    in[2].i32Field = 314041;
    in[3].i32Field = 9410;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
        testCache.Put(i, in[i]);

    CheckSingleResult<int64_t>("SELECT {fn MIN(i32Field)} FROM TestType", in[1].i32Field);
}

BOOST_AUTO_TEST_CASE(TestAggregateFunctionSum)
{
    std::vector<TestType> in(4);

    in[0].i32Field = 121;
    in[1].i32Field = 17;
    in[2].i32Field = 314041;
    in[3].i32Field = 9410;

    int64_t sum = 0;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
    {
        testCache.Put(i, in[i]);

        sum += in[i].i32Field;
    }

    CheckSingleResult<int64_t>("SELECT {fn SUM(i32Field)} FROM TestType", sum);
}

BOOST_AUTO_TEST_CASE(TestAggregateFunctionSumDistinct)
{
    std::vector<TestType> in(4);

    in[0].i32Field = 121;
    in[1].i32Field = 17;
    in[2].i32Field = 314041;
    in[3].i32Field = 9410;

    int64_t sum = 0;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
    {
        testCache.Put(i, in[i]);

        sum += in[i].i32Field;
    }

    testCache.Put(in.size() + 10, in[0]);

    CheckSingleResult<int64_t>("SELECT {fn SUM(DISTINCT i32Field)} FROM TestType", sum);
}

BOOST_AUTO_TEST_SUITE_END()
