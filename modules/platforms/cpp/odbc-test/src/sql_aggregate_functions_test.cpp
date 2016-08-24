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
#include "sql_function_test_suite_fixture.h"

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::query;
using namespace ignite::common;

using namespace boost::unit_test;

using ignite::impl::binary::BinaryUtils;

BOOST_FIXTURE_TEST_SUITE(SqlAggregateFunctionTestSuite, ignite::SqlFunctionTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestAggredateFunctionAvgInt)
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

BOOST_AUTO_TEST_CASE(TestAggredateFunctionAvgFloat)
{
    std::vector<TestType> in(3);

    in[0].floatField = 43.0;
    in[1].floatField = 311.0;
    in[2].floatField = 7.0;

    float avg = 0;

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
    {
        testCache.Put(i, in[i]);

        avg += in[i].i32Field;
    }

    avg /= in.size();

    CheckSingleResult<float>("SELECT {fn AVG(floatField)} FROM TestType", avg);
}

BOOST_AUTO_TEST_CASE(TestAggredateFunctionCount)
{
    std::vector<TestType> in(8);

    for (int32_t i = 0; i < static_cast<int32_t>(in.size()); ++i)
        testCache.Put(i, in[i]);

    CheckSingleResult<int64_t>("SELECT {fn COUNT(*)} FROM TestType", in.size());
}

BOOST_AUTO_TEST_CASE(TestAggredateFunctionMax)
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

BOOST_AUTO_TEST_CASE(TestAggredateFunctionMin)
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

BOOST_AUTO_TEST_CASE(TestAggredateFunctionSum)
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

BOOST_AUTO_TEST_SUITE_END()
