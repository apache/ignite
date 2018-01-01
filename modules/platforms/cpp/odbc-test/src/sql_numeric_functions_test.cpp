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

BOOST_FIXTURE_TEST_SUITE(SqlNumericFunctionTestSuite, ignite::SqlTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestNumericFunctionAbs)
{
    TestType in;

    in.i32Field = -42;

    testCache.Put(1, in);

    CheckSingleResult<int32_t>("SELECT {fn ABS(i32Field)} FROM TestType", std::abs(in.i32Field));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionAcos)
{
    TestType in;

    in.doubleField = 0.32;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn ACOS(doubleField)} FROM TestType", std::acos(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionAsin)
{
    TestType in;

    in.doubleField = 0.12;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn ASIN(doubleField)} FROM TestType", std::asin(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionAtan)
{
    TestType in;

    in.doubleField = 0.14;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn ATAN(doubleField)} FROM TestType", std::atan(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionAtan2)
{
    TestType in;

    in.doubleField = 0.24;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn ATAN2(doubleField, 0.2)} FROM TestType", std::atan2(in.doubleField, 0.2));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionCeiling)
{
    TestType in;

    in.doubleField = 7.31;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn CEILING(doubleField)} FROM TestType", std::ceil(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionCos)
{
    TestType in;

    in.doubleField = 2.31;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn COS(doubleField)} FROM TestType", std::cos(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionCot)
{
    TestType in;

    in.doubleField = 2.31;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn COT(doubleField)} FROM TestType", 1 / std::tan(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionDegrees)
{
    TestType in;

    in.doubleField = 2.31;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn DEGREES(doubleField)} FROM TestType", in.doubleField * M_1_PI * 180);
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionExp)
{
    TestType in;

    in.doubleField = 1.23;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn EXP(doubleField)} FROM TestType", std::exp(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionFloor)
{
    TestType in;

    in.doubleField = 5.29;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn FLOOR(doubleField)} FROM TestType", std::floor(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionLog)
{
    TestType in;

    in.doubleField = 15.3;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn LOG(doubleField)} FROM TestType", std::log(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionLog10)
{
    TestType in;

    in.doubleField = 15.3;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn LOG10(doubleField)} FROM TestType", std::log10(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionMod)
{
    TestType in;

    in.i64Field = 26;

    testCache.Put(1, in);

    CheckSingleResult<int64_t>("SELECT {fn MOD(i64Field, 3)} FROM TestType", in.i64Field % 3);
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionPi)
{
    CheckSingleResult<double>("SELECT {fn PI()}", M_PI);
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionPower)
{
    TestType in;

    in.doubleField = 1.81;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn POWER(doubleField, 2.5)} FROM TestType", std::pow(in.doubleField, 2.5));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionRadians)
{
    TestType in;

    in.doubleField = 161;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn RADIANS(doubleField)} FROM TestType", in.doubleField * M_PI / 180.0);
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionRand)
{
    CheckSingleResult<double>("SELECT {fn RAND()} * 0", 0);
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionRound)
{
    TestType in;

    in.doubleField = 5.29;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn ROUND(doubleField)} FROM TestType", std::floor(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionSign)
{
    TestType in;

    in.doubleField = -1.39;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn SIGN(doubleField)} FROM TestType", in.doubleField < 0 ? -1 : in.doubleField == 0 ? 0 : 1);
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionSin)
{
    TestType in;

    in.doubleField = 1.01;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn SIN(doubleField)} FROM TestType", std::sin(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionSqrt)
{
    TestType in;

    in.doubleField = 2.56;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn SQRT(doubleField)} FROM TestType", std::sqrt(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionTan)
{
    TestType in;

    in.doubleField = 0.56;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn TAN(doubleField)} FROM TestType", std::tan(in.doubleField));
}

BOOST_AUTO_TEST_CASE(TestNumericFunctionTruncate)
{
    TestType in;

    in.doubleField = 4.17133;

    testCache.Put(1, in);

    CheckSingleResult<double>("SELECT {fn TRUNCATE(doubleField, 3)} FROM TestType", 4.171);
}

BOOST_AUTO_TEST_SUITE_END()
