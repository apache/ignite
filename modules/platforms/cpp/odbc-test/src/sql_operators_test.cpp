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

#ifdef _WIN32
#   include <windows.h>
#endif

#include <vector>
#include <string>

#ifndef _MSC_VER
#   define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include "test_type.h"
#include "sql_function_test_suite_fixture.h"
#include <ignite/common/decimal.h>

using namespace ignite;

BOOST_FIXTURE_TEST_SUITE(SqlOperatorTestSuite, ignite::SqlFunctionTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestOperatorAddInt)
{
    CheckSingleResult<int32_t>("SELECT 123 + 51", 123 + 51);
};

BOOST_AUTO_TEST_CASE(TestOperatorSubInt)
{
    CheckSingleResult<int32_t>("SELECT 123 - 51", 123 - 51);
};

BOOST_AUTO_TEST_CASE(TestOperatorDivInt)
{
    CheckSingleResult<int32_t>("SELECT 123 / 51", 123 / 51);
};

BOOST_AUTO_TEST_CASE(TestOperatorModInt)
{
    CheckSingleResult<int32_t>("SELECT 123 % 51", 123 % 51);
};

BOOST_AUTO_TEST_CASE(TestOperatorMultInt)
{
    CheckSingleResult<int32_t>("SELECT 123 * 51", 123 * 51);
};

BOOST_AUTO_TEST_CASE(TestOperatorAddDouble)
{
    CheckSingleResult<double>("SELECT 123.0 + 51.0", 123.0 + 51.0);
};

BOOST_AUTO_TEST_CASE(TestOperatorSubDouble)
{
    CheckSingleResult<double>("SELECT 123.0 - 51.0", 123.0 - 51.0);
};

BOOST_AUTO_TEST_CASE(TestOperatorDivDouble)
{
    CheckSingleResult<double>("SELECT 123.0 / 51.0", 123.0 / 51.0);
};

BOOST_AUTO_TEST_CASE(TestOperatorModDouble)
{
    CheckSingleResult<double>("SELECT 123.0 % 51.0", 123 % 51);
};

BOOST_AUTO_TEST_CASE(TestOperatorMultDouble)
{
    CheckSingleResult<double>("SELECT 123.0 * 51.0", 123.0 * 51.0);
};

BOOST_AUTO_TEST_CASE(TestOperatorConcatString)
{
    CheckSingleResult<std::string>("SELECT \'Hello\' || \' \' || \'World\' || \'!\'", "Hello World!");
};

BOOST_AUTO_TEST_SUITE_END()
