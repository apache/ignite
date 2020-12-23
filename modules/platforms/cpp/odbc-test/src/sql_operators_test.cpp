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

#include <boost/test/unit_test.hpp>

#include "test_type.h"
#include "sql_test_suite_fixture.h"
#include <ignite/common/decimal.h>

using namespace ignite;

BOOST_FIXTURE_TEST_SUITE(SqlOperatorTestSuite, ignite::SqlTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestOperatorAddInt)
{
    CheckSingleResult<SQLINTEGER>("SELECT 123 + 51", 123 + 51);
}

BOOST_AUTO_TEST_CASE(TestOperatorSubInt)
{
    CheckSingleResult<SQLINTEGER>("SELECT 123 - 51", 123 - 51);
}

BOOST_AUTO_TEST_CASE(TestOperatorDivInt)
{
    CheckSingleResult<SQLINTEGER>("SELECT 123 / 51", 123 / 51);
}

BOOST_AUTO_TEST_CASE(TestOperatorModInt)
{
    CheckSingleResult<SQLINTEGER>("SELECT 123 % 51", 123 % 51);
}

BOOST_AUTO_TEST_CASE(TestOperatorMultInt)
{
    CheckSingleResult<SQLINTEGER>("SELECT 123 * 51", 123 * 51);
}

BOOST_AUTO_TEST_CASE(TestOperatorAddDouble)
{
    CheckSingleResult<double>("SELECT 123.0 + 51.0", 123.0 + 51.0);
}

BOOST_AUTO_TEST_CASE(TestOperatorSubDouble)
{
    CheckSingleResult<double>("SELECT 123.0 - 51.0", 123.0 - 51.0);
}

BOOST_AUTO_TEST_CASE(TestOperatorDivDouble)
{
    CheckSingleResult<double>("SELECT 123.0 / 51.0", 123.0 / 51.0);
}

BOOST_AUTO_TEST_CASE(TestOperatorModDouble)
{
    CheckSingleResult<double>("SELECT 123.0 % 51.0", 123 % 51);
}

BOOST_AUTO_TEST_CASE(TestOperatorMultDouble)
{
    CheckSingleResult<double>("SELECT 123.0 * 51.0", 123.0 * 51.0);
}

BOOST_AUTO_TEST_CASE(TestOperatorConcatString)
{
    CheckSingleResult<std::string>("SELECT 'Hello' || ' ' || 'World' || '!'", "Hello World!");
}

BOOST_AUTO_TEST_CASE(TestOperatorGreaterInt)
{
    CheckSingleResult<bool>("SELECT 2 > 3", false);
    CheckSingleResult<bool>("SELECT 3 > 3", false);
    CheckSingleResult<bool>("SELECT 34 > 3", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorLessInt)
{
    CheckSingleResult<bool>("SELECT 4 < 4", false);
    CheckSingleResult<bool>("SELECT 4 < 4", false);
    CheckSingleResult<bool>("SELECT 8 < 42", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorGreaterEquealInt)
{
    CheckSingleResult<bool>("SELECT 2 >= 3", false);
    CheckSingleResult<bool>("SELECT 3 >= 3", true);
    CheckSingleResult<bool>("SELECT 34 >= 3", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorLessEquealInt)
{
    CheckSingleResult<bool>("SELECT 4 <= 3", false);
    CheckSingleResult<bool>("SELECT 4 <= 4", true);
    CheckSingleResult<bool>("SELECT 8 <= 42", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorEquealInt)
{
    CheckSingleResult<bool>("SELECT 4 = 3", false);
    CheckSingleResult<bool>("SELECT 4 = 4", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorNotEquealInt)
{
    CheckSingleResult<bool>("SELECT 4 != 3", true);
    CheckSingleResult<bool>("SELECT 4 != 4", false);
}

BOOST_AUTO_TEST_CASE(TestOperatorGreaterDouble)
{
    CheckSingleResult<bool>("SELECT 2 > 3", false);
    CheckSingleResult<bool>("SELECT 3 > 3", false);
    CheckSingleResult<bool>("SELECT 34 > 3", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorLessDouble)
{
    CheckSingleResult<bool>("SELECT 4.0 < 4.0", false);
    CheckSingleResult<bool>("SELECT 4.0 < 4.0", false);
    CheckSingleResult<bool>("SELECT 8.0 < 42.0", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorGreaterEquealDouble)
{
    CheckSingleResult<bool>("SELECT 2.0 >= 3.0", false);
    CheckSingleResult<bool>("SELECT 3.0 >= 3.0", true);
    CheckSingleResult<bool>("SELECT 34.0 >= 3.0", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorLessEquealDouble)
{
    CheckSingleResult<bool>("SELECT 4.0 <= 3.0", false);
    CheckSingleResult<bool>("SELECT 4.0 <= 4.0", true);
    CheckSingleResult<bool>("SELECT 8.0 <= 42.0", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorEquealDouble)
{
    CheckSingleResult<bool>("SELECT 4.0 = 3.0", false);
    CheckSingleResult<bool>("SELECT 4.0 = 4.0", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorNotEquealDouble)
{
    CheckSingleResult<bool>("SELECT 4.0 != 3.0", true);
    CheckSingleResult<bool>("SELECT 4.0 != 4.0", false);
}

BOOST_AUTO_TEST_CASE(TestOperatorGreaterString)
{
    CheckSingleResult<bool>("SELECT 'abc' > 'bcd'", false);
    CheckSingleResult<bool>("SELECT 'abc' > 'abc'", false);
    CheckSingleResult<bool>("SELECT 'bcd' > 'abc'", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorLessString)
{
    CheckSingleResult<bool>("SELECT 'bcd' < 'abc'", false);
    CheckSingleResult<bool>("SELECT 'abc' < 'abc'", false);
    CheckSingleResult<bool>("SELECT 'abc' < 'bcd'", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorGreaterEquealString)
{
    CheckSingleResult<bool>("SELECT 'abc' >= 'bcd'", false);
    CheckSingleResult<bool>("SELECT 'abc' >= 'abc'", true);
    CheckSingleResult<bool>("SELECT 'bcd' >= 'abc'", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorLessEquealString)
{
    CheckSingleResult<bool>("SELECT 'bcd' <= 'abc'", false);
    CheckSingleResult<bool>("SELECT 'abc' <= 'bcd'", true);
    CheckSingleResult<bool>("SELECT 'abc' <= 'abc'", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorEquealString)
{
    CheckSingleResult<bool>("SELECT 'bcd' = 'abc'", false);
    CheckSingleResult<bool>("SELECT 'abc' = 'abc'", true);
}

BOOST_AUTO_TEST_CASE(TestOperatorNotEquealString)
{
    CheckSingleResult<bool>("SELECT 'abc' != 'abc'", false);
    CheckSingleResult<bool>("SELECT 'bcd' != 'abc'", true);
}

BOOST_AUTO_TEST_SUITE_END()
