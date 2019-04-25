/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <boost/test/unit_test.hpp>

#include "sql_test_suite_fixture.h"

using namespace ignite;

using namespace boost::unit_test;

BOOST_FIXTURE_TEST_SUITE(SqlSystemFunctionTestSuite, ignite::SqlTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestSystemFunctionDatabase)
{
    CheckSingleResult<std::string>("SELECT {fn DATABASE()}");
}

BOOST_AUTO_TEST_CASE(TestSystemFunctionUser)
{
    CheckSingleResult<std::string>("SELECT {fn USER()}");
}

BOOST_AUTO_TEST_CASE(TestSystemFunctionIfnull)
{
    CheckSingleResult<SQLINTEGER>("SELECT {fn IFNULL(NULL, 42)}", 42);
}

BOOST_AUTO_TEST_SUITE_END()
