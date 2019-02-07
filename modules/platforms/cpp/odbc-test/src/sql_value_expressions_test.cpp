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

#include "sql_test_suite_fixture.h"

using namespace ignite;

using namespace boost::unit_test;

BOOST_FIXTURE_TEST_SUITE(SqlValueExpressionTestSuite, ignite::SqlTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestCase)
{
    TestType in;

    in.i32Field = 82;

    testCache.Put(1, in);

    SqlTestSuiteFixture::CheckSingleResult<SQLINTEGER>(
        "SELECT "
            "CASE i32Field WHEN 82 "
                "THEN (i32Field / 2) "
                "ELSE (i32Field / 3) "
            "END "
        "FROM TestType", in.i32Field / 2);

    SqlTestSuiteFixture::CheckSingleResult<SQLINTEGER>(
        "SELECT "
            "CASE i32Field WHEN 22 "
                "THEN (i32Field / 2) "
                "ELSE (i32Field / 3) "
            "END "
        "FROM TestType", in.i32Field / 3);
}

BOOST_AUTO_TEST_CASE(TestCast)
{
    TestType in;

    in.i32Field = 12345;
    in.strField = "54321";

    testCache.Put(1, in);

    CheckSingleResult<SQLINTEGER>("SELECT CAST(strField AS INT) + i32Field FROM TestType", 
        common::LexicalCast<int32_t>(in.strField) + in.i32Field);

    CheckSingleResult<std::string>("SELECT CAST(i32Field AS VARCHAR) || strField FROM TestType",
        common::LexicalCast<std::string>(in.i32Field) + in.strField);
}

BOOST_AUTO_TEST_CASE(TestCoalesce)
{
    CheckSingleResult<std::string>("SELECT COALESCE('One', 'Two', 'Three')", "One");
    CheckSingleResult<std::string>("SELECT COALESCE(NULL, 'Two', 'Three')", "Two");
    CheckSingleResult<std::string>("SELECT COALESCE(NULL, 'Two', NULL)", "Two");
    CheckSingleResult<std::string>("SELECT COALESCE(NULL, NULL, 'Three')", "Three");
}

BOOST_AUTO_TEST_CASE(TestNullif)
{
    TestType in;

    in.strField = "SomeValue";

    testCache.Put(1, in);

    CheckSingleResult<std::string>("SELECT NULLIF(strField, 'blablabla') FROM TestType", in.strField);
    CheckSingleResult<std::string>("SELECT NULLIF(strField, 'SomeValue') FROM TestType", "");
}

BOOST_AUTO_TEST_SUITE_END()
