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

BOOST_FIXTURE_TEST_SUITE(SqlTypesTestSuite, ignite::SqlTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestGuidTrivial)
{
    CheckSingleResult<std::string>("SELECT {guid '04CC382A-0B82-F520-08D0-07A0620C0004'}",
        "04cc382a-0b82-f520-08d0-07a0620c0004");

    CheckSingleResult<std::string>("SELECT {guid '63802467-9f4a-4f71-8fc8-cf2d99a28ddf'}",
        "63802467-9f4a-4f71-8fc8-cf2d99a28ddf");
}

BOOST_AUTO_TEST_CASE(TestGuidEqualsToColumn)
{
    TestType in1;
    TestType in2;

    in1.guidField = Guid(0x638024679f4a4f71, 0x8fc8cf2d99a28ddf);
    in2.guidField = Guid(0x04cc382a0b82f520, 0x08d007a0620c0004);

    in1.i32Field = 1;
    in2.i32Field = 2;

    testCache.Put(1, in1);
    testCache.Put(2, in2);

    CheckSingleResult<int32_t>(
        "SELECT i32Field FROM TestType WHERE guidField = {guid '04cc382a-0b82-f520-08d0-07a0620c0004'}", in2.i32Field);
}


BOOST_AUTO_TEST_SUITE_END()
