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

#include "ignite/ignite_error.h"

using namespace ignite;
using namespace boost::unit_test;

BOOST_AUTO_TEST_SUITE(IgniteErrorTestSuite)

BOOST_AUTO_TEST_CASE(TestIgniteErrorDerivesStdException)
{
    const std::string testMsg = "Exception was not caught as it was supposed to.";

    try
    {
        throw IgniteError(IgniteError::IGNITE_ERR_GENERIC, testMsg.c_str());
    }
    catch (std::exception& e)
    {
        BOOST_REQUIRE_EQUAL(testMsg, std::string(e.what()));
    }
}

BOOST_AUTO_TEST_SUITE_END()
