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
    #define BOOST_TEST_DYN_LINK
#endif

#include <boost/test/unit_test.hpp>

#include "ignite/ignite_error.h"

using namespace ignite;
using namespace boost::unit_test;

BOOST_AUTO_TEST_SUITE(IgniteErrorTestSuite)

BOOST_AUTO_TEST_CASE(TestIgniteErrorDerivesStdException)
{
    try
    {
        throw IgniteError();
    }
    catch (std::exception&)
    {
        // No operations.
    }
    catch (IgniteError&)
    {
        BOOST_FAIL("Should not reach here if implements std::exception");
    }
}

BOOST_AUTO_TEST_SUITE_END()