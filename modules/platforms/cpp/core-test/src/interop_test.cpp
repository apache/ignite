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

#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;
using namespace boost::unit_test;

BOOST_AUTO_TEST_SUITE(InteropTestSuite)

BOOST_AUTO_TEST_CASE(StringUtfTest)
{
    IgniteConfiguration cfg;

    Ignite ignite = Ignition::Start(cfg);

    Cache<std::string, std::string> cache = ignite.CreateCache<std::string, std::string>("Test");

    std::string initialValue;

    initialValue.push_back(0xD8);
    initialValue.push_back(0x00);

    try
    {
        cache.Put("key", initialValue);

        std::string cachedValue = cache.Get("key");

        BOOST_FAIL("Exception is expected due to invalid format.");
    }
    catch (const IgniteError&)
    {
        // Expected.
    }

    Ignition::StopAll(false);
}

BOOST_AUTO_TEST_SUITE_END()