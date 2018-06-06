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

#include <ignite/ignition.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>

#include <test_utils.h>

using namespace ignite::thin;
using namespace boost::unit_test;

class CacheClientTestSuiteFixture
{
public:
    CacheClientTestSuiteFixture()
    {
        serverNode = ignite_test::StartCrossPlatformServerNode("cache.xml", "ServerNode");
    }

    ~CacheClientTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

private:
    /** Server node. */
    ignite::Ignite serverNode;
};

BOOST_FIXTURE_TEST_SUITE(CacheClientTestSuite, CacheClientTestSuiteFixture)

BOOST_AUTO_TEST_CASE(CacheClientGetCacheExisting)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    client.GetCache<int32_t, std::string>("local");
}

BOOST_AUTO_TEST_CASE(CacheClientGetCacheNonxisting)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    client.GetCache<int32_t, std::string>("unknown");
}

BOOST_AUTO_TEST_CASE(CacheClientGetOrCreateCacheExisting)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    client.GetOrCreateCache<std::string, int32_t>("local");
}

BOOST_AUTO_TEST_CASE(CacheClientGetOrCreateCacheNonexisting)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    client.GetOrCreateCache<std::string, int32_t>("unknown");
}

BOOST_AUTO_TEST_CASE(CacheClientCreateCacheExisting)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    BOOST_REQUIRE_THROW((client.CreateCache<std::string, int32_t>("local")), ignite::IgniteError);
}

BOOST_AUTO_TEST_CASE(CacheClientCreateCacheNonexisting)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110"); 

    IgniteClient client = IgniteClient::Start(cfg);

    client.CreateCache<std::string, int32_t>("unknown");
}

BOOST_AUTO_TEST_CASE(CacheClientPutGet)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache = client.GetCache<int32_t, std::string>("local");

    cache.Put(42, "Lorem ipsum");

    std::string val = cache.Get(42);

    BOOST_CHECK(val, std::string("Lorem ipsum"));
}

BOOST_AUTO_TEST_SUITE_END()
