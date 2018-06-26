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

#include <ignite/complex_type.h>
#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>

#include <test_utils.h>

using namespace ignite::thin;
using namespace boost::unit_test;

class CacheClientTestSuiteFixture
{
public:
    static ignite::Ignite StartNode(const char* name)
    {
        return ignite_test::StartCrossPlatformServerNode("cache.xml", name);
    }

    CacheClientTestSuiteFixture()
    {
        serverNode = StartNode("ServerNode");
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

    BOOST_REQUIRE_THROW((client.GetCache<int32_t, std::string>("unknown")), ignite::IgniteError);
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

BOOST_AUTO_TEST_CASE(CacheClientDestroyCacheExisting)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110"); 

    IgniteClient client = IgniteClient::Start(cfg);
    
    client.DestroyCache("local");
}

BOOST_AUTO_TEST_CASE(CacheClientDestroyCacheNonexisting)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110"); 

    IgniteClient client = IgniteClient::Start(cfg);
    
    BOOST_REQUIRE_THROW(client.DestroyCache("unknown"), ignite::IgniteError);
}

BOOST_AUTO_TEST_CASE(CacheClientGetCacheNames)
{
    std::set<std::string> expectedNames;

    expectedNames.insert("local_atomic");
    expectedNames.insert("partitioned_atomic_near");
    expectedNames.insert("partitioned_near");
    expectedNames.insert("partitioned2");
    expectedNames.insert("partitioned");
    expectedNames.insert("replicated");
    expectedNames.insert("replicated_atomic");
    expectedNames.insert("local");
    expectedNames.insert("partitioned_atomic");

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110"); 

    IgniteClient client = IgniteClient::Start(cfg);

    std::vector<std::string> caches;

    client.GetCacheNames(caches);

    BOOST_CHECK_EQUAL(expectedNames.size(), caches.size());

    for (std::vector<std::string>::const_iterator it = caches.begin(); it != caches.end(); ++it)
    {
        BOOST_CHECK_EQUAL(expectedNames.count(*it), 1);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPutGetBasicKeyValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache = client.GetCache<int32_t, std::string>("local");

    int32_t key = 42;
    std::string valIn = "Lorem ipsum";

    cache.Put(key, valIn);

    std::string valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valOut, valIn);
}

BOOST_AUTO_TEST_CASE(CacheClientPutGetComplexValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, ignite::ComplexType> cache = client.GetCache<int32_t, ignite::ComplexType>("local");

    ignite::ComplexType valIn;

    int32_t key = 42;

    valIn.i32Field = 123;
    valIn.strField = "Test value";
    valIn.objField.f1 = 42;
    valIn.objField.f2 = "Inner value";

    cache.Put(key, valIn);

    ignite::ComplexType valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valIn.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn.objField.f2, valOut.objField.f2);
}

BOOST_AUTO_TEST_CASE(CacheClientPutGetComplexKey)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<ignite::ComplexType, int32_t> cache = client.GetCache<ignite::ComplexType, int32_t>("local");

    ignite::ComplexType key;

    key.i32Field = 123;
    key.strField = "Test value";
    key.objField.f1 = 42;
    key.objField.f2 = "Inner value";

    int32_t valIn = 42;

    cache.Put(key, valIn);

    int32_t valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valIn, valOut);
}

BOOST_AUTO_TEST_CASE(CacheClientContainsBasicKey)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache = client.GetCache<int32_t, std::string>("local");

    int32_t key = 42;
    std::string valIn = "Lorem ipsum";

    BOOST_CHECK(!cache.ContainsKey(key));

    cache.Put(key, valIn);
    
    BOOST_CHECK(cache.ContainsKey(key));
}

BOOST_AUTO_TEST_CASE(CacheClientContainsComplexKey)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);
    
    cache::CacheClient<ignite::ComplexType, int32_t> cache = client.GetCache<ignite::ComplexType, int32_t>("local");

    ignite::ComplexType key;

    key.i32Field = 123;
    key.strField = "Test value";
    key.objField.f1 = 42;
    key.objField.f2 = "Inner value";

    int32_t valIn = 42;

    BOOST_CHECK(!cache.ContainsKey(key));

    cache.Put(key, valIn);
    
    BOOST_CHECK(cache.ContainsKey(key));
}

BOOST_AUTO_TEST_CASE(CacheClientUpdatePartitions)
{
    StartNode("node1");
    StartNode("node2");

    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, int32_t> cache = client.GetCache<int32_t, int32_t>("partitioned");

    for (int32_t i = 0; i < 1024; ++i)
        cache.Put(i, i * 10);   

    cache.UpdatePartitions();
}

BOOST_AUTO_TEST_SUITE_END()
