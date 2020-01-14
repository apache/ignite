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
#include <boost/thread/thread.hpp>

#include <ignite/ignition.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>

#include <ignite/thin/cache/cache_peek_mode.h>

#include <ignite/complex_type.h>
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

    template<typename K, typename V>
    void PutWithRetry(cache::CacheClient<K,V>& cache, const K& key, const V& value)
    {
        try
        {
            cache.Put(key, value);
        }
        catch(const ignite::IgniteError& err)
        {
            if (err.GetCode() == ignite::IgniteError::IGNITE_ERR_NETWORK_FAILURE)
            {
                cache.Put(key, value);
            }
        }
    }

    template<typename K, typename V>
    void LocalPeek(cache::CacheClient<K,V>& cache, const K& key, V& value)
    {
        using namespace ignite::impl::thin;
        using namespace ignite::impl::thin::cache;

        CacheClientProxy& proxy = CacheClientProxy::GetFromCacheClient(cache);

        WritableKeyImpl<K> wkey(key);
        ReadableImpl<V> rvalue(value);

        proxy.LocalPeek(wkey, rvalue);
    }

    template<typename KeyType>
    void NumPartitionTest(int64_t num)
    {
        StartNode("node1");
        StartNode("node2");

        boost::this_thread::sleep_for(boost::chrono::seconds(2));

        IgniteClientConfiguration cfg;
        cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");
        cfg.SetPartitionAwareness(true);

        IgniteClient client = IgniteClient::Start(cfg);

        cache::CacheClient<KeyType, int64_t> cache =
            client.GetCache<KeyType, int64_t>("partitioned");

        cache.RefreshAffinityMapping();

        for (int64_t i = 1; i < num; ++i)
            cache.Put(static_cast<KeyType>(i * 39916801), i * 5039);

        for (int64_t i = 1; i < num; ++i)
        {
            int64_t val;
            LocalPeek(cache, static_cast<KeyType>(i * 39916801), val);

            BOOST_REQUIRE_EQUAL(val, i * 5039);
        }
    }

    void SizeTest(int32_t peekMode)
    {
        IgniteClientConfiguration cfg;
        cfg.SetEndPoints("127.0.0.1:11110");

        IgniteClient client = IgniteClient::Start(cfg);

        cache::CacheClient<int32_t, int32_t> cache =
            client.GetCache<int32_t, int32_t>("partitioned");

        for (int32_t i = 0; i < 1000; ++i)
            cache.Put(i, i * 5039);

        int64_t size = cache.GetSize(peekMode);
        BOOST_CHECK_EQUAL(size, 1000);
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

BOOST_AUTO_TEST_CASE(CacheClientReplaceBasicKey)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache = client.GetCache<int32_t, std::string>("local");

    int32_t key = 42;
    std::string valIn = "Lorem ipsum";

    cache.Put(key, valIn);

    BOOST_REQUIRE(!cache.Replace(1, "Test"));
    BOOST_REQUIRE(cache.Replace(42, "Test"));

    BOOST_REQUIRE_EQUAL(cache.Get(1), "");
    BOOST_REQUIRE_EQUAL(cache.Get(42), "Test");
}

BOOST_AUTO_TEST_CASE(CacheClientReplaceComplexKey)
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

    BOOST_REQUIRE(!cache.Replace(ignite::ComplexType(), 2));
    BOOST_REQUIRE(cache.Replace(key, 13));

    BOOST_REQUIRE_EQUAL(cache.Get(key), 13);
    BOOST_REQUIRE_EQUAL(cache.Get(ignite::ComplexType()), 0);
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsInt8)
{
    NumPartitionTest<int8_t>(100);
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsInt16)
{
    NumPartitionTest<int16_t>(2000);
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsInt32)
{
    NumPartitionTest<int32_t>(1050);
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsInt64)
{
    NumPartitionTest<int64_t>(2000);
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsUint16)
{
    NumPartitionTest<uint16_t>(1500);
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsFloat)
{
    NumPartitionTest<float>(1500);
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsDouble)
{
    NumPartitionTest<double>(500);
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsString)
{
    StartNode("node1");
    StartNode("node2");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");
    cfg.SetPartitionAwareness(true);

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<std::string, int64_t> cache =
        client.GetCache<std::string, int64_t>("partitioned");

    cache.RefreshAffinityMapping();

    for (int64_t i = 1; i < 1000; ++i)
        cache.Put(ignite::common::LexicalCast<std::string>(i * 39916801), i * 5039);

    for (int64_t i = 1; i < 1000; ++i)
    {
        int64_t val;
        LocalPeek(cache, ignite::common::LexicalCast<std::string>(i * 39916801), val);

        BOOST_REQUIRE_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsGuid)
{
    StartNode("node1");
    StartNode("node2");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");
    cfg.SetPartitionAwareness(true);

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<ignite::Guid, int64_t> cache =
        client.GetCache<ignite::Guid, int64_t>("partitioned");

    cache.RefreshAffinityMapping();

    for (int64_t i = 1; i < 1000; ++i)
        cache.Put(ignite::Guid(i * 406586897, i * 87178291199), i * 5039);

    for (int64_t i = 1; i < 1000; ++i)
    {
        int64_t val;
        LocalPeek(cache, ignite::Guid(i * 406586897, i * 87178291199), val);

        BOOST_REQUIRE_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsComplexType)
{
    StartNode("node1");
    StartNode("node2");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");
    cfg.SetPartitionAwareness(true);

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<ignite::ComplexType, int64_t> cache =
        client.GetCache<ignite::ComplexType, int64_t>("partitioned");

    cache.RefreshAffinityMapping();

    for (int64_t i = 1; i < 1000; ++i)
    {
        ignite::ComplexType key;

        key.i32Field = static_cast<int32_t>(i * 406586897);
        key.strField = ignite::common::LexicalCast<std::string>(i * 39916801);
        key.objField.f1 = static_cast<int32_t>(i * 87178291199);
        key.objField.f2 = ignite::common::LexicalCast<std::string>(i * 59969537);

        cache.Put(key, i * 5039);
    }

    for (int64_t i = 1; i < 1000; ++i)
    {
        ignite::ComplexType key;

        key.i32Field = static_cast<int32_t>(i * 406586897);
        key.strField = ignite::common::LexicalCast<std::string>(i * 39916801);
        key.objField.f1 = static_cast<int32_t>(i * 87178291199);
        key.objField.f2 = ignite::common::LexicalCast<std::string>(i * 59969537);

        int64_t val;
        LocalPeek(cache, key, val);

        BOOST_REQUIRE_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsDate)
{
    StartNode("node1");
    StartNode("node2");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");
    cfg.SetPartitionAwareness(true);

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<ignite::Date, int64_t> cache =
        client.GetCache<ignite::Date, int64_t>("partitioned");

    cache.RefreshAffinityMapping();

    for (int64_t i = 1; i < 1000; ++i)
        cache.Put(ignite::common::MakeDateGmt(
            static_cast<int>(1990 + i),
            std::abs(static_cast<int>((i * 87178291199) % 11) + 1),
            std::abs(static_cast<int>((i * 39916801) % 27) + 1),
            std::abs(static_cast<int>(9834497 * i) % 24),
            std::abs(static_cast<int>(i * 87178291199) % 60),
            std::abs(static_cast<int>(i * 39916801) % 60)),
            i * 5039);

    for (int64_t i = 1; i < 1000; ++i)
    {
        int64_t val;
        LocalPeek(cache, ignite::common::MakeDateGmt(
            static_cast<int>(1990 + i),
            std::abs(static_cast<int>((i * 87178291199) % 11) + 1),
            std::abs(static_cast<int>((i * 39916801) % 27) + 1),
            std::abs(static_cast<int>(9834497 * i) % 24),
            std::abs(static_cast<int>(i * 87178291199) % 60),
            std::abs(static_cast<int>(i * 39916801) % 60)),
            val);

        BOOST_REQUIRE_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsTime)
{
    StartNode("node1");
    StartNode("node2");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");
    cfg.SetPartitionAwareness(true);

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<ignite::Time, int64_t> cache =
        client.GetCache<ignite::Time, int64_t>("partitioned");

    cache.RefreshAffinityMapping();

    for (int64_t i = 1; i < 100; ++i)
        cache.Put(ignite::common::MakeTimeGmt(
            std::abs(static_cast<int>(9834497 * i) % 24),
            std::abs(static_cast<int>(i * 87178291199) % 60),
            std::abs(static_cast<int>(i * 39916801) % 60)),
            i * 5039);

    for (int64_t i = 1; i < 100; ++i)
    {
        int64_t val;
        LocalPeek(cache, ignite::common::MakeTimeGmt(
            std::abs(static_cast<int>(9834497 * i) % 24),
            std::abs(static_cast<int>(i * 87178291199) % 60),
            std::abs(static_cast<int>(i * 39916801) % 60)),
            val);

        BOOST_REQUIRE_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsTimestamp)
{
    StartNode("node1");
    StartNode("node2");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");
    cfg.SetPartitionAwareness(true);

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<ignite::Timestamp, int64_t> cache =
        client.GetCache<ignite::Timestamp, int64_t>("partitioned");

    cache.RefreshAffinityMapping();

    for (int64_t i = 1; i < 1000; ++i)
        cache.Put(ignite::common::MakeTimestampGmt(
            static_cast<int>(1990 + i),
            std::abs(static_cast<int>(i * 87178291199) % 11 + 1),
            std::abs(static_cast<int>(i * 39916801) % 28),
            std::abs(static_cast<int>(9834497 * i) % 24),
            std::abs(static_cast<int>(i * 87178291199) % 60),
            std::abs(static_cast<int>(i * 39916801) % 60),
            std::abs(static_cast<long>((i * 303595777) % 1000000000))),
            i * 5039);

    for (int64_t i = 1; i < 1000; ++i)
    {
        int64_t val;
        LocalPeek(cache, ignite::common::MakeTimestampGmt(
            static_cast<int>(1990 + i),
            std::abs(static_cast<int>(i * 87178291199) % 11 + 1),
            std::abs(static_cast<int>(i * 39916801) % 28),
            std::abs(static_cast<int>(9834497 * i) % 24),
            std::abs(static_cast<int>(i * 87178291199) % 60),
            std::abs(static_cast<int>(i * 39916801) % 60),
            std::abs(static_cast<long>((i * 303595777) % 1000000000))),
            val);

        BOOST_REQUIRE_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientGetSizeAll)
{
    SizeTest(cache::CachePeekMode::ALL);
}

BOOST_AUTO_TEST_CASE(CacheClientGetSizePrimary)
{
    SizeTest(cache::CachePeekMode::PRIMARY);
}

BOOST_AUTO_TEST_CASE(CacheClientGetSizeOnheap)
{
    SizeTest(cache::CachePeekMode::ONHEAP);
}

BOOST_AUTO_TEST_CASE(CacheClientGetSizeSeveral)
{
    using cache::CachePeekMode;

    SizeTest(
        CachePeekMode::NEAR_CACHE |
        CachePeekMode::PRIMARY |
        CachePeekMode::BACKUP |
        CachePeekMode::ONHEAP |
        CachePeekMode::OFFHEAP
    );
}

BOOST_AUTO_TEST_CASE(CacheClientRemoveAll)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, int32_t> cache =
        client.GetCache<int32_t, int32_t>("partitioned");

    for (int32_t i = 0; i < 1000; ++i)
        cache.Put(i, i * 5039);

    int64_t size = cache.GetSize(cache::CachePeekMode::ALL);
    BOOST_CHECK_EQUAL(size, 1000);

    cache.RemoveAll();

    size = cache.GetSize(cache::CachePeekMode::ALL);
    BOOST_CHECK_EQUAL(size, 0);
}

BOOST_AUTO_TEST_CASE(CacheClientClearAll)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, int32_t> cache =
        client.GetCache<int32_t, int32_t>("partitioned");

    for (int32_t i = 0; i < 1000; ++i)
        cache.Put(i, i * 5039);

    int64_t size = cache.GetSize(cache::CachePeekMode::ALL);
    BOOST_CHECK_EQUAL(size, 1000);

    cache.Clear();

    size = cache.GetSize(cache::CachePeekMode::ALL);
    BOOST_CHECK_EQUAL(size, 0);
}

BOOST_AUTO_TEST_CASE(CacheClientRemove)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, int32_t> cache =
        client.GetCache<int32_t, int32_t>("partitioned");

    for (int32_t i = 0; i < 1000; ++i)
        cache.Put(i, i * 5039);

    int64_t size = cache.GetSize(cache::CachePeekMode::ALL);
    BOOST_CHECK_EQUAL(size, 1000);

    for (int32_t i = 0; i < 1000; ++i)
    {
        BOOST_CHECK(cache.Remove(i));

        size = cache.GetSize(cache::CachePeekMode::ALL);
        BOOST_CHECK_EQUAL(size, 1000 - i - 1);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientClear)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, int32_t> cache =
        client.GetCache<int32_t, int32_t>("partitioned");

    for (int32_t i = 0; i < 1000; ++i)
        cache.Put(i, i * 5039);

    int64_t size = cache.GetSize(cache::CachePeekMode::ALL);
    BOOST_CHECK_EQUAL(size, 1000);

    for (int32_t i = 0; i < 1000; ++i)
    {
        cache.Clear(i);

        size = cache.GetSize(cache::CachePeekMode::ALL);
        BOOST_CHECK_EQUAL(size, 1000 - i - 1);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientDefaultDynamicCache)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110..11120");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<std::string, int64_t> cache =
        client.CreateCache<std::string, int64_t>("defaultdynamic1");

    cache.RefreshAffinityMapping();

    for (int64_t i = 1; i < 1000; ++i)
        cache.Put(ignite::common::LexicalCast<std::string>(i * 39916801), i * 5039);

    for (int64_t i = 1; i < 1000; ++i)
    {
        int64_t val;
        LocalPeek(cache, ignite::common::LexicalCast<std::string>(i * 39916801), val);

        BOOST_REQUIRE_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsDefaultDynamicCacheThreeNodes)
{
    StartNode("node1");
    StartNode("node2");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");
    cfg.SetPartitionAwareness(true);

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<std::string, int64_t> cache =
        client.CreateCache<std::string, int64_t>("defaultdynamic2");

    // No-op, but should compile.
    cache.RefreshAffinityMapping();

    for (int64_t i = 1; i < 1000; ++i)
        cache.Put(ignite::common::LexicalCast<std::string>(i * 39916801), i * 5039);

    for (int64_t i = 1; i < 1000; ++i)
    {
        int64_t val;
        LocalPeek(cache, ignite::common::LexicalCast<std::string>(i * 39916801), val);

        BOOST_REQUIRE_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsRebalance)
{
    StartNode("node1");
    StartNode("node2");
    StartNode("node3");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112,127.0.0.1:11113");
    cfg.SetPartitionAwareness(true);

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<std::string, int64_t> cache =
        client.CreateCache<std::string, int64_t>("defaultdynamic3");

    for (int64_t i = 1; i < 1000; ++i)
        cache.Put(ignite::common::LexicalCast<std::string>(i * 39916801), i * 5039);

    for (int64_t i = 1; i < 1000; ++i)
    {
        int64_t val;
        LocalPeek(cache, ignite::common::LexicalCast<std::string>(i * 39916801), val);

        BOOST_REQUIRE_EQUAL(val, i * 5039);
    }

    ignite::Ignition::Stop("node3", true);

    boost::this_thread::sleep_for(boost::chrono::seconds(3));

    for (int64_t i = 1; i < 1000; ++i)
        PutWithRetry(cache, ignite::common::LexicalCast<std::string>(i * 39916801), i * 5039);

    for (int64_t i = 1; i < 1000; ++i)
    {
        int64_t val;
        LocalPeek(cache, ignite::common::LexicalCast<std::string>(i * 39916801), val);

        BOOST_REQUIRE_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsDisabledThreeNodes)
{
    StartNode("node1");
    StartNode("node2");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<std::string, int64_t> cache =
        client.CreateCache<std::string, int64_t>("defaultdynamic4");

    // No-op, but should compile.
    cache.RefreshAffinityMapping();

    for (int64_t i = 1; i < 1000; ++i)
        cache.Put(ignite::common::LexicalCast<std::string>(i * 39916801), i * 5039);

    int32_t cnt = 0;

    for (int64_t i = 1; i < 1000; ++i)
    {
        int64_t val;
        LocalPeek(cache, ignite::common::LexicalCast<std::string>(i * 39916801), val);

        if (val == i * 5039)
            ++cnt;
    }

    BOOST_REQUIRE_LT(cnt, 1000);
}

BOOST_AUTO_TEST_CASE(CacheClientGetAllContainers)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
        client.CreateCache<int32_t, std::string>("test");

    std::vector<int32_t> keys;

    keys.push_back(1);
    keys.push_back(2);
    keys.push_back(3);

    std::vector<std::string> values;

    values.push_back("first");
    values.push_back("second");
    values.push_back("third");

    for (size_t i = 0; i < keys.size(); ++i)
        cache.Put(keys[i], values[i]);

    std::map<int32_t, std::string> res;

    cache.GetAll(keys, res);

    BOOST_REQUIRE_EQUAL(res.size(), keys.size());

    for (size_t i = 0; i < keys.size(); ++i)
        BOOST_REQUIRE_EQUAL(values[i], res[keys[i]]);
}

BOOST_AUTO_TEST_CASE(CacheClientGetAllIterators)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
        client.CreateCache<int32_t, std::string>("test");

    std::vector<int32_t> keys;

    keys.push_back(1);
    keys.push_back(2);
    keys.push_back(3);

    std::vector<std::string> values;

    values.push_back("first");
    values.push_back("second");
    values.push_back("third");

    for (size_t i = 0; i < keys.size(); ++i)
        cache.Put(keys[i], values[i]);

    std::map<int32_t, std::string> res;

    cache.GetAll(keys.begin(), keys.end(), std::inserter(res, res.end()));

    BOOST_REQUIRE_EQUAL(res.size(), keys.size());

    for (size_t i = 0; i < keys.size(); ++i)
        BOOST_REQUIRE_EQUAL(values[i], res[keys[i]]);
}

BOOST_AUTO_TEST_CASE(CacheClientPutAllContainers)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
        client.CreateCache<int32_t, std::string>("test");

    std::map<int32_t, std::string> toPut;

    toPut[1] = "first";
    toPut[2] = "second";
    toPut[3] = "third";

    cache.PutAll(toPut);

    for (std::map<int32_t, std::string>::const_iterator it = toPut.begin(); it != toPut.end(); ++it)
        BOOST_REQUIRE_EQUAL(cache.Get(it->first), it->second);
}

BOOST_AUTO_TEST_CASE(CacheClientPutAllIterators)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
        client.CreateCache<int32_t, std::string>("test");

    std::map<int32_t, std::string> toPut;

    toPut[1] = "first";
    toPut[2] = "second";
    toPut[3] = "third";

    cache.PutAll(toPut.begin(), toPut.end());

    for (std::map<int32_t, std::string>::const_iterator it = toPut.begin(); it != toPut.end(); ++it)
        BOOST_REQUIRE_EQUAL(cache.Get(it->first), it->second);
}

BOOST_AUTO_TEST_CASE(CacheClientRemoveAllContainers)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
        client.CreateCache<int32_t, std::string>("test");

    std::vector<int32_t> keys;

    keys.push_back(1);
    keys.push_back(2);
    keys.push_back(3);

    std::vector<std::string> values;

    values.push_back("first");
    values.push_back("second");
    values.push_back("third");

    for (size_t i = 0; i < keys.size(); ++i)
        cache.Put(keys[i], values[i]);

    BOOST_REQUIRE_EQUAL(cache.GetSize(cache::CachePeekMode::PRIMARY), 3);

    for (size_t i = 0; i < keys.size(); ++i)
        BOOST_REQUIRE_EQUAL(cache.Get(keys[i]), values[i]);

    cache.RemoveAll(keys);

    BOOST_REQUIRE_EQUAL(cache.GetSize(cache::CachePeekMode::PRIMARY), 0);
}

BOOST_AUTO_TEST_CASE(CacheClientRemoveAllIterators)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
        client.CreateCache<int32_t, std::string>("test");

    std::vector<int32_t> keys;

    keys.push_back(1);
    keys.push_back(2);
    keys.push_back(3);

    std::vector<std::string> values;

    values.push_back("first");
    values.push_back("second");
    values.push_back("third");

    for (size_t i = 0; i < keys.size(); ++i)
        cache.Put(keys[i], values[i]);

    BOOST_REQUIRE_EQUAL(cache.GetSize(cache::CachePeekMode::PRIMARY), 3);

    for (size_t i = 0; i < keys.size(); ++i)
        BOOST_REQUIRE_EQUAL(cache.Get(keys[i]), values[i]);

    cache.RemoveAll(keys.begin(), keys.end());

    BOOST_REQUIRE_EQUAL(cache.GetSize(cache::CachePeekMode::PRIMARY), 0);
}

BOOST_AUTO_TEST_CASE(CacheClientClearAllContainers)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
        client.CreateCache<int32_t, std::string>("test");

    std::vector<int32_t> keys;

    keys.push_back(1);
    keys.push_back(2);
    keys.push_back(3);

    std::vector<std::string> values;

    values.push_back("first");
    values.push_back("second");
    values.push_back("third");

    for (size_t i = 0; i < keys.size(); ++i)
        cache.Put(keys[i], values[i]);

    BOOST_REQUIRE_EQUAL(cache.GetSize(cache::CachePeekMode::PRIMARY), 3);

    for (size_t i = 0; i < keys.size(); ++i)
        BOOST_REQUIRE_EQUAL(cache.Get(keys[i]), values[i]);

    cache.ClearAll(keys);

    BOOST_REQUIRE_EQUAL(cache.GetSize(cache::CachePeekMode::PRIMARY), 0);
}

BOOST_AUTO_TEST_CASE(CacheClientClearAllIterators)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
        client.CreateCache<int32_t, std::string>("test");

    std::vector<int32_t> keys;

    keys.push_back(1);
    keys.push_back(2);
    keys.push_back(3);

    std::vector<std::string> values;

    values.push_back("first");
    values.push_back("second");
    values.push_back("third");

    for (size_t i = 0; i < keys.size(); ++i)
        cache.Put(keys[i], values[i]);

    BOOST_REQUIRE_EQUAL(cache.GetSize(cache::CachePeekMode::PRIMARY), 3);

    for (size_t i = 0; i < keys.size(); ++i)
        BOOST_REQUIRE_EQUAL(cache.Get(keys[i]), values[i]);

    cache.ClearAll(keys.begin(), keys.end());

    BOOST_REQUIRE_EQUAL(cache.GetSize(cache::CachePeekMode::PRIMARY), 0);
}

BOOST_AUTO_TEST_CASE(CacheClientContainsKeysContainers)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
        client.CreateCache<int32_t, std::string>("test");

    std::vector<int32_t> keys;

    keys.push_back(1);
    keys.push_back(2);
    keys.push_back(3);

    std::vector<std::string> values;

    values.push_back("first");
    values.push_back("second");
    values.push_back("third");

    for (size_t i = 0; i < keys.size(); ++i)
        cache.Put(keys[i], values[i]);

    BOOST_REQUIRE_EQUAL(cache.GetSize(cache::CachePeekMode::PRIMARY), 3);

    BOOST_REQUIRE(cache.ContainsKeys(keys));

    std::set<int32_t> check;

    BOOST_REQUIRE(cache.ContainsKeys(check));

    check.insert(1);
    BOOST_REQUIRE(cache.ContainsKeys(check));

    check.insert(2);
    BOOST_REQUIRE(cache.ContainsKeys(check));

    check.insert(3);
    BOOST_REQUIRE(cache.ContainsKeys(check));

    check.insert(4);
    BOOST_REQUIRE(!cache.ContainsKeys(check));

    check.erase(2);
    BOOST_REQUIRE(!cache.ContainsKeys(check));

    check.erase(4);
    BOOST_REQUIRE(cache.ContainsKeys(check));
}

BOOST_AUTO_TEST_CASE(CacheClientContainsKeysIterators)
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache =
        client.CreateCache<int32_t, std::string>("test");

    std::vector<int32_t> keys;

    keys.push_back(1);
    keys.push_back(2);
    keys.push_back(3);

    std::vector<std::string> values;

    values.push_back("first");
    values.push_back("second");
    values.push_back("third");

    for (size_t i = 0; i < keys.size(); ++i)
        cache.Put(keys[i], values[i]);

    BOOST_REQUIRE_EQUAL(cache.GetSize(cache::CachePeekMode::PRIMARY), 3);

    BOOST_REQUIRE(cache.ContainsKeys(keys));

    std::set<int32_t> check;

    BOOST_REQUIRE(cache.ContainsKeys(check.begin(), check.end()));

    check.insert(1);
    BOOST_REQUIRE(cache.ContainsKeys(check.begin(), check.end()));

    check.insert(2);
    BOOST_REQUIRE(cache.ContainsKeys(check.begin(), check.end()));

    check.insert(3);
    BOOST_REQUIRE(cache.ContainsKeys(check.begin(), check.end()));

    check.insert(4);
    BOOST_REQUIRE(!cache.ContainsKeys(check.begin(), check.end()));

    check.erase(2);
    BOOST_REQUIRE(!cache.ContainsKeys(check.begin(), check.end()));

    check.erase(4);
    BOOST_REQUIRE(cache.ContainsKeys(check.begin(), check.end()));
}

BOOST_AUTO_TEST_CASE(CacheClientReplaceIfEqualsBasicKeyValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache = client.GetCache<int32_t, std::string>("local");

    int32_t key = 42;
    std::string valIn1 = "Lorem ipsum";
    std::string valIn2 = "Test";

    cache.Put(key, valIn1);

    BOOST_CHECK(!cache.Replace(key, valIn2, valIn2));

    std::string valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    BOOST_CHECK(cache.Replace(key, valIn1, valIn2));

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn2);
}

BOOST_AUTO_TEST_CASE(CacheClientReplaceIfEqualsComplexValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, ignite::ComplexType> cache = client.GetCache<int32_t, ignite::ComplexType>("local");

    int32_t key = 42;

    ignite::ComplexType valIn1;
    valIn1.i32Field = 123;
    valIn1.strField = "Test value";
    valIn1.objField.f1 = 42;
    valIn1.objField.f2 = "Inner value";

    ignite::ComplexType valIn2;
    valIn2.i32Field = 4234;
    valIn2.strField = "Some";
    valIn2.objField.f1 = 654;
    valIn2.objField.f2 = "Lorem";

    cache.Put(key, valIn1);

    BOOST_REQUIRE(!cache.Replace(key, valIn2, valIn2));

    ignite::ComplexType valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valIn1.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn1.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn1.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn1.objField.f2, valOut.objField.f2);

    BOOST_CHECK(cache.Replace(key, valIn1, valIn2));

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valIn2.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn2.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn2.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn2.objField.f2, valOut.objField.f2);
}

BOOST_AUTO_TEST_CASE(CacheClientReplaceIfEqualsComplexKey)
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

    int32_t valIn1 = 123;
    int32_t valIn2 = 321;

    cache.Put(key, valIn1);

    BOOST_CHECK(!cache.Replace(key, valIn2, valIn2));

    int32_t valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    BOOST_CHECK(cache.Replace(key, valIn1, valIn2));

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn2);
}

BOOST_AUTO_TEST_CASE(CacheClientRemoveIfEqualsBasicKeyValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache = client.GetCache<int32_t, std::string>("local");

    int32_t key = 42;
    std::string valIn1 = "Lorem ipsum";
    std::string valIn2 = "Test";

    cache.Put(key, valIn1);

    BOOST_REQUIRE(!cache.Remove(key, valIn2));

    BOOST_CHECK(cache.ContainsKey(key));

    std::string valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    BOOST_CHECK(cache.Remove(key, valIn1));

    BOOST_CHECK(!cache.ContainsKey(key));
}

BOOST_AUTO_TEST_CASE(CacheClientRemoveIfEqualsComplexValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, ignite::ComplexType> cache = client.GetCache<int32_t, ignite::ComplexType>("local");

    int32_t key = 42;

    ignite::ComplexType valIn1;
    valIn1.i32Field = 123;
    valIn1.strField = "Test value";
    valIn1.objField.f1 = 42;
    valIn1.objField.f2 = "Inner value";

    ignite::ComplexType valIn2;
    valIn2.i32Field = 4234;
    valIn2.strField = "Some";
    valIn2.objField.f1 = 654;
    valIn2.objField.f2 = "Lorem";

    cache.Put(key, valIn1);

    BOOST_CHECK(!cache.Remove(key, valIn2));

    BOOST_CHECK(cache.ContainsKey(key));

    ignite::ComplexType valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valIn1.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn1.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn1.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn1.objField.f2, valOut.objField.f2);

    BOOST_CHECK(cache.Remove(key, valIn1));

    BOOST_CHECK(!cache.ContainsKey(key));
}

BOOST_AUTO_TEST_CASE(CacheClientRemoveIfEqualsComplexKey)
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

    int32_t valIn1 = 123;
    int32_t valIn2 = 321;

    cache.Put(key, valIn1);

    BOOST_CHECK(!cache.Remove(key, valIn2));

    BOOST_CHECK(cache.ContainsKey(key));

    int32_t valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    BOOST_CHECK(cache.Remove(key, valIn1));

    BOOST_CHECK(!cache.ContainsKey(key));
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndPutBasicKeyValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache = client.GetCache<int32_t, std::string>("local");

    int32_t key = 42;
    std::string valIn1 = "Lorem ipsum";
    std::string valIn2 = "Test";

    cache.Put(key, valIn1);
    std::string valOut = cache.GetAndPut(key, valIn2);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn2);
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndPutComplexValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, ignite::ComplexType> cache = client.GetCache<int32_t, ignite::ComplexType>("local");

    int32_t key = 42;

    ignite::ComplexType valIn1;
    valIn1.i32Field = 123;
    valIn1.strField = "Test value";
    valIn1.objField.f1 = 42;
    valIn1.objField.f2 = "Inner value";

    ignite::ComplexType valIn2;
    valIn2.i32Field = 4234;
    valIn2.strField = "Some";
    valIn2.objField.f1 = 654;
    valIn2.objField.f2 = "Lorem";

    ignite::ComplexType valOut;

    cache.Put(key, valIn1);
    cache.GetAndPut(key, valIn2, valOut);

    BOOST_CHECK_EQUAL(valIn1.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn1.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn1.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn1.objField.f2, valOut.objField.f2);

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valIn2.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn2.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn2.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn2.objField.f2, valOut.objField.f2);
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndPutComplexKey)
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

    int32_t valIn1 = 123;
    int32_t valIn2 = 321;

    cache.Put(key, valIn1);
    int32_t valOut = cache.GetAndPut(key, valIn2);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn2);
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndRemoveBasicKeyValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache = client.GetCache<int32_t, std::string>("local");

    int32_t key = 42;
    std::string valIn = "Lorem ipsum";

    cache.Put(key, valIn);
    std::string valOut = cache.GetAndRemove(key);

    BOOST_CHECK_EQUAL(valOut, valIn);

    BOOST_CHECK(!cache.ContainsKey(key));
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndRemoveComplexValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, ignite::ComplexType> cache = client.GetCache<int32_t, ignite::ComplexType>("local");

    int32_t key = 42;

    ignite::ComplexType valIn;
    valIn.i32Field = 123;
    valIn.strField = "Test value";
    valIn.objField.f1 = 42;
    valIn.objField.f2 = "Inner value";

    ignite::ComplexType valOut;

    cache.Put(key, valIn);
    cache.GetAndRemove(key, valOut);

    BOOST_CHECK_EQUAL(valIn.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn.objField.f2, valOut.objField.f2);

    BOOST_CHECK(!cache.ContainsKey(key));
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndRemoveComplexKey)
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

    int32_t valIn = 123;

    cache.Put(key, valIn);
    int32_t valOut = cache.GetAndRemove(key);

    BOOST_CHECK_EQUAL(valOut, valIn);

    BOOST_CHECK(!cache.ContainsKey(key));
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndReplaceBasicKeyValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache = client.GetCache<int32_t, std::string>("local");

    int32_t key = 42;
    std::string valIn1 = "Lorem ipsum";
    std::string valIn2 = "Test";

    std::string valOut;
    cache.GetAndReplace(key, valIn1, valOut);

    BOOST_CHECK(valOut.empty());
    BOOST_CHECK(!cache.ContainsKey(key));

    cache.Put(key, valIn1);
    valOut = cache.GetAndReplace(key, valIn2);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn2);
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndReplaceComplexValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, ignite::ComplexType> cache = client.GetCache<int32_t, ignite::ComplexType>("local");

    int32_t key = 42;

    ignite::ComplexType valIn1;
    valIn1.i32Field = 123;
    valIn1.strField = "Test value";
    valIn1.objField.f1 = 42;
    valIn1.objField.f2 = "Inner value";

    ignite::ComplexType valIn2;
    valIn2.i32Field = 4234;
    valIn2.strField = "Some";
    valIn2.objField.f1 = 654;
    valIn2.objField.f2 = "Lorem";

    ignite::ComplexType valOut = cache.GetAndReplace(key, valIn1);

    BOOST_CHECK(!cache.ContainsKey(key));

    cache.Put(key, valIn1);
    cache.GetAndReplace(key, valIn2, valOut);

    BOOST_CHECK_EQUAL(valIn1.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn1.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn1.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn1.objField.f2, valOut.objField.f2);

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valIn2.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn2.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn2.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn2.objField.f2, valOut.objField.f2);
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndReplaceComplexKey)
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

    int32_t valIn1 = 123;
    int32_t valIn2 = 321;

    int32_t valOut;
    cache.GetAndReplace(key, valIn1, valOut);

    BOOST_CHECK_EQUAL(valOut, 0);
    BOOST_CHECK(!cache.ContainsKey(key));

    cache.Put(key, valIn1);
    valOut = cache.GetAndReplace(key, valIn2);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn2);
}

BOOST_AUTO_TEST_CASE(CacheClientPutIfAbsentBasicKeyValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache = client.GetCache<int32_t, std::string>("local");

    int32_t key = 42;
    std::string valIn1 = "Lorem ipsum";
    std::string valIn2 = "Test";

    BOOST_CHECK(cache.PutIfAbsent(key, valIn1));
    BOOST_CHECK(cache.ContainsKey(key));

    std::string valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    BOOST_CHECK(!cache.PutIfAbsent(key, valIn2));

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn1);
}

BOOST_AUTO_TEST_CASE(CacheClientPutIfAbsentComplexValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, ignite::ComplexType> cache = client.GetCache<int32_t, ignite::ComplexType>("local");

    int32_t key = 42;

    ignite::ComplexType valIn1;
    valIn1.i32Field = 123;
    valIn1.strField = "Test value";
    valIn1.objField.f1 = 42;
    valIn1.objField.f2 = "Inner value";

    ignite::ComplexType valIn2;
    valIn2.i32Field = 4234;
    valIn2.strField = "Some";
    valIn2.objField.f1 = 654;
    valIn2.objField.f2 = "Lorem";

    BOOST_CHECK(cache.PutIfAbsent(key, valIn1));
    BOOST_CHECK(cache.ContainsKey(key));

    ignite::ComplexType valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valIn1.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn1.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn1.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn1.objField.f2, valOut.objField.f2);

    BOOST_CHECK(!cache.PutIfAbsent(key, valIn2));

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valIn1.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn1.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn1.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn1.objField.f2, valOut.objField.f2);
}

BOOST_AUTO_TEST_CASE(CacheClientPutIfAbsentComplexKey)
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

    int32_t valIn1 = 123;
    int32_t valIn2 = 321;

    BOOST_CHECK(cache.PutIfAbsent(key, valIn1));
    BOOST_CHECK(cache.ContainsKey(key));

    int32_t valOut = cache.Get(key);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    BOOST_CHECK(!cache.PutIfAbsent(key, valIn2));

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn1);
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndPutIfAbsentBasicKeyValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cache = client.GetCache<int32_t, std::string>("local");

    int32_t key = 42;
    std::string valIn1 = "Lorem ipsum";
    std::string valIn2 = "Test";

    std::string valOut = cache.GetAndPutIfAbsent(key, valIn1);

    BOOST_CHECK(valOut.empty());
    BOOST_CHECK(cache.ContainsKey(key));

    cache.GetAndPutIfAbsent(key, valIn2, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn1);
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndPutIfAbsentComplexValue)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, ignite::ComplexType> cache = client.GetCache<int32_t, ignite::ComplexType>("local");

    int32_t key = 42;

    ignite::ComplexType valIn1;
    valIn1.i32Field = 123;
    valIn1.strField = "Test value";
    valIn1.objField.f1 = 42;
    valIn1.objField.f2 = "Inner value";

    ignite::ComplexType valIn2;
    valIn2.i32Field = 4234;
    valIn2.strField = "Some";
    valIn2.objField.f1 = 654;
    valIn2.objField.f2 = "Lorem";

    ignite::ComplexType valOut = cache.GetAndPutIfAbsent(key, valIn1);

    BOOST_CHECK(cache.ContainsKey(key));

    cache.GetAndPutIfAbsent(key, valIn2, valOut);

    BOOST_CHECK_EQUAL(valIn1.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn1.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn1.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn1.objField.f2, valOut.objField.f2);

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valIn1.i32Field, valOut.i32Field);
    BOOST_CHECK_EQUAL(valIn1.strField, valOut.strField);
    BOOST_CHECK_EQUAL(valIn1.objField.f1, valOut.objField.f1);
    BOOST_CHECK_EQUAL(valIn1.objField.f2, valOut.objField.f2);
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndPutIfAbsentComplexValuePtr)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, ignite::ComplexType*> cache = client.GetCache<int32_t, ignite::ComplexType*>("local");

    int32_t key = 42;

    ignite::ComplexType valIn1;
    valIn1.i32Field = 123;
    valIn1.strField = "Test value";
    valIn1.objField.f1 = 42;
    valIn1.objField.f2 = "Inner value";

    ignite::ComplexType valIn2;
    valIn2.i32Field = 4234;
    valIn2.strField = "Some";
    valIn2.objField.f1 = 654;
    valIn2.objField.f2 = "Lorem";

    ignite::ComplexType* valOut = cache.GetAndPutIfAbsent(key, &valIn1);

    BOOST_CHECK(cache.ContainsKey(key));
    BOOST_CHECK(valOut == 0);

    cache.GetAndPutIfAbsent(key, &valIn2, valOut);

    BOOST_CHECK_EQUAL(valIn1.i32Field, valOut->i32Field);
    BOOST_CHECK_EQUAL(valIn1.strField, valOut->strField);
    BOOST_CHECK_EQUAL(valIn1.objField.f1, valOut->objField.f1);
    BOOST_CHECK_EQUAL(valIn1.objField.f2, valOut->objField.f2);

    delete valOut;

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valIn1.i32Field, valOut->i32Field);
    BOOST_CHECK_EQUAL(valIn1.strField, valOut->strField);
    BOOST_CHECK_EQUAL(valIn1.objField.f1, valOut->objField.f1);
    BOOST_CHECK_EQUAL(valIn1.objField.f2, valOut->objField.f2);

    delete valOut;
}

BOOST_AUTO_TEST_CASE(CacheClientGetAndPutIfAbsentComplexKey)
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

    int32_t valIn1 = 123;
    int32_t valIn2 = 321;

    int32_t valOut = cache.GetAndPutIfAbsent(key, valIn1);

    BOOST_CHECK_EQUAL(valOut, 0);
    BOOST_CHECK(cache.ContainsKey(key));

    cache.GetAndPutIfAbsent(key, valIn2, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn1);

    cache.Get(key, valOut);

    BOOST_CHECK_EQUAL(valOut, valIn1);
}

BOOST_AUTO_TEST_SUITE_END()
