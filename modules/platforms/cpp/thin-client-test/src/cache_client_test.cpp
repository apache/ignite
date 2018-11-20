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

        boost::this_thread::sleep_for(boost::chrono::seconds(2));

        IgniteClientConfiguration cfg;
        cfg.SetEndPoints("127.0.0.1:11110..11120");

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

            BOOST_CHECK_EQUAL(val, i * 5039);
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

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110..11120");

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

        BOOST_CHECK_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsGuid)
{
    StartNode("node1");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110..11120");

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

        BOOST_CHECK_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsComplexType)
{
    StartNode("node1");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110..11120");

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

        BOOST_CHECK_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsDate)
{
    StartNode("node1");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110..11120");

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

        BOOST_CHECK_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsTime)
{
    StartNode("node1");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110..11120");

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

        BOOST_CHECK_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_CASE(CacheClientPartitionsTimestamp)
{
    StartNode("node1");

    boost::this_thread::sleep_for(boost::chrono::seconds(2));

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110..11120");

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

        BOOST_CHECK_EQUAL(val, i * 5039);
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
        client.CreateCache<std::string, int64_t>("defaultdynamic");

    cache.RefreshAffinityMapping();

    for (int64_t i = 1; i < 1000; ++i)
        cache.Put(ignite::common::LexicalCast<std::string>(i * 39916801), i * 5039);

    for (int64_t i = 1; i < 1000; ++i)
    {
        int64_t val;
        LocalPeek(cache, ignite::common::LexicalCast<std::string>(i * 39916801), val);

        BOOST_CHECK_EQUAL(val, i * 5039);
    }
}

BOOST_AUTO_TEST_SUITE_END()
