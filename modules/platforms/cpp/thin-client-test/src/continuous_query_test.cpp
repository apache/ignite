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

#include <deque>

#include <boost/test/unit_test.hpp>
#include <boost/optional.hpp>

#include <ignite/common/concurrent.h>

#include <ignite/ignition.h>
#include <ignite/thin/ignite_client.h>

#include <test_utils.h>

using namespace ignite;
using namespace ignite::thin;
using namespace ignite::thin::cache;
using namespace ignite::thin::cache::event;
using namespace ignite::thin::cache::query;
using namespace ignite::thin::cache::query::continuous;

using namespace boost::unit_test;


/**
 * Very simple concurrent queue implementation.
 */
template<typename T>
class ConcurrentQueue
{
public:
    /**
     * Constructor.
     */
    ConcurrentQueue()
    {
        // No-op.
    }

    /**
     * Push next element to queue.
     *
     * @param val Value to push.
     */
    void Push(const T& val)
    {
        common::concurrent::CsLockGuard guard(mutex);

        queue.push_back(val);

        cv.NotifyOne();
    }

    /**
     * Pull element from the queue with the specified timeout.
     *
     * @param val Value is placed there on success.
     * @param timeout Timeout in ms.
     * @return True on success and false on timeout.
     */
    bool Pull(T& val, int32_t timeout)
    {
        common::concurrent::CsLockGuard guard(mutex);

        if (queue.empty())
        {
            bool notified = cv.WaitFor(mutex, timeout);

            if (!notified)
                return false;
        }

        assert(!queue.empty());

        val = queue.front();

        queue.pop_front();

        return true;
    }

private:
    /** Mutex. */
    common::concurrent::CriticalSection mutex;

    /** Condition variable. */
    common::concurrent::ConditionVariable cv;

    /** Queue. */
    std::deque<T> queue;
};

/**
 * Test listener class. Stores events it has been notified about in concurrent
 * queue so they can be checked later.
 */
template<typename K, typename V>
class Listener : public CacheEntryEventListener<K, V>
{
public:
    /*
     * Default constructor.
     */
    Listener()
    {
        // No-op.
    }

    /**
     * Event callback.
     *
     * @param evts Events.
     * @param num Events number.
     */
    virtual void OnEvent(const CacheEntryEvent<K, V>* evts, uint32_t num)
    {
        for (uint32_t i = 0; i < num; ++i)
            eventQueue.Push(evts[i]);
    }

    /*
     * Check that next received event contains specific values.
     *
     * @param key Key.
     * @param oldVal Old value.
     * @param val Current value.
     */
    void CheckNextEvent(const K& key, boost::optional<V> oldVal, boost::optional<V> val)
    {
        CacheEntryEvent<K, V> event;
        bool success = eventQueue.Pull(event, 1000);

        BOOST_REQUIRE(success);

        BOOST_CHECK_EQUAL(event.GetKey(), key);
        BOOST_CHECK_EQUAL(event.HasOldValue(), oldVal.is_initialized());
        BOOST_CHECK_EQUAL(event.HasValue(), val.is_initialized());

        if (oldVal && event.HasOldValue())
            BOOST_CHECK_EQUAL(event.GetOldValue().value, oldVal->value);

        if (val && event.HasValue())
            BOOST_CHECK_EQUAL(event.GetValue().value, val->value);
    }

    /*
     * Check that there is no event for the specified ammount of time.
     *
     * @param timeout Timeout.
     */
    template <typename Rep, typename Period>
    void CheckNoEvent(const boost::chrono::duration<Rep, Period>& timeout)
    {
        CacheEntryEvent<K, V> event;
        bool success = eventQueue.Pull(event, timeout);

        BOOST_REQUIRE(!success);
    }

private:
    // Events queue.
    ConcurrentQueue< CacheEntryEvent<K, V> > eventQueue;
};

/*
 * Test entry.
 */
struct TestEntry
{
    /*
     * Default constructor.
     */
    TestEntry() : value(0)
    {
        // No-op.
    }

    /*
     * Constructor.
     */
    TestEntry(int32_t val) : value(val)
    {
        // No-op.
    }

    /* Value */
    int32_t value;
};

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<TestEntry> : BinaryTypeDefaultAll<TestEntry>
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "TestEntry";
            }

            static void Write(BinaryWriter& writer, const TestEntry& obj)
            {
                writer.WriteInt32("value", obj.value);
            }

            static void Read(BinaryReader& reader, TestEntry& dst)
            {
                dst.value = reader.ReadInt32("value");
            }
        };
    }
}

/**
 * Test setup fixture.
 */
class ContinuousQueryTestSuiteFixture
{
public:
    /**
     * Constructor.
     */
    ContinuousQueryTestSuiteFixture() :
        node(ignite_test::StartCrossPlatformServerNode("cache-query-continuous.xml", "node-01")),
        client(),
        cache()
    {
        client = StartClient();
        cache = GetTestCache(client);
    }

    /**
     * Destructor.
     */
    ~ContinuousQueryTestSuiteFixture()
    {
        Ignition::StopAll(false);

        node = Ignite();
    }

    /**
     * Start new client.
     */
    IgniteClient StartClient()
    {
        IgniteClientConfiguration cfg;
        cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111,127.0.0.1:11112");

        return IgniteClient::Start(cfg);
    }

    /**
     * Get test cache using client.
     *
     * @param client Client to use.
     */
    CacheClient<int32_t, TestEntry> GetTestCache(IgniteClient& client)
    {
        return client.GetOrCreateCache<int32_t, TestEntry>("ContinuousQueryTestSuite");
    }

protected:
    /** Node. */
    Ignite node;

    /** Client. */
    IgniteClient client;

    /** Cache client. */
    CacheClient<int32_t, TestEntry> cache;
};

void CheckEvents(CacheClient<int32_t, TestEntry>& cache, Listener<int32_t, TestEntry>& lsnr)
{
    cache.Put(1, TestEntry(10));
    lsnr.CheckNextEvent(1, boost::none, TestEntry(10));

    cache.Put(1, TestEntry(20));
    lsnr.CheckNextEvent(1, TestEntry(10), TestEntry(20));

    cache.Put(2, TestEntry(20));
    lsnr.CheckNextEvent(2, boost::none, TestEntry(20));

    cache.Remove(1);
    lsnr.CheckNextEvent(1, TestEntry(20), TestEntry(20));
}

BOOST_FIXTURE_TEST_SUITE(ContinuousQueryTestSuite, ContinuousQueryTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestBasic)
{
    Listener<int32_t, TestEntry> lsnr;

    ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(lsnr));

    ContinuousQueryHandleClient handle = cache.QueryContinuous(qry);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestExpiredQuery)
{
    Listener<int32_t, TestEntry> lsnr;
    ContinuousQueryHandleClient handle;

    {
        // Query scope.
        ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(lsnr));

        handle = cache.QueryContinuous(qry);
    }

    // Query is destroyed here.

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestGetSetBufferSize)
{
    typedef ContinuousQueryClient<int32_t, TestEntry> QueryType;
    Listener<int32_t, TestEntry> lsnr;

    ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(lsnr));

    BOOST_CHECK_EQUAL(qry.GetBufferSize(), (int32_t) QueryType::DEFAULT_BUFFER_SIZE);

    qry.SetBufferSize(2 * QueryType::DEFAULT_BUFFER_SIZE);

    BOOST_CHECK_EQUAL(qry.GetBufferSize(), 2 * QueryType::DEFAULT_BUFFER_SIZE);

    ContinuousQueryHandleClient handle = cache.QueryContinuous(qry);

    BOOST_CHECK_EQUAL(qry.GetBufferSize(), 2 * QueryType::DEFAULT_BUFFER_SIZE);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestGetSetTimeInterval)
{
    typedef ContinuousQueryClient<int32_t, TestEntry> QueryType;
    Listener<int32_t, TestEntry> lsnr;

    ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(lsnr));

    qry.SetBufferSize(10);

    BOOST_CHECK_EQUAL(qry.GetTimeInterval(), static_cast<int>(QueryType::DEFAULT_TIME_INTERVAL));

    qry.SetTimeInterval(500);

    BOOST_CHECK_EQUAL(qry.GetTimeInterval(), 500);

    ContinuousQueryHandleClient handle = cache.QueryContinuous(qry);

    BOOST_CHECK_EQUAL(qry.GetTimeInterval(), 500);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestPublicPrivateConstantsConsistence)
{
    typedef ContinuousQueryClient<int32_t, TestEntry> QueryType;
    typedef impl::cache::query::continuous::ContinuousQueryImpl<int, TestEntry> QueryImplType;
    
    BOOST_CHECK_EQUAL(static_cast<int>(QueryImplType::DEFAULT_TIME_INTERVAL),
        static_cast<int>(QueryType::DEFAULT_TIME_INTERVAL));

    BOOST_CHECK_EQUAL(static_cast<int>(QueryImplType::DEFAULT_BUFFER_SIZE),
        static_cast<int>(QueryType::DEFAULT_BUFFER_SIZE));
}

BOOST_AUTO_TEST_CASE(TestFilterSingleNode)
{
    Listener<int32_t, TestEntry> lsnr;
//    RangeFilter<int, TestEntry> filter(100, 150);

    ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(lsnr));

    ContinuousQueryHandleClient handle = cache.QueryContinuous(qry);

    cache.Put(1, TestEntry(10));
    cache.Put(1, TestEntry(11));

    cache.Put(2, TestEntry(20));
    cache.Remove(2);

    cache.Put(100, TestEntry(1000));
    cache.Put(101, TestEntry(1010));

    cache.Put(142, TestEntry(1420));
    cache.Put(142, TestEntry(1421));
    cache.Remove(142);

    cache.Put(149, TestEntry(1490));
    cache.Put(150, TestEntry(1500));
    cache.Put(150, TestEntry(1502));
    cache.Remove(150);

    lsnr.CheckNextEvent(100, boost::none, TestEntry(1000));
    lsnr.CheckNextEvent(101, boost::none, TestEntry(1010));

    lsnr.CheckNextEvent(142, boost::none, TestEntry(1420));
    lsnr.CheckNextEvent(142, TestEntry(1420), TestEntry(1421));
    lsnr.CheckNextEvent(142, TestEntry(1421), TestEntry(1421));

    lsnr.CheckNextEvent(149, boost::none, TestEntry(1490));
}

BOOST_AUTO_TEST_CASE(TestFilterMultipleNodes)
{
    Ignite node2 = ignite_test::StartCrossPlatformServerNode("cache-query-continuous.xml", "node-02");
    Ignite node3 = ignite_test::StartCrossPlatformServerNode("cache-query-continuous.xml", "node-03");

    Listener<int32_t, TestEntry> lsnr;
//    RangeFilter<int, TestEntry> filter(100, 150);

    ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(lsnr));

    ContinuousQueryHandleClient handle = cache.QueryContinuous(qry);

    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client2 = StartClient();
    CacheClient<int32_t, TestEntry> cache2 = GetTestCache(client2);

    cache2.Put(1, TestEntry(10));
    cache2.Put(1, TestEntry(11));

    cache2.Put(2, TestEntry(20));
    cache2.Remove(2);

    cache2.Put(100, TestEntry(1000));
    cache2.Put(101, TestEntry(1010));

    cache2.Put(142, TestEntry(1420));
    cache2.Put(142, TestEntry(1421));
    cache2.Remove(142);

    cache2.Put(149, TestEntry(1490));
    cache2.Put(150, TestEntry(1500));
    cache2.Put(150, TestEntry(1502));
    cache2.Remove(150);

    for (int i = 200; i < 250; ++i)
        cache2.Put(i, TestEntry(i * 10));

    lsnr.CheckNextEvent(100, boost::none, TestEntry(1000));
    lsnr.CheckNextEvent(101, boost::none, TestEntry(1010));

    lsnr.CheckNextEvent(142, boost::none, TestEntry(1420));
    lsnr.CheckNextEvent(142, TestEntry(1420), TestEntry(1421));
    lsnr.CheckNextEvent(142, TestEntry(1421), TestEntry(1421));

    lsnr.CheckNextEvent(149, boost::none, TestEntry(1490));
}

BOOST_AUTO_TEST_SUITE_END()
