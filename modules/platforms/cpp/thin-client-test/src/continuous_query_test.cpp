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
    explicit TestEntry(int32_t val) : value(val)
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
    static IgniteClient StartClient()
    {
        IgniteClientConfiguration cfg;
        cfg.SetEndPoints("127.0.0.1:11110");

        return IgniteClient::Start(cfg);
    }

    /**
     * Get test cache using client.
     *
     * @param client Client to use.
     */
    static CacheClient<int32_t, TestEntry> GetTestCache(IgniteClient& client)
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

void CheckEvents(CacheClient<int32_t, TestEntry>& cache, Listener<int32_t, TestEntry>& listener)
{
    cache.Put(1, TestEntry(10));
    listener.CheckNextEvent(1, boost::none, TestEntry(10));

    cache.Put(1, TestEntry(20));
    listener.CheckNextEvent(1, TestEntry(10), TestEntry(20));

    cache.Put(2, TestEntry(20));
    listener.CheckNextEvent(2, boost::none, TestEntry(20));

    cache.Remove(1);
    listener.CheckNextEvent(1, TestEntry(20), TestEntry(20));
}

BOOST_FIXTURE_TEST_SUITE(ContinuousQueryTestSuite, ContinuousQueryTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestBasic)
{
    Listener<int32_t, TestEntry> listener;

    ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(listener));

    ContinuousQueryHandleClient handle = cache.QueryContinuous(qry);

    CheckEvents(cache, listener);
}

BOOST_AUTO_TEST_CASE(TestExpiredQuery)
{
    Listener<int32_t, TestEntry> listener;
    ContinuousQueryHandleClient handle;

    {
        // Query scope.
        ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(listener));

        handle = cache.QueryContinuous(qry);
    }

    // Query is destroyed here.

    CheckEvents(cache, listener);
}

BOOST_AUTO_TEST_CASE(TestGetSetBufferSize)
{
    typedef ContinuousQueryClient<int32_t, TestEntry> QueryType;
    Listener<int32_t, TestEntry> listener;

    ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(listener));

    BOOST_CHECK_EQUAL(qry.GetBufferSize(), (int32_t) QueryType::DEFAULT_BUFFER_SIZE);

    qry.SetBufferSize(2 * QueryType::DEFAULT_BUFFER_SIZE);

    BOOST_CHECK_EQUAL(qry.GetBufferSize(), 2 * QueryType::DEFAULT_BUFFER_SIZE);

    ContinuousQueryHandleClient handle = cache.QueryContinuous(qry);

    BOOST_CHECK_EQUAL(qry.GetBufferSize(), 2 * QueryType::DEFAULT_BUFFER_SIZE);

    CheckEvents(cache, listener);
}

BOOST_AUTO_TEST_CASE(TestGetSetTimeInterval)
{
    typedef ContinuousQueryClient<int32_t, TestEntry> QueryType;
    Listener<int32_t, TestEntry> listener;

    ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(listener));

    qry.SetBufferSize(10);

    BOOST_CHECK_EQUAL(qry.GetTimeInterval(), static_cast<int>(QueryType::DEFAULT_TIME_INTERVAL));

    qry.SetTimeInterval(500);

    BOOST_CHECK_EQUAL(qry.GetTimeInterval(), 500);

    ContinuousQueryHandleClient handle = cache.QueryContinuous(qry);

    BOOST_CHECK_EQUAL(qry.GetTimeInterval(), 500);

    CheckEvents(cache, listener);
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

BOOST_AUTO_TEST_SUITE_END()
