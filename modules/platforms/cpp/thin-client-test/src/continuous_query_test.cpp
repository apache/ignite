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
#include <ignite/test_type.h>

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
    enum { DEFAULT_WAIT_TIMEOUT = 1000 };

    /**
     * Default constructor.
     */
    Listener() :
        disconnected(false),
        handlingDelay(0)
    {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param handlingDelay Handling delay.
     */
    Listener(int32_t handlingDelay) :
        disconnected(false),
        handlingDelay(handlingDelay)
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
        {
            if (handlingDelay)
                boost::this_thread::sleep_for(boost::chrono::milliseconds(handlingDelay));

            eventQueue.Push(evts[i]);
        }
    }

    /**
     * Disconnected callback.
     *
     * Called if channel was disconnected. This also means that continuous query was closed and no more
     * events will be provided for this listener.
     */
    virtual void OnDisconnected()
    {
        common::concurrent::CsLockGuard guard(disconnectedMutex);

        disconnected = true;
        disconnectedCv.NotifyAll();
    }

    /**
     * Check that next received event contains specific values.
     *
     * @param key Key.
     * @param oldVal Old value.
     * @param val Current value.
     * @param eType Event type.
     */
    void CheckNextEvent(const K& key, boost::optional<V> oldVal, boost::optional<V> val, CacheEntryEventType::Type eType)
    {
        CacheEntryEvent<K, V> event;
        bool success = eventQueue.Pull(event, DEFAULT_WAIT_TIMEOUT);

        BOOST_REQUIRE(success);

        BOOST_CHECK_EQUAL(event.GetKey(), key);
        BOOST_CHECK_EQUAL(event.HasOldValue(), oldVal.is_initialized());
        BOOST_CHECK_EQUAL(event.HasValue(), val.is_initialized());
        BOOST_CHECK_EQUAL(event.GetEventType(), eType);

        if (oldVal && event.HasOldValue())
            BOOST_CHECK_EQUAL(event.GetOldValue(), *oldVal);

        if (val && event.HasValue())
            BOOST_CHECK_EQUAL(event.GetValue(), *val);
    }

    /**
     * Check that there is no nex event in specified period of time.
     *
     * @param millis Time span in milliseconds.
     */
    void CheckNoEvent(int32_t millis = DEFAULT_WAIT_TIMEOUT)
    {
        CacheEntryEvent<K, V> event;
        bool success = eventQueue.Pull(event, millis);

        BOOST_CHECK(!success);
    }

    /**
     * Check that next event is the cache entry expiry event.
     *
     * @param key Key.
     */
    void CheckExpired(const K& key)
    {
        CacheEntryEvent<K, V> event;
        bool success = eventQueue.Pull(event, DEFAULT_WAIT_TIMEOUT);

        BOOST_CHECK(success);

        BOOST_CHECK_EQUAL(event.GetKey(), key);
        BOOST_CHECK_EQUAL(event.GetEventType(), CacheEntryEventType::EXPIRED);
    }

    /**
     * Make sure that channel is disconnected within specified time.
     *
     * @param millis Time span in milliseconds.
     */
    void CheckDisconnected(int32_t millis = DEFAULT_WAIT_TIMEOUT)
    {
        common::concurrent::CsLockGuard guard(disconnectedMutex);

        if (disconnected)
            return;

        disconnectedCv.WaitFor(disconnectedMutex, millis);
        BOOST_CHECK(disconnected);
    }

private:
    /** Disconnected Mutex. */
    common::concurrent::CriticalSection disconnectedMutex;

    /** Disconnected Condition variable. */
    common::concurrent::ConditionVariable disconnectedCv;

    /** Disconnected flag. */
    bool disconnected;

    /** Handling delay. */
    int32_t handlingDelay;

    /** Events queue. */
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

    /**
     * Converting to int32_t.
     */
    operator int32_t() const
    {
        return value;
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
        cache = GetDefaultCache(client);
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
    static CacheClient<int32_t, TestEntry> GetDefaultCache(IgniteClient& client)
    {
        return client.GetOrCreateCache<int32_t, TestEntry>("transactional_no_backup");
    }

    /**
     * Get cache with configured expiry policy using client.
     *
     * @param client Client to use.
     */
    static CacheClient<int32_t, TestEntry> GetExpiryCache(IgniteClient& client)
    {
        return client.GetOrCreateCache<int32_t, TestEntry>("with_expiry");
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
    listener.CheckNextEvent(1, boost::none, TestEntry(10), CacheEntryEventType::CREATED);

    cache.Put(1, TestEntry(20));
    listener.CheckNextEvent(1, TestEntry(10), TestEntry(20), CacheEntryEventType::UPDATED);

    cache.Put(2, TestEntry(20));
    listener.CheckNextEvent(2, boost::none, TestEntry(20), CacheEntryEventType::CREATED);

    cache.Remove(1);
    listener.CheckNextEvent(1, TestEntry(20), TestEntry(20), CacheEntryEventType::REMOVED);
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

BOOST_AUTO_TEST_CASE(TestExpiredEventsReceived)
{
    cache = GetExpiryCache(client);

    Listener<int32_t, TestEntry> listener;

    ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(listener));
    qry.SetIncludeExpired(true);

    ContinuousQueryHandleClient handle = cache.QueryContinuous(qry);

    cache.Put(1, TestEntry(10));
    listener.CheckNextEvent(1, boost::none, TestEntry(10), CacheEntryEventType::CREATED);
    listener.CheckNoEvent(100);
    listener.CheckExpired(1);
}

BOOST_AUTO_TEST_CASE(TestExpiredEventsNotReceived)
{
    cache = GetExpiryCache(client);

    Listener<int32_t, TestEntry> listener;

    ContinuousQueryClient<int32_t, TestEntry> qry(MakeReference(listener));
    qry.SetIncludeExpired(false);

    ContinuousQueryHandleClient handle = cache.QueryContinuous(qry);

    cache.Put(1, TestEntry(10));
    listener.CheckNextEvent(1, boost::none, TestEntry(10), CacheEntryEventType::CREATED);
    listener.CheckNoEvent();
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

BOOST_AUTO_TEST_CASE(TestLongEventsProcessingDisconnect)
{
    boost::shared_ptr< Listener<int32_t, TestEntry> > listener(new Listener<int32_t, TestEntry>(200));

    ContinuousQueryClient<int32_t, TestEntry> qry(MakeReferenceFromSmartPointer(listener));

    ContinuousQueryHandleClient handle = cache.QueryContinuous(qry);

    for (int32_t i = 0; i < 20; ++i)
        cache.Put(i, TestEntry(i * 10));

    Ignition::Stop(node.GetName(), true);

    listener->CheckDisconnected();
}

BOOST_AUTO_TEST_CASE(TestJavaFilterFactory)
{
    CacheClient<int, std::string> cacheStr = client.GetCache<int, std::string>("transactional_no_backup");

    Listener<int, std::string> lsnr;

    ContinuousQueryClient<int, std::string> qry(MakeReference(lsnr));

    JavaCacheEntryEventFilter filter("org.apache.ignite.platform.PlatformCacheEntryEventFilterFactory");
    filter.SetProperty<std::string>("startsWith", "valid");

    qry.SetJavaFilter(filter);

    ContinuousQueryHandleClient handle = cacheStr.QueryContinuous(qry);

    cacheStr.Put(1, "notValid");
    cacheStr.Put(2, "validValue");
    cacheStr.Put(3, "alsoNotValid");
    cacheStr.Put(3, "validReplacement");

    cacheStr.Remove(2);
    cacheStr.Remove(1);
    cacheStr.Remove(3);

    lsnr.CheckNextEvent(2, boost::none, std::string("validValue"), CacheEntryEventType::CREATED);
    lsnr.CheckNextEvent(3, std::string("alsoNotValid"), std::string("validReplacement"), CacheEntryEventType::UPDATED);

    lsnr.CheckNextEvent(2, std::string("validValue"), std::string("validValue"), CacheEntryEventType::REMOVED);
    lsnr.CheckNextEvent(3, std::string("validReplacement"), std::string("validReplacement"), CacheEntryEventType::REMOVED);
}

BOOST_AUTO_TEST_CASE(TestJavaFilter)
{
    CacheClient<int, std::string> cacheStr = client.GetCache<int, std::string>("transactional_no_backup");

    Listener<int, std::string> lsnr;
    ContinuousQueryClient<int, std::string> qry(MakeReference(lsnr));

    JavaCacheEntryEventFilter filter("org.apache.ignite.platform.PlatformCacheEntryEventFilter");
    filter.SetProperty<std::string>("startsWith", "valid");
    filter.SetProperty<uint16_t>("charField", static_cast<uint16_t>('a'));
    filter.SetProperty<int8_t>("byteField", 1);
    filter.SetProperty<int16_t>("shortField", 3);
    filter.SetProperty<int32_t>("intField", 5);
    filter.SetProperty<int64_t>("longField", 7);
    filter.SetProperty<float>("floatField", 9.99f);
    filter.SetProperty<double>("doubleField", 10.123);
    filter.SetProperty<bool>("boolField", true);
    filter.SetProperty<ignite::Guid>("guidField", ignite::Guid(0x1c579241509d47c6, 0xa1a087462ae31e59));

    TestType objField;
    objField.i32Field = 1;
    objField.strField = "2";
    filter.SetProperty("objField", objField);

    std::vector<uint16_t> charArr;
    charArr.push_back(static_cast<uint16_t>('a'));
    filter.SetProperty("charArr", charArr);

    std::vector<int8_t> byteArr;
    charArr.push_back(1);
    filter.SetProperty("byteArr", byteArr);

    std::vector<int16_t> shortArr;
    charArr.push_back(3);
    filter.SetProperty("shortArr", shortArr);

    std::vector<int32_t> intArr;
    charArr.push_back(5);
    filter.SetProperty("intArr", intArr);

    std::vector<int64_t> longArr;
    charArr.push_back(7);
    filter.SetProperty("longArr", longArr);

    qry.SetJavaFilter(filter);

    ContinuousQueryHandleClient handle = cacheStr.QueryContinuous(qry);

    cacheStr.Put(1, "notValid");
    cacheStr.Put(2, "validValue");
    cacheStr.Put(3, "alsoNotValid");
    cacheStr.Put(3, "validReplacement");

    cacheStr.Remove(2);
    cacheStr.Remove(1);
    cacheStr.Remove(3);

    lsnr.CheckNextEvent(2, boost::none, std::string("validValue"), CacheEntryEventType::CREATED);
    lsnr.CheckNextEvent(3, std::string("alsoNotValid"), std::string("validReplacement"), CacheEntryEventType::UPDATED);

    lsnr.CheckNextEvent(2, std::string("validValue"), std::string("validValue"), CacheEntryEventType::REMOVED);
    lsnr.CheckNextEvent(3, std::string("validReplacement"), std::string("validReplacement"), CacheEntryEventType::REMOVED);
}

BOOST_AUTO_TEST_SUITE_END()
