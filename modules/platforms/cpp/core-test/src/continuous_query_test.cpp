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
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include "ignite/ignition.h"
#include "ignite/cache/cache.h"
#include "ignite/test_utils.h"
#include "ignite/test_type.h"

using namespace ignite;
using namespace ignite::cache;
using namespace ignite::cache::event;
using namespace ignite::cache::query;
using namespace ignite::cache::query::continuous;
using namespace boost::unit_test;

/**
 * Very simple concurrent queue implementation.
 */
template<typename T>
class ConcurrentQueue
{
public:
    /*
     * Constructor.
     */
    ConcurrentQueue()
    {
        // No-op.
    }

    /*
     * Push next element to queue.
     *
     * @param val Value to push.
     */
    void Push(const T& val)
    {
        boost::unique_lock<boost::mutex> guard(mutex);

        queue.push_back(val);

        cv.notify_one();
    }

    /*
     * Pull element from the queue with the specified timeout.
     *
     * @param val Value is placed there on success.
     * @param timeout Timeout.
     * @return True on success and false on timeout.
     */
    template <typename Rep, typename Period>
    bool Pull(T& val, const boost::chrono::duration<Rep, Period>& timeout)
    {
        boost::unique_lock<boost::mutex> guard(mutex);

        if (queue.empty())
        {
            boost::cv_status res = cv.wait_for(guard, timeout);

            if (res == boost::cv_status::timeout)
                return false;
        }

        assert(!queue.empty());

        val = queue.front();

        queue.pop_front();

        return true;
    }

private:
    boost::mutex mutex;

    boost::condition_variable cv;

    std::deque<T> queue;
};

/*
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
     * @param eType Evenet type.
     */
    void CheckNextEvent(const K& key, boost::optional<V> oldVal, boost::optional<V> val, CacheEntryEventType::T eType)
    {
        CacheEntryEvent<K, V> event;
        bool success = eventQueue.Pull(event, boost::chrono::seconds(1));

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

/**
 * Only lets through keys from the range.
 */
template<typename K, typename V>
struct RangeFilter : CacheEntryEventFilter<K, V>
{
    /**
     * Default constructor.
     */
    RangeFilter() :
        rangeBegin(0),
        rangeEnd(0)
    {
        // No-op.
    }

    /**
     * Constructor.
     *
     * @param from Range beginning. Inclusive.
     * @param to Range end. Not inclusive.
     */
    RangeFilter(const K& from, const K& to) :
        rangeBegin(from),
        rangeEnd(to)
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    virtual ~RangeFilter()
    {
        // No-op.
    }

    /**
     * Event callback.
     *
     * @param event Event.
     * @return True if the event passes filter.
     */
    virtual bool Process(const CacheEntryEvent<K, V>& event)
    {
        return event.GetKey() >= rangeBegin && event.GetKey() < rangeEnd;
    }

    /** Beginning of the range. */
    K rangeBegin;

    /** End of the range. */
    K rangeEnd;
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

        template<typename K, typename V>
        struct BinaryType< RangeFilter<K,V> > : BinaryTypeDefaultAll< RangeFilter<K,V> >
        {
            static void GetTypeName(std::string& dst)
            {
                dst = "RangeFilter";
            }

            static void Write(BinaryWriter& writer, const RangeFilter<K,V>& obj)
            {
                writer.WriteObject("rangeBegin", obj.rangeBegin);
                writer.WriteObject("rangeEnd", obj.rangeEnd);
            }

            static void Read(BinaryReader& reader, RangeFilter<K, V>& dst)
            {
                dst.rangeBegin = reader.ReadObject<K>("rangeBegin");
                dst.rangeEnd = reader.ReadObject<K>("rangeEnd");
            }
        };
    }
}

/*
 * Test setup fixture.
 */
struct ContinuousQueryTestSuiteFixture
{
    Ignite node;

    Cache<int, TestEntry> cache;

    /*
     * Constructor.
     */
    ContinuousQueryTestSuiteFixture() :
#ifdef IGNITE_TESTS_32
        node(ignite_test::StartNode("cache-query-continuous-32.xml", "node-01")),
#else
        node(ignite_test::StartNode("cache-query-continuous.xml", "node-01")),
#endif
        cache(node.GetCache<int, TestEntry>("transactional_no_backup"))
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~ContinuousQueryTestSuiteFixture()
    {
        Ignition::StopAll(false);

        node = Ignite();
    }
};

void CheckEvents(Cache<int, TestEntry>& cache, Listener<int, TestEntry>& lsnr)
{
    cache.Put(1, TestEntry(10));
    lsnr.CheckNextEvent(1, boost::none, TestEntry(10), CacheEntryEventType::CREATE);

    cache.Put(1, TestEntry(20));
    lsnr.CheckNextEvent(1, TestEntry(10), TestEntry(20), CacheEntryEventType::UPDATE);

    cache.Put(2, TestEntry(20));
    lsnr.CheckNextEvent(2, boost::none, TestEntry(20), CacheEntryEventType::CREATE);

    cache.Remove(1);
    lsnr.CheckNextEvent(1, TestEntry(20), TestEntry(20), CacheEntryEventType::REMOVE);
}

IGNITE_EXPORTED_CALL void IgniteModuleInit0(ignite::IgniteBindingContext& context)
{
    IgniteBinding binding = context.GetBinding();

    binding.RegisterCacheEntryEventFilter< RangeFilter<int, TestEntry> >();
}

BOOST_FIXTURE_TEST_SUITE(ContinuousQueryTestSuite, ContinuousQueryTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestBasic)
{
    Listener<int, TestEntry> lsnr;

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestInitialQueryScan)
{
    Listener<int, TestEntry> lsnr;

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

    cache.Put(11, TestEntry(111));
    cache.Put(22, TestEntry(222));
    cache.Put(33, TestEntry(333));

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry, ScanQuery());

    std::vector< CacheEntry<int, TestEntry> > vals;

    handle.GetInitialQueryCursor().GetAll(vals);

    BOOST_CHECK_THROW(handle.GetInitialQueryCursor(), IgniteError);

    BOOST_REQUIRE_EQUAL(vals.size(), 3);

    BOOST_CHECK_EQUAL(vals[0].GetKey(), 11);
    BOOST_CHECK_EQUAL(vals[1].GetKey(), 22);
    BOOST_CHECK_EQUAL(vals[2].GetKey(), 33);

    BOOST_CHECK_EQUAL(vals[0].GetValue().value, 111);
    BOOST_CHECK_EQUAL(vals[1].GetValue().value, 222);
    BOOST_CHECK_EQUAL(vals[2].GetValue().value, 333);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestInitialQuerySql)
{
    Listener<int, TestEntry> lsnr;

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

    cache.Put(11, TestEntry(111));
    cache.Put(22, TestEntry(222));
    cache.Put(33, TestEntry(333));

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry, SqlQuery("TestEntry", "value > 200"));

    std::vector< CacheEntry<int, TestEntry> > vals;

    handle.GetInitialQueryCursor().GetAll(vals);

    BOOST_CHECK_THROW(handle.GetInitialQueryCursor(), IgniteError);

    BOOST_REQUIRE_EQUAL(vals.size(), 2);

    BOOST_CHECK_EQUAL(vals[0].GetKey(), 22);
    BOOST_CHECK_EQUAL(vals[1].GetKey(), 33);

    BOOST_CHECK_EQUAL(vals[0].GetValue().value, 222);
    BOOST_CHECK_EQUAL(vals[1].GetValue().value, 333);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestInitialQueryText)
{
    Listener<int, TestEntry> lsnr;

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

    cache.Put(11, TestEntry(111));
    cache.Put(22, TestEntry(222));
    cache.Put(33, TestEntry(333));

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry, TextQuery("TestEntry", "222"));

    std::vector< CacheEntry<int, TestEntry> > vals;

    handle.GetInitialQueryCursor().GetAll(vals);

    BOOST_CHECK_THROW(handle.GetInitialQueryCursor(), IgniteError);

    BOOST_REQUIRE_EQUAL(vals.size(), 1);

    BOOST_CHECK_EQUAL(vals[0].GetKey(), 22);

    BOOST_CHECK_EQUAL(vals[0].GetValue().value, 222);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestBasicNoExcept)
{
    Listener<int, TestEntry> lsnr;

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

    IgniteError err;

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry, err);

    BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_SUCCESS);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestInitialQueryScanNoExcept)
{
    Listener<int, TestEntry> lsnr;

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

    cache.Put(11, TestEntry(111));
    cache.Put(22, TestEntry(222));
    cache.Put(33, TestEntry(333));

    IgniteError err;

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry, ScanQuery(), err);

    BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_SUCCESS);

    std::vector< CacheEntry<int, TestEntry> > vals;

    handle.GetInitialQueryCursor().GetAll(vals);

    BOOST_CHECK_THROW(handle.GetInitialQueryCursor(), IgniteError);

    BOOST_REQUIRE_EQUAL(vals.size(), 3);

    BOOST_CHECK_EQUAL(vals[0].GetKey(), 11);
    BOOST_CHECK_EQUAL(vals[1].GetKey(), 22);
    BOOST_CHECK_EQUAL(vals[2].GetKey(), 33);

    BOOST_CHECK_EQUAL(vals[0].GetValue().value, 111);
    BOOST_CHECK_EQUAL(vals[1].GetValue().value, 222);
    BOOST_CHECK_EQUAL(vals[2].GetValue().value, 333);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestInitialQuerySqlNoExcept)
{
    Listener<int, TestEntry> lsnr;

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

    cache.Put(11, TestEntry(111));
    cache.Put(22, TestEntry(222));
    cache.Put(33, TestEntry(333));

    IgniteError err;

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry, SqlQuery("TestEntry", "value > 200"), err);

    BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_SUCCESS);

    std::vector< CacheEntry<int, TestEntry> > vals;

    handle.GetInitialQueryCursor().GetAll(vals);

    BOOST_CHECK_THROW(handle.GetInitialQueryCursor(), IgniteError);

    BOOST_REQUIRE_EQUAL(vals.size(), 2);

    BOOST_CHECK_EQUAL(vals[0].GetKey(), 22);
    BOOST_CHECK_EQUAL(vals[1].GetKey(), 33);

    BOOST_CHECK_EQUAL(vals[0].GetValue().value, 222);
    BOOST_CHECK_EQUAL(vals[1].GetValue().value, 333);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestInitialQueryTextNoExcept)
{
    Listener<int, TestEntry> lsnr;

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

    cache.Put(11, TestEntry(111));
    cache.Put(22, TestEntry(222));
    cache.Put(33, TestEntry(333));

    IgniteError err;

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry, TextQuery("TestEntry", "222"), err);

    BOOST_REQUIRE(err.GetCode() == IgniteError::IGNITE_SUCCESS);

    std::vector< CacheEntry<int, TestEntry> > vals;

    handle.GetInitialQueryCursor().GetAll(vals);

    BOOST_CHECK_THROW(handle.GetInitialQueryCursor(), IgniteError);

    BOOST_REQUIRE_EQUAL(vals.size(), 1);

    BOOST_CHECK_EQUAL(vals[0].GetKey(), 22);

    BOOST_CHECK_EQUAL(vals[0].GetValue().value, 222);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestExpiredQuery)
{
    Listener<int, TestEntry> lsnr;
    ContinuousQueryHandle<int, TestEntry> handle;

    {
        // Query scope.
        ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

        handle = cache.QueryContinuous(qry);
    }

    // Query is destroyed here.

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestSetGetLocal)
{
    Listener<int, TestEntry> lsnr;

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

    BOOST_CHECK(!qry.GetLocal());

    qry.SetLocal(true);

    BOOST_CHECK(qry.GetLocal());

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry);

    BOOST_CHECK(qry.GetLocal());

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestGetSetBufferSize)
{
    typedef ContinuousQuery<int, TestEntry> QueryType;
    Listener<int, TestEntry> lsnr;

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

    BOOST_CHECK_EQUAL(qry.GetBufferSize(), (int32_t) QueryType::DEFAULT_BUFFER_SIZE);

    qry.SetBufferSize(2 * QueryType::DEFAULT_BUFFER_SIZE);

    BOOST_CHECK_EQUAL(qry.GetBufferSize(), 2 * QueryType::DEFAULT_BUFFER_SIZE);

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry);

    BOOST_CHECK_EQUAL(qry.GetBufferSize(), 2 * QueryType::DEFAULT_BUFFER_SIZE);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestGetSetTimeInterval)
{
    typedef ContinuousQuery<int, TestEntry> QueryType;
    Listener<int, TestEntry> lsnr;

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr));

    qry.SetBufferSize(10);

    BOOST_CHECK_EQUAL(qry.GetTimeInterval(), static_cast<int>(QueryType::DEFAULT_TIME_INTERVAL));

    qry.SetTimeInterval(500);

    BOOST_CHECK_EQUAL(qry.GetTimeInterval(), 500);

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry);

    BOOST_CHECK_EQUAL(qry.GetTimeInterval(), 500);

    CheckEvents(cache, lsnr);
}

BOOST_AUTO_TEST_CASE(TestPublicPrivateConstantsConsistence)
{
    typedef ContinuousQuery<int, TestEntry> QueryType;
    typedef impl::cache::query::continuous::ContinuousQueryImpl<int, TestEntry> QueryImplType;
    
    BOOST_CHECK_EQUAL(static_cast<int>(QueryImplType::DEFAULT_TIME_INTERVAL),
        static_cast<int>(QueryType::DEFAULT_TIME_INTERVAL));

    BOOST_CHECK_EQUAL(static_cast<int>(QueryImplType::DEFAULT_BUFFER_SIZE),
        static_cast<int>(QueryType::DEFAULT_BUFFER_SIZE));
}

BOOST_AUTO_TEST_CASE(TestFilterSingleNode)
{
    Listener<int, TestEntry> lsnr;
    RangeFilter<int, TestEntry> filter(100, 150);

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr), MakeReference(filter));

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry);

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

    lsnr.CheckNextEvent(100, boost::none, TestEntry(1000), CacheEntryEventType::CREATE);
    lsnr.CheckNextEvent(101, boost::none, TestEntry(1010), CacheEntryEventType::CREATE);

    lsnr.CheckNextEvent(142, boost::none, TestEntry(1420), CacheEntryEventType::CREATE);
    lsnr.CheckNextEvent(142, TestEntry(1420), TestEntry(1421), CacheEntryEventType::UPDATE);
    lsnr.CheckNextEvent(142, TestEntry(1421), TestEntry(1421), CacheEntryEventType::REMOVE);

    lsnr.CheckNextEvent(149, boost::none, TestEntry(1490), CacheEntryEventType::CREATE);
}

BOOST_AUTO_TEST_CASE(TestFilterMultipleNodes)
{
#ifdef IGNITE_TESTS_32
    Ignite node2 = ignite_test::StartNode("cache-query-continuous-32.xml", "node-02");
    Ignite node3 = ignite_test::StartNode("cache-query-continuous-32.xml", "node-03");
#else
    Ignite node2 = ignite_test::StartNode("cache-query-continuous.xml", "node-02");
    Ignite node3 = ignite_test::StartNode("cache-query-continuous.xml", "node-03");
#endif

    Listener<int, TestEntry> lsnr;
    RangeFilter<int, TestEntry> filter(100, 150);

    ContinuousQuery<int, TestEntry> qry(MakeReference(lsnr), MakeReference(filter));

    ContinuousQueryHandle<int, TestEntry> handle = cache.QueryContinuous(qry);

    Cache<int, TestEntry> cache2 = node2.GetCache<int, TestEntry>("transactional_no_backup");

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

    lsnr.CheckNextEvent(100, boost::none, TestEntry(1000), CacheEntryEventType::CREATE);
    lsnr.CheckNextEvent(101, boost::none, TestEntry(1010), CacheEntryEventType::CREATE);

    lsnr.CheckNextEvent(142, boost::none, TestEntry(1420), CacheEntryEventType::CREATE);
    lsnr.CheckNextEvent(142, TestEntry(1420), TestEntry(1421), CacheEntryEventType::UPDATE);
    lsnr.CheckNextEvent(142, TestEntry(1421), TestEntry(1421), CacheEntryEventType::REMOVE);

    lsnr.CheckNextEvent(149, boost::none, TestEntry(1490), CacheEntryEventType::CREATE);
}

BOOST_AUTO_TEST_CASE(TestJavaFilterFactory)
{
    Cache<int, std::string> cacheStr = node.GetCache<int, std::string>("transactional_no_backup");

    Listener<int, std::string> lsnr;

    JavaCacheEntryEventFilter filter("org.apache.ignite.platform.PlatformCacheEntryEventFilterFactory");
    filter.SetProperty<std::string>("startsWith", "valid");

    ContinuousQuery<int, std::string> qry(MakeReference(lsnr), filter);
    ContinuousQueryHandle<int, std::string> handle = cacheStr.QueryContinuous(qry);

    cacheStr.Put(1, "notValid");
    cacheStr.Put(2, "validValue");
    cacheStr.Put(3, "alsoNotValid");
    cacheStr.Put(3, "validReplacement");

    cacheStr.Remove(2);
    cacheStr.Remove(1);
    cacheStr.Remove(3);

    lsnr.CheckNextEvent(2, boost::none, std::string("validValue"), CacheEntryEventType::CREATE);
    lsnr.CheckNextEvent(3, std::string("alsoNotValid"), std::string("validReplacement"), CacheEntryEventType::UPDATE);

    lsnr.CheckNextEvent(2, std::string("validValue"), std::string("validValue"), CacheEntryEventType::REMOVE);
    lsnr.CheckNextEvent(3, std::string("validReplacement"), std::string("validReplacement"), CacheEntryEventType::REMOVE);
}

BOOST_AUTO_TEST_CASE(TestJavaFilter)
{
    Cache<int, std::string> cacheStr = node.GetCache<int, std::string>("transactional_no_backup");

    Listener<int, std::string> lsnr;

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

    ContinuousQuery<int, std::string> qry(MakeReference(lsnr), filter);
    ContinuousQueryHandle<int, std::string> handle = cacheStr.QueryContinuous(qry);

    cacheStr.Put(1, "notValid");
    cacheStr.Put(2, "validValue");
    cacheStr.Put(3, "alsoNotValid");
    cacheStr.Put(3, "validReplacement");

    cacheStr.Remove(2);
    cacheStr.Remove(1);
    cacheStr.Remove(3);

    lsnr.CheckNextEvent(2, boost::none, std::string("validValue"), CacheEntryEventType::CREATE);
    lsnr.CheckNextEvent(3, std::string("alsoNotValid"), std::string("validReplacement"), CacheEntryEventType::UPDATE);

    lsnr.CheckNextEvent(2, std::string("validValue"), std::string("validValue"), CacheEntryEventType::REMOVE);
    lsnr.CheckNextEvent(3, std::string("validReplacement"), std::string("validReplacement"), CacheEntryEventType::REMOVE);
}

BOOST_AUTO_TEST_SUITE_END()
