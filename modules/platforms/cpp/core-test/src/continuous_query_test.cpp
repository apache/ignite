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

#include <deque>

#include <boost/test/unit_test.hpp>
#include <boost/optional.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/condition_variable.hpp>

#include "ignite/ignition.h"
#include "ignite/cache/cache.h"
#include "ignite/test_utils.h"

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
     */
    void CheckNextEvent(const K& key, boost::optional<V> oldVal, boost::optional<V> val)
    {
        CacheEntryEvent<K, V> event;
        bool success = eventQueue.Pull(event, boost::chrono::seconds(1));

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
        /**
        * Binary type definition.
        */
        IGNITE_BINARY_TYPE_START(TestEntry)
            IGNITE_BINARY_GET_TYPE_ID_AS_HASH(TestEntry)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(TestEntry)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_GET_HASH_CODE_ZERO(TestEntry)
            IGNITE_BINARY_IS_NULL_FALSE(TestEntry)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(TestEntry)

            void Write(BinaryWriter& writer, const TestEntry& obj)
            {
                writer.WriteInt32("value", obj.value);
            }

            TestEntry Read(BinaryReader& reader)
            {
                TestEntry res;
                res.value = reader.ReadInt32("value");

                return res;
            }

        IGNITE_BINARY_TYPE_END
    }
}

/*
 * Test setup fixture.
 */
struct ContinuousQueryTestSuiteFixture
{
    Ignite grid;

    Cache<int, TestEntry> cache;

    /*
     * Constructor.
     */
    ContinuousQueryTestSuiteFixture() :
        grid(ignite_test::StartNode("cache-query-continuous.xml", "node-01")),
        cache(grid.GetCache<int, TestEntry>("transactional_no_backup"))
    {
        // No-op.
    }

    /*
     * Destructor.
     */
    ~ContinuousQueryTestSuiteFixture()
    {
        Ignition::StopAll(false);

        grid = Ignite();
    }
};

void CheckEvents(Cache<int, TestEntry>& cache, Listener<int, TestEntry>& lsnr)
{
    cache.Put(1, TestEntry(10));
    lsnr.CheckNextEvent(1, boost::none, TestEntry(10));

    cache.Put(1, TestEntry(20));
    lsnr.CheckNextEvent(1, TestEntry(10), TestEntry(20));

    cache.Put(2, TestEntry(20));
    lsnr.CheckNextEvent(2, boost::none, TestEntry(20));

    cache.Remove(1);
    lsnr.CheckNextEvent(1, TestEntry(20), boost::none);
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

    BOOST_CHECK_EQUAL(qry.GetBufferSize(), QueryType::DEFAULT_BUFFER_SIZE);

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

BOOST_AUTO_TEST_SUITE_END()