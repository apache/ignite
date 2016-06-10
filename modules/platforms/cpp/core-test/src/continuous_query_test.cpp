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
class Listener : public CacheEntryEventListener<int, int>
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
    virtual void OnEvent(const CacheEntryEvent<int, int>* evts, uint32_t num)
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
    void CheckNextEvent(int key, boost::optional<int> oldVal, boost::optional<int> val)
    {
        CacheEntryEvent<int, int> event;
        bool success = eventQueue.Pull(event, boost::chrono::seconds(1));

        BOOST_REQUIRE(success);

        BOOST_CHECK_EQUAL(event.GetKey(), key);
        BOOST_CHECK_EQUAL(event.HasOldValue(), oldVal.is_initialized());
        BOOST_CHECK_EQUAL(event.HasValue(), val.is_initialized());

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
        CacheEntryEvent<int, int> event;
        bool success = eventQueue.Pull(event, timeout);

        BOOST_REQUIRE(!success);
    }

private:
    // Events queue.
    ConcurrentQueue< CacheEntryEvent<int, int> > eventQueue;
};

/*
 * Test setup fixture.
 */
struct ContinuousQueryTestSuiteFixture
{
    Ignite grid;

    Cache<int, int> cache;

    /*
     * Get configuration for nodes.
     */
    IgniteConfiguration GetConfiguration()
    {
        IgniteConfiguration cfg;

        cfg.jvmOpts.push_back("-Xdebug");
        cfg.jvmOpts.push_back("-Xnoagent");
        cfg.jvmOpts.push_back("-Djava.compiler=NONE");
        cfg.jvmOpts.push_back("-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005");
        cfg.jvmOpts.push_back("-XX:+HeapDumpOnOutOfMemoryError");

#ifdef IGNITE_TESTS_32
        cfg.jvmInitMem = 256;
        cfg.jvmMaxMem = 768;
#else
        cfg.jvmInitMem = 1024;
        cfg.jvmMaxMem = 4096;
#endif

        char* cfgPath = getenv("IGNITE_NATIVE_TEST_CPP_CONFIG_PATH");

        cfg.springCfgPath = std::string(cfgPath).append("/").append("cache-query-continuous.xml");

        return cfg;
    }

    /*
     * Constructor.
     */
    ContinuousQueryTestSuiteFixture() :
        grid(Ignition::Start(GetConfiguration(), "node-01")),
        cache(grid.GetCache<int, int>("transactional_no_backup"))
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

BOOST_FIXTURE_TEST_SUITE(ContinuousQueryTestSuite, ContinuousQueryTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestBasic)
{
    Listener lsnr;

    ContinuousQuery<int, int> qry(lsnr);

    cache.QueryContinuous(qry);

    cache.Put(1, 10);
    lsnr.CheckNextEvent(1, boost::none, 10);

    cache.Put(1, 20);
    lsnr.CheckNextEvent(1, 10, 20);

    cache.Put(2, 20);
    lsnr.CheckNextEvent(2, boost::none, 20);

    cache.Remove(1);
    lsnr.CheckNextEvent(1, 20, boost::none);
}

BOOST_AUTO_TEST_CASE(TestInitialQueryScan)
{
    Listener lsnr;

    ContinuousQuery<int, int> qry(lsnr);

    cache.Put(11, 111);
    cache.Put(22, 222);
    cache.Put(33, 333);

    ContinuousQueryHandle<int, int> handle = cache.QueryContinuous(qry, ScanQuery());

    std::vector< CacheEntry<int, int> > vals;

    handle.GetInitialQueryCursor().GetAll(vals);

    BOOST_CHECK_THROW(handle.GetInitialQueryCursor(), IgniteError);

    BOOST_REQUIRE_EQUAL(vals.size(), 3);

    BOOST_CHECK_EQUAL(vals[0].GetKey(), 11);
    BOOST_CHECK_EQUAL(vals[1].GetKey(), 22);
    BOOST_CHECK_EQUAL(vals[2].GetKey(), 33);

    BOOST_CHECK_EQUAL(vals[0].GetValue(), 111);
    BOOST_CHECK_EQUAL(vals[1].GetValue(), 222);
    BOOST_CHECK_EQUAL(vals[2].GetValue(), 333);

    cache.Put(1, 10);
    lsnr.CheckNextEvent(1, boost::none, 10);

    cache.Put(1, 20);
    lsnr.CheckNextEvent(1, 10, 20);

    cache.Put(2, 20);
    lsnr.CheckNextEvent(2, boost::none, 20);

    cache.Remove(1);
    lsnr.CheckNextEvent(1, 20, boost::none);
}

BOOST_AUTO_TEST_SUITE_END()