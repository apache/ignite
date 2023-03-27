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

#include <boost/chrono.hpp>
#include <boost/thread.hpp>
#include <boost/test/unit_test.hpp>

#include <ignite/ignition.h>

#include "ignite/thin/ignite_client_configuration.h"
#include "ignite/thin/ignite_client.h"
#include "ignite/thin/cache/cache_peek_mode.h"
#include "ignite/thin/transactions/transaction_consts.h"

#include <test_utils.h>
#include <ignite/ignite_error.h>

using namespace ignite::thin;
using namespace boost::unit_test;
using namespace ignite::thin::transactions;
using namespace ignite::common::concurrent;

class IgniteTxTestSuiteFixture
{
public:
    IgniteTxTestSuiteFixture()
    {
        node1 = ignite_test::StartCrossPlatformServerNode("cache.xml", "node1");
        node2 = ignite_test::StartCrossPlatformServerNode("cache.xml", "node2");
    }

    ~IgniteTxTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

    /**
     * Start client.
     */
    static IgniteClient StartClient()
    {
        IgniteClientConfiguration cfg;

        cfg.SetEndPoints("127.0.0.1:11110,127.0.0.1:11111");

        return IgniteClient::Start(cfg);
    }

private:
    /** Server node #1. */
    ignite::Ignite node1;

    /** Server node #2. */
    ignite::Ignite node2;
};

BOOST_FIXTURE_TEST_SUITE(IgniteTxTestSuite, IgniteTxTestSuiteFixture)

bool correctCloseMessage(const ignite::IgniteError& ex)
{
    BOOST_CHECK_EQUAL(ex.what(), std::string(TX_ALREADY_CLOSED));

    return true;
}

bool separateThreadMessage(const ignite::IgniteError& ex)
{
    BOOST_CHECK_EQUAL(ex.what(), std::string(TX_DIFFERENT_THREAD));

    return true;
}

bool checkTxTimeoutMessage(const ignite::IgniteError& ex)
{
    return std::string(ex.what()).find("Cache transaction timed out") != std::string::npos;
}

BOOST_AUTO_TEST_CASE(TestCacheOpsWithTx)
{
    IgniteClient client = StartClient();

    cache::CacheClient<int, int> cache =
        client.GetCache<int, int>("partitioned");

    cache.Put(1, 1);

    transactions::ClientTransactions transactions = client.ClientTransactions();

    transactions::ClientTransaction tx = transactions.TxStart();

    cache.Put(1, 10);

    BOOST_CHECK_EQUAL(10, cache.Get(1));

    tx.Rollback();

    BOOST_CHECK_EQUAL(1, cache.Get(1));

    //---

    tx = transactions.TxStart();

    cache.Put(1, 10);

    tx.Commit();

    BOOST_CHECK_EQUAL(10, cache.Get(1));

    cache.Put(1, 1);

    //---

    tx = transactions.TxStart();

    cache.Put(1, 10);

    BOOST_CHECK_EQUAL(10, cache.Get(1));

    tx.Close();

    BOOST_CHECK_EQUAL(1, cache.Get(1));

    //---

    tx = transactions.TxStart(TransactionConcurrency::OPTIMISTIC, TransactionIsolation::SERIALIZABLE);

    cache.Put(1, 10);

    BOOST_CHECK_EQUAL(10, cache.Get(1));

    tx.Close();

    BOOST_CHECK_EQUAL(1, cache.Get(1));

    //---

    tx = transactions.TxStart(TransactionConcurrency::OPTIMISTIC, TransactionIsolation::SERIALIZABLE, 1000, 100);

    cache.Put(1, 10);

    BOOST_CHECK_EQUAL(10, cache.Get(1));

    tx.Close();

    BOOST_CHECK_EQUAL(1, cache.Get(1));

    //---

    tx = transactions.TxStart();

    cache.Replace(1, 10);

    BOOST_CHECK_EQUAL(10, cache.Get(1));

    tx.Rollback();

    BOOST_CHECK_EQUAL(1, cache.Get(1));

    //---

    tx = transactions.TxStart();

    cache.Replace(1, 1, 10);

    BOOST_CHECK_EQUAL(10, cache.Get(1));

    tx.Rollback();

    BOOST_CHECK_EQUAL(1, cache.Get(1));

    //---

    tx = transactions.TxStart();

    cache.Put(2, 20);

    BOOST_CHECK_EQUAL(cache.ContainsKey(2), true);

    tx.Rollback();

    BOOST_CHECK_EQUAL(cache.ContainsKey(2), false);

    //---

    tx = transactions.TxStart();

    cache.Put(2, 20);

    tx.Rollback();

    BOOST_CHECK_EQUAL(cache.GetSize(cache::CachePeekMode::PRIMARY), 1);

    //---

    tx = transactions.TxStart();

    int res1 = cache.GetAndPutIfAbsent(1, 10);

    int res2 = cache.GetAndPutIfAbsent(2, 20);

    BOOST_CHECK_EQUAL(1, res1);

    BOOST_CHECK_EQUAL(cache.Get(2), 20);

    BOOST_CHECK_EQUAL(0, res2);

    tx.Rollback();

    BOOST_CHECK_EQUAL(cache.Get(2), 0);

    //---

    tx = transactions.TxStart();

    cache.Remove(1);

    tx.Rollback();

    BOOST_CHECK_EQUAL(cache.ContainsKey(1), true);

    // Test transaction with a timeout.

    const uint32_t TX_TIMEOUT = 200;

    tx = transactions.TxStart(TransactionConcurrency::OPTIMISTIC, TransactionIsolation::SERIALIZABLE, TX_TIMEOUT);

    cache.Put(1, 10);

    boost::this_thread::sleep_for(boost::chrono::milliseconds(2 * TX_TIMEOUT));

    BOOST_CHECK_EXCEPTION(cache.Put(1, 20), ignite::IgniteError, checkTxTimeoutMessage);

    BOOST_CHECK_EXCEPTION(tx.Commit(), ignite::IgniteError, checkTxTimeoutMessage);

    tx.Close();

    BOOST_CHECK_EQUAL(1, cache.Get(1));
}

void startAnotherClientAndTx(SharedPointer<SingleLatch>& l)
{
    IgniteClient client = IgniteTxTestSuiteFixture::StartClient();

    cache::CacheClient<int, int> cache =
        client.GetCache<int, int>("partitioned");

    transactions::ClientTransactions transactions = client.ClientTransactions();

    transactions::ClientTransaction tx = transactions.TxStart();

    l.Get()->CountDown();

    cache.Put(2, 20);

    tx.Commit();
}

BOOST_AUTO_TEST_CASE(TestTxOps)
{
    IgniteClient client = StartClient();

    cache::CacheClient<int, int> cache =
        client.GetCache<int, int>("partitioned");

    cache.Put(1, 1);

    transactions::ClientTransactions transactions = client.ClientTransactions();

    transactions::ClientTransaction tx = transactions.TxStart();

    BOOST_CHECK_THROW( transactions.TxStart(), ignite::IgniteError );

    tx.Close();

    //Test end of already completed transaction.

    tx = transactions.TxStart();

    tx.Close();

    BOOST_CHECK_EXCEPTION(tx.Commit(), ignite::IgniteError, correctCloseMessage);

    BOOST_CHECK_EXCEPTION(tx.Rollback(), ignite::IgniteError, correctCloseMessage);

    // Test end of outdated transaction.

    transactions::ClientTransaction tx1 = transactions.TxStart();

    BOOST_CHECK_EXCEPTION(tx.Commit(), ignite::IgniteError, separateThreadMessage);

    tx1.Close();

    // Test end of outdated transaction.

    tx = transactions.TxStart();

    tx.Commit();

    BOOST_CHECK_EXCEPTION(tx.Commit(), ignite::IgniteError, correctCloseMessage);

    tx.Close();

    // Check multi threads.

    SharedPointer<SingleLatch> latch = SharedPointer<SingleLatch>(new SingleLatch());

    tx = transactions.TxStart();

    cache.Put(1, 10);

    boost::thread t2(startAnotherClientAndTx, latch);

    latch.Get()->Await();

    tx.Rollback();

    t2.join();

    BOOST_CHECK_EQUAL(1, cache.Get(1));

    BOOST_CHECK_EQUAL(20, cache.Get(2));
}

const std::string label = std::string("label_2_check");

std::string label1 = std::string("label_2_check1");

bool checkTxLabelMessage(const ignite::IgniteError& ex)
{
    return std::string(ex.what()).find(label) != std::string::npos;
}

bool checkTxLabel1Message(const ignite::IgniteError& ex)
{
    return std::string(ex.what()).find("label_2_check1") != std::string::npos;
}

BOOST_AUTO_TEST_CASE(TestTxWithLabel)
{
    IgniteClient client = StartClient();

    cache::CacheClient<int, int> cache =
        client.GetCache<int, int>("partitioned");

    const uint32_t TX_TIMEOUT = 200;

    transactions::ClientTransactions transactions = client.ClientTransactions();

    transactions::ClientTransaction tx = transactions.withLabel(label).TxStart(TransactionConcurrency::OPTIMISTIC, TransactionIsolation::SERIALIZABLE, TX_TIMEOUT);

    cache.Put(1, 10);

    boost::this_thread::sleep_for(boost::chrono::milliseconds(2 * TX_TIMEOUT));

    BOOST_CHECK_EXCEPTION(tx.Commit(), ignite::IgniteError, checkTxLabelMessage);

    tx.Close();

    // New label

    tx = transactions.TxStart(TransactionConcurrency::OPTIMISTIC, TransactionIsolation::SERIALIZABLE, TX_TIMEOUT);

    cache.Put(1, 10);

    boost::this_thread::sleep_for(boost::chrono::milliseconds(2 * TX_TIMEOUT));

    BOOST_CHECK_EXCEPTION(tx.Commit(), ignite::IgniteError, !checkTxLabelMessage);

    tx.Close();

    // Label is gone

    tx = transactions.withLabel(label1).TxStart(TransactionConcurrency::OPTIMISTIC, TransactionIsolation::SERIALIZABLE, TX_TIMEOUT);

    label1 = "NULL";

    cache.Put(1, 10);

    boost::this_thread::sleep_for(boost::chrono::milliseconds(2 * TX_TIMEOUT));

    BOOST_CHECK_EXCEPTION(tx.Commit(), ignite::IgniteError, checkTxLabel1Message);

    tx.Close();
}

BOOST_AUTO_TEST_CASE(ManyTransactions)
{
    IgniteClient client = StartClient();

    cache::CacheClient<int, int> cache =
            client.GetCache<int, int>("partitioned");

    transactions::ClientTransactions transactions = client.ClientTransactions();
    const int32_t key = 42;

    for (int32_t val = 0; val < 100; ++val) {
        transactions::ClientTransaction tx = transactions.TxStart();

        cache.Put(key, val);

        tx.Commit();

        BOOST_CHECK_EQUAL(val, cache.Get(key));
    }

    const int32_t expected = -42;

    cache.Put(key, expected);

    BOOST_CHECK_EQUAL(expected, cache.Get(key));

    for (int32_t val = 0; val < 100; ++val) {
        transactions::ClientTransaction tx = transactions.TxStart();

        cache.Put(key, val);

        tx.Rollback();

        BOOST_CHECK_EQUAL(expected, cache.Get(key));
    }
}

BOOST_AUTO_TEST_SUITE_END()
