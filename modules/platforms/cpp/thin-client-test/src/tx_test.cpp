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

#include <ignite/ignition.h>

#include <ignite/thin/ignite_client_configuration.h>
#include <ignite/thin/ignite_client.h>
#include <ignite/thin/cache/cache_peek_mode.h>

#include <test_utils.h>
#include <ignite/ignite_error.h>

using namespace ignite::thin;
using namespace boost::unit_test;

class IgniteTxTestSuiteFixture
{
public:
    IgniteTxTestSuiteFixture()
    {
        serverNode = ignite_test::StartCrossPlatformServerNode("cache.xml", "ServerNode");
    }

    ~IgniteTxTestSuiteFixture()
    {
        ignite::Ignition::StopAll(false);
    }

private:
    /** Server node. */
    ignite::Ignite serverNode;
};

BOOST_FIXTURE_TEST_SUITE(IgniteClientTestSuite, IgniteTxTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TestCacheOpsWithTx)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

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

    BOOST_CHECK_EQUAL(cache.GetSize(cache::CachePeekMode::ALL), 1);

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
}

BOOST_AUTO_TEST_CASE(TestTxOps)
{
    IgniteClientConfiguration cfg;

    cfg.SetEndPoints("127.0.0.1:11110");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int, int> cache =
        client.GetCache<int, int>("partitioned");

    cache.Put(1, 1);

    transactions::ClientTransactions transactions = client.ClientTransactions();

    transactions::ClientTransaction tx1 = transactions.TxStart();

    BOOST_CHECK_THROW( transactions.TxStart(), ignite::IgniteError );

    tx1.Close();
}

BOOST_AUTO_TEST_SUITE_END()
