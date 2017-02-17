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

#include <boost/test/unit_test.hpp>

#include "ignite/ignition.h"
#include "ignite/test_utils.h"

using namespace ignite;
using namespace ignite::transactions;
using namespace ignite::cache;
using namespace boost::unit_test;

/*
 * Test setup fixture.
 */
struct TransactionsTestSuiteFixture {
    Ignite grid;

    /*
     * Constructor.
     */
    TransactionsTestSuiteFixture()
    {
        grid = ignite_test::StartNode("cache-test.xml", "txTest");
    }

    /*
     * Destructor.
     */
    ~TransactionsTestSuiteFixture()
    {
        Ignition::Stop(grid.GetName(), true);

        grid = Ignite();
    }
};

BOOST_FIXTURE_TEST_SUITE(TransactionsTestSuite, TransactionsTestSuiteFixture)

BOOST_AUTO_TEST_CASE(TransactionsMetrics)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    Transactions transactions = grid.GetTransactions();

    TransactionMetrics metrics = transactions.GetMetrics();
    BOOST_REQUIRE(metrics.IsValid());

    BOOST_CHECK_EQUAL(0, metrics.GetCommits());
    BOOST_CHECK_EQUAL(0, metrics.GetRollbacks());
}

BOOST_AUTO_TEST_CASE(TransactionCommit)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.GetTx();
    BOOST_REQUIRE(!tx.IsValid());

    tx = transactions.TxStart();

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    cache.Put(1, 1);
    cache.Put(2, 2);

    tx.Commit();

    BOOST_CHECK_EQUAL(1, cache.Get(1));
    BOOST_CHECK_EQUAL(2, cache.Get(2));

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_CASE(TransactionRollback)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    cache.Put(1, 1);
    cache.Put(2, 2);

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.GetTx();
    BOOST_REQUIRE(!tx.IsValid());

    tx = transactions.TxStart();

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    cache.Put(1, 10);
    cache.Put(2, 20);

    tx.Rollback();

    BOOST_CHECK_EQUAL(1, cache.Get(1));
    BOOST_CHECK_EQUAL(2, cache.Get(2));

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_CASE(TransactionClose)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    cache.Put(1, 1);
    cache.Put(2, 2);

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.GetTx();
    BOOST_REQUIRE(!tx.IsValid());

    tx = transactions.TxStart();

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    cache.Put(1, 10);
    cache.Put(2, 20);

    tx.Close();

    BOOST_CHECK_EQUAL(1, cache.Get(1));
    BOOST_CHECK_EQUAL(2, cache.Get(2));

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_CASE(TransactionRollbackOnly)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    cache.Put(1, 1);
    cache.Put(2, 2);

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.TxStart();

    cache.Put(1, 10);
    cache.Put(2, 20);

    BOOST_CHECK(!tx.IsRollbackOnly());

    tx.SetRollbackOnly();

    BOOST_CHECK(tx.IsRollbackOnly());

    try
    {
        tx.Commit();

        BOOST_FAIL("Commit must fail for rollback-only transaction.");
    }
    catch (IgniteError& error)
    {
        // Expected exception.
        BOOST_CHECK(error.GetCode() != IgniteError::IGNITE_SUCCESS);
    }

    tx.Close();

    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ROLLED_BACK, tx.GetState());
    BOOST_CHECK(tx.IsRollbackOnly());

    BOOST_CHECK_EQUAL(1, cache.Get(1));
    BOOST_CHECK_EQUAL(2, cache.Get(2));

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_CASE(TransactionAttributes)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.GetTx();
    BOOST_REQUIRE(!tx.IsValid());

    tx = transactions.TxStart(IGNITE_TX_CONCURRENCY_OPTIMISTIC,
        IGNITE_TX_ISOLATION_SERIALIZABLE, 1000, 100);

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    BOOST_CHECK_EQUAL(IGNITE_TX_CONCURRENCY_OPTIMISTIC, tx.GetConcurrency());
    BOOST_CHECK_EQUAL(IGNITE_TX_ISOLATION_SERIALIZABLE, tx.GetIsolation());
    BOOST_CHECK_EQUAL(1000, tx.GetTimeout());
    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ACTIVE, tx.GetState());

    tx.Commit();

    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_COMMITTED, tx.GetState());

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());

    tx = transactions.TxStart(IGNITE_TX_CONCURRENCY_PESSIMISTIC,
        IGNITE_TX_ISOLATION_READ_COMMITTED, 2000, 10);

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    BOOST_CHECK_EQUAL(IGNITE_TX_CONCURRENCY_PESSIMISTIC, tx.GetConcurrency());
    BOOST_CHECK_EQUAL(IGNITE_TX_ISOLATION_READ_COMMITTED, tx.GetIsolation());
    BOOST_CHECK_EQUAL(2000, tx.GetTimeout());
    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ACTIVE, tx.GetState());

    tx.Rollback();

    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ROLLED_BACK, tx.GetState());

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());

    tx = transactions.TxStart(IGNITE_TX_CONCURRENCY_OPTIMISTIC,
        IGNITE_TX_ISOLATION_REPEATABLE_READ, 3000, 0);

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    BOOST_CHECK_EQUAL(IGNITE_TX_CONCURRENCY_OPTIMISTIC, tx.GetConcurrency());
    BOOST_CHECK_EQUAL(IGNITE_TX_ISOLATION_REPEATABLE_READ, tx.GetIsolation());
    BOOST_CHECK_EQUAL(3000, tx.GetTimeout());
    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ACTIVE, tx.GetState());

    tx.Close();

    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ROLLED_BACK, tx.GetState());

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_CASE(CrossCacheCommit)
{
    Cache<int, int> cache1 = grid.GetCache<int, int>("partitioned");
    Cache<int, int> cache2 = grid.GetCache<int, int>("partitioned2");

    cache1.Put(1, 1);
    cache1.Put(2, 1);

    cache2.Put(1, 2);
    cache2.Put(2, 2);

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.GetTx();
    BOOST_REQUIRE(!tx.IsValid());

    tx = transactions.TxStart();

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    cache1.Put(1, 10);
    cache1.Put(2, 10);

    cache2.Put(1, 20);
    cache2.Put(2, 20);

    tx.Commit();

    BOOST_CHECK_EQUAL(10, cache1.Get(1));
    BOOST_CHECK_EQUAL(10, cache1.Get(2));

    BOOST_CHECK_EQUAL(20, cache2.Get(1));
    BOOST_CHECK_EQUAL(20, cache2.Get(2));

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_CASE(CrossCacheRollback)
{
    Cache<int, int> cache1 = grid.GetCache<int, int>("partitioned");
    Cache<int, int> cache2 = grid.GetCache<int, int>("partitioned2");

    cache1.Put(1, 1);
    cache1.Put(2, 1);

    cache2.Put(1, 2);
    cache2.Put(2, 2);

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.GetTx();
    BOOST_REQUIRE(!tx.IsValid());

    tx = transactions.TxStart();

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    cache1.Put(1, 10);
    cache1.Put(2, 10);

    cache2.Put(1, 20);
    cache2.Put(2, 20);

    tx.Rollback();

    BOOST_CHECK_EQUAL(1, cache1.Get(1));
    BOOST_CHECK_EQUAL(1, cache1.Get(2));

    BOOST_CHECK_EQUAL(2, cache2.Get(1));
    BOOST_CHECK_EQUAL(2, cache2.Get(2));

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_CASE(TransactionsMetricsNe)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    Transactions transactions = grid.GetTransactions();

    IgniteError err;

    TransactionMetrics metrics = transactions.GetMetrics(err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_REQUIRE(metrics.IsValid());

    BOOST_CHECK_EQUAL(0, metrics.GetCommits());
    BOOST_CHECK_EQUAL(0, metrics.GetRollbacks());
}

BOOST_AUTO_TEST_CASE(TransactionCommitNe)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    Transactions transactions = grid.GetTransactions();

    IgniteError err;

    Transaction tx = transactions.GetTx();
    BOOST_REQUIRE(!tx.IsValid());

    tx = transactions.TxStart(err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    cache.Put(1, 1, err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    cache.Put(2, 2, err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    tx.Commit(err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_CHECK_EQUAL(1, cache.Get(1, err));
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_CHECK_EQUAL(2, cache.Get(2, err));
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_CASE(TransactionRollbackNe)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    IgniteError err;

    cache.Put(1, 1, err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    cache.Put(2, 2, err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.GetTx();
    BOOST_REQUIRE(!tx.IsValid());

    tx = transactions.TxStart(err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    cache.Put(1, 10, err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    cache.Put(2, 20, err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    tx.Rollback(err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_CHECK_EQUAL(1, cache.Get(1, err));
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_CHECK_EQUAL(2, cache.Get(2, err));
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_CASE(TransactionCloseNe)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    IgniteError err;

    cache.Put(1, 1, err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    cache.Put(2, 2, err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.GetTx();
    BOOST_REQUIRE(!tx.IsValid());

    tx = transactions.TxStart(err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    cache.Put(1, 10, err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    cache.Put(2, 20, err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    tx.Close(err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_CHECK_EQUAL(1, cache.Get(1, err));
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_CHECK_EQUAL(2, cache.Get(2, err));
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_CASE(TransactionRollbackOnlyNe)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    IgniteError err;

    cache.Put(1, 1);
    cache.Put(2, 2);

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.TxStart();

    cache.Put(1, 10);
    cache.Put(2, 20);

    BOOST_CHECK(!tx.IsRollbackOnly());

    tx.SetRollbackOnly(err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_CHECK(tx.IsRollbackOnly());

    tx.Commit(err);
    BOOST_REQUIRE(err.GetCode() != IgniteError::IGNITE_SUCCESS);

    tx.Close(err);
    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ROLLED_BACK, tx.GetState());
    BOOST_CHECK(tx.IsRollbackOnly());

    BOOST_CHECK_EQUAL(1, cache.Get(1));
    BOOST_CHECK_EQUAL(2, cache.Get(2));

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_CASE(TransactionAttributesNe)
{
    Cache<int, int> cache = grid.GetCache<int, int>("partitioned");

    IgniteError err;

    Transactions transactions = grid.GetTransactions();

    Transaction tx = transactions.GetTx();
    BOOST_REQUIRE(!tx.IsValid());

    tx = transactions.TxStart(IGNITE_TX_CONCURRENCY_OPTIMISTIC,
        IGNITE_TX_ISOLATION_SERIALIZABLE, 1000, 100, err);

    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    BOOST_CHECK_EQUAL(IGNITE_TX_CONCURRENCY_OPTIMISTIC, tx.GetConcurrency());
    BOOST_CHECK_EQUAL(IGNITE_TX_ISOLATION_SERIALIZABLE, tx.GetIsolation());
    BOOST_CHECK_EQUAL(1000, tx.GetTimeout());
    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ACTIVE, tx.GetState());

    tx.Commit();

    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_COMMITTED, tx.GetState());

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());

    tx = transactions.TxStart(IGNITE_TX_CONCURRENCY_PESSIMISTIC,
        IGNITE_TX_ISOLATION_READ_COMMITTED, 2000, 10, err);

    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    BOOST_CHECK_EQUAL(IGNITE_TX_CONCURRENCY_PESSIMISTIC, tx.GetConcurrency());
    BOOST_CHECK_EQUAL(IGNITE_TX_ISOLATION_READ_COMMITTED, tx.GetIsolation());
    BOOST_CHECK_EQUAL(2000, tx.GetTimeout());
    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ACTIVE, tx.GetState());

    tx.Rollback();

    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ROLLED_BACK, tx.GetState());

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());

    tx = transactions.TxStart(IGNITE_TX_CONCURRENCY_OPTIMISTIC,
        IGNITE_TX_ISOLATION_REPEATABLE_READ, 3000, 0, err);

    if (!err.GetCode() == IgniteError::IGNITE_SUCCESS)
        BOOST_ERROR(err.GetText());

    BOOST_REQUIRE(transactions.GetTx().IsValid());

    BOOST_CHECK_EQUAL(IGNITE_TX_CONCURRENCY_OPTIMISTIC, tx.GetConcurrency());
    BOOST_CHECK_EQUAL(IGNITE_TX_ISOLATION_REPEATABLE_READ, tx.GetIsolation());
    BOOST_CHECK_EQUAL(3000, tx.GetTimeout());
    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ACTIVE, tx.GetState());

    tx.Close();

    BOOST_CHECK_EQUAL(IGNITE_TX_STATE_ROLLED_BACK, tx.GetState());

    tx = transactions.GetTx();

    BOOST_CHECK(!tx.IsValid());
}

BOOST_AUTO_TEST_SUITE_END()
