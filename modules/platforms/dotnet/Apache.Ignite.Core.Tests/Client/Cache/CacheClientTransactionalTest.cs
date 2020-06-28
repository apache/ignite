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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System.Transactions;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Transactional cache client tests.
    /// </summary>
    public class CacheClientTransactionalTest : ClientTestBase
    {
        /** */
        private const int ServerCount = 3;

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheClientTransactionalTest" /> class.
        /// </summary>
        public CacheClientTransactionalTest() : base(ServerCount)
        {
            // No-op.
        }

        /// <summary>
        /// Tests that commit applies cache changes.
        /// </summary>
        [Test]
        public void TestTxCommit()
        {
            var cache = TransactionalCache();

            cache.Put(1, 1);
            cache.Put(2, 2);

            using (var tx = Client.Transactions.TxStart())
            {
                cache.Put(1, 10);
                cache.Put(2, 20);

                tx.Commit();
            }

            Assert.AreEqual(10, cache.Get(1));
            Assert.AreEqual(20, cache.Get(2));
        }

        /// <summary>
        /// Tests that rollback reverts cache changes.
        /// </summary>
        [Test]
        public void TestTxRollback()
        {
            var cache = TransactionalCache();

            cache.Put(1, 1);
            cache.Put(2, 2);

            using (var tx = Client.Transactions.TxStart())
            {
                cache.Put(1, 10);
                cache.Put(2, 20);

                tx.Rollback();
            }

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
        }

        /// <summary>
        /// Tests that closing transaction without commit reverts cache changes.
        /// </summary>
        [Test]
        public void TestTxClose()
        {
            var cache = TransactionalCache();

            cache.Put(1, 1);
            cache.Put(2, 2);

            using (var tx = Client.Transactions.TxStart())
            {
                cache.Put(1, 10);
                cache.Put(2, 20);
            }

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
        }

        /// <summary>
        /// Tests that client can't start multiple transactions in one thread.
        /// </summary>
        [Test]
        public void TestThrowsIfMultipleStarted()
        {
            Assert.Throws<IgniteClientException>(() =>
            {
                using (Client.Transactions.TxStart())
                using (Client.Transactions.TxStart())
                {
                    // No-op.
                }
            });
        }

        /// <summary>
        /// Tests that different clients can start transactions in one thread.
        /// </summary>
        [Test]
        public void TestDifferentClientsCanStartTransactions()
        {
            Assert.DoesNotThrow(() =>
            {
                using (Client.Transactions.TxStart())
                using (GetClient().Transactions.TxStart())
                {
                    // No-op.
                }
            });
        }

        /// <summary>
        /// Test Ignite thin client transaction enlistment in ambient <see cref="TransactionScope"/>.
        /// </summary>
        [Test]
        public void TestTransactionScopeSingleCache()
        {
            var cache = TransactionalCache();

            cache[1] = 1;
            cache[2] = 2;

            // Commit.
            using (var ts = new TransactionScope())
            {
                cache[1] = 10;
                cache[2] = 20;

                ts.Complete();
            }

            Assert.AreEqual(10, cache[1]);
            Assert.AreEqual(20, cache[2]);

            // Rollback.
            using (new TransactionScope())
            {
                cache[1] = 100;
                cache[2] = 200;
            }

            Assert.AreEqual(10, cache[1]);
            Assert.AreEqual(20, cache[2]);
        }

        /// <summary>
        /// Test Ignite thin client transaction enlistment in ambient <see cref="TransactionScope"/>
        /// with multiple participating caches.
        /// </summary>
        [Test]
        public void TestTransactionScopeMultiCache()
        {
            var cache1 = TransactionalCache();

            var cache2 = TransactionalCache(cache1.Name + "1");

            cache1[1] = 1;
            cache2[1] = 2;

            // Commit.
            using (var ts = new TransactionScope())
            {
                cache1.Put(1, 10);
                cache2.Put(1, 20);

                ts.Complete();
            }

            Assert.AreEqual(10, cache1[1]);
            Assert.AreEqual(20, cache2[1]);

            // Rollback.
            using (new TransactionScope())
            {
                cache1.Put(1, 100);
                cache2.Put(1, 200);
            }

            Assert.AreEqual(10, cache1[1]);
            Assert.AreEqual(20, cache2[1]);
        }

        /// <summary>
        /// Test Ignite thin client transaction enlistment in ambient <see cref="TransactionScope"/>
        /// when Ignite tx is started manually.
        /// </summary>
        [Test]
        public void TestTransactionScopeWithManualIgniteTx()
        {
            var cache = TransactionalCache();
            var transactions = Client.Transactions;

            cache[1] = 1;

            // When Ignite tx is started manually, it won't be enlisted in TransactionScope.
            using (var tx = transactions.TxStart())
            {
                using (new TransactionScope())
                {
                    cache[1] = 2;
                }  // Revert transaction scope.

                tx.Commit();  // Commit manual tx.
            }

            Assert.AreEqual(2, cache[1]);
        }

        /// <summary>
        /// Test Ignite transaction with <see cref="TransactionScopeOption.Suppress"/> option.
        /// </summary>
        [Test]
        public void TestSuppressedTransactionScope()
        {
            var cache = TransactionalCache();

            cache[1] = 1;

            using (new TransactionScope(TransactionScopeOption.Suppress))
            {
                cache[1] = 2;
            }

            // Even though transaction is not completed, the value is updated, because tx is suppressed.
            Assert.AreEqual(2, cache[1]);
        }

        /// <summary>
        /// Test Ignite thin client transaction enlistment in ambient <see cref="TransactionScope"/> with nested scopes.
        /// </summary>
        [Test]
        public void TestNestedTransactionScope()
        {
            var cache = TransactionalCache();

            cache[1] = 1;

            foreach (var option in new[] {TransactionScopeOption.Required, TransactionScopeOption.RequiresNew})
            {
                // Commit.
                using (var ts1 = new TransactionScope())
                {
                    using (var ts2 = new TransactionScope(option))
                    {
                        cache[1] = 2;
                        ts2.Complete();
                    }

                    cache[1] = 3;
                    ts1.Complete();
                }

                Assert.AreEqual(3, cache[1]);

                // Rollback.
                using (new TransactionScope())
                {
                    using (new TransactionScope(option))
                        cache[1] = 4;

                    cache[1] = 5;
                }

                // In case with Required option there is a single tx
                // that gets aborted, second put executes outside the tx.
                Assert.AreEqual(option == TransactionScopeOption.Required ? 5 : 3, cache[1], option.ToString());
            }
        }

        /// <summary>
        /// Gets or creates transactional cache
        /// </summary>
        private ICacheClient<int, int> TransactionalCache(string name = "client_transactional")
        {
            return Client.GetOrCreateCache<int, int>(new CacheClientConfiguration
            {
                Name = name,
                AtomicityMode = CacheAtomicityMode.Transactional
            });
        }
    }
}