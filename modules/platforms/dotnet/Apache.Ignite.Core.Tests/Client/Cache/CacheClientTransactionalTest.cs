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
        /// Test Ignite transaction enlistment in ambient <see cref="TransactionScope"/>.
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