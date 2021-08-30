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

namespace Apache.Ignite.Core.Tests.Cache
{
    using System.Threading.Tasks;
    using System.Transactions;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="TransactionConcurrency.Optimistic"/> mode.
    /// </summary>
    public class OptimisticTransactionTest : TestBase
    {
        /// <summary>
        /// Tests explicit optimistic transactions.
        /// </summary>
        [Test]
        public void TestExplicitOptimisticTransactionThrowsOptimisticExceptionOnConflict()
        {
            var cache = GetCache();
            var transactions = Ignite.GetTransactions();

            using (var tx = transactions.TxStart())
            {
                Assert.IsNotNull(transactions.Tx);
                Assert.AreEqual(TransactionConcurrency.Optimistic, tx.Concurrency);
                Assert.AreEqual(TransactionIsolation.Serializable, tx.Isolation);

                var old = cache[1];

                Task.Factory.StartNew(() =>
                {
                    Assert.IsNull(transactions.Tx);
                    cache[1] = -1;
                }).Wait();

                Assert.AreEqual(old, cache[1]);
                cache[1] = old + 1;

                var ex = Assert.Throws<TransactionOptimisticException>(() => tx.Commit());
                StringAssert.StartsWith(
                    "Failed to prepare transaction, read/write conflict [key=1, keyCls=java.lang.Integer, val=-1",
                    ex.Message);
            }

            Assert.AreEqual(-1, cache[1]);
        }

        /// <summary>
        /// Tests ambient optimistic transactions (with <see cref="TransactionScope"/>).
        /// </summary>
        [Test]
        public void TestAmbientOptimisticTransactionThrowsOptimisticExceptionOnConflict()
        {
            var cache = GetCache();
            var transactions = Ignite.GetTransactions();

            var scope = new TransactionScope();
            var old = cache[1];

            Assert.IsNotNull(transactions.Tx);
            Assert.AreEqual(TransactionConcurrency.Optimistic, transactions.Tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.Serializable, transactions.Tx.Isolation);

            Task.Factory.StartNew(() =>
                {
                    Assert.IsNull(transactions.Tx);
                    cache[1] = -1;
                }, TaskCreationOptions.LongRunning)
                .Wait();

            Assert.AreEqual(old, cache[1]);
            cache[1] = old + 1;

            // Complete() just sets a flag, actual Commit is called from Dispose().
            scope.Complete();

            var ex = Assert.Throws<TransactionOptimisticException>(() => scope.Dispose());
            StringAssert.StartsWith(
                "Failed to prepare transaction, read/write conflict [key=1, keyCls=java.lang.Integer, val=-1",
                ex.Message);

            Assert.AreEqual(-1, cache[1]);
            Assert.IsNull(transactions.Tx);
        }

        /** <inheritdoc /> */
        protected override IgniteConfiguration GetConfig()
        {
            return new IgniteConfiguration(base.GetConfig())
            {
                TransactionConfiguration = new TransactionConfiguration
                {
                    DefaultTransactionConcurrency = TransactionConcurrency.Optimistic,
                    DefaultTransactionIsolation = TransactionIsolation.Serializable
                }
            };
        }

        /// <summary>
        /// Gets the cache.
        /// </summary>
        private ICache<int, int> GetCache()
        {
            var cacheConfiguration = new CacheConfiguration(TestUtils.TestName)
            {
                AtomicityMode = CacheAtomicityMode.Transactional
            };

            var cache = Ignite.GetOrCreateCache<int, int>(cacheConfiguration);

            cache[1] = 1;

            return cache;
        }
    }
}
