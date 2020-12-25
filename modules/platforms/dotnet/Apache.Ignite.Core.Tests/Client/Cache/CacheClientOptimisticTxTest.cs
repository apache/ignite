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
    using System.Linq;
    using System.Threading.Tasks;
    using System.Transactions;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Client.Transactions;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="TransactionConcurrency.Optimistic"/> mode.
    /// </summary>
    public class CacheClientOptimisticTxTest : ClientTestBase
    {
        /// <summary>
        /// Tests client explicit optimistic transactions.
        /// </summary>
        [Test]
        public void TestExplicitOptimisticTransactionThrowsOptimisticExceptionOnConflict()
        {
            var cache = GetTransactionalCache();
            cache[1] = 1;
            var transactions = Client.GetTransactions();

            ITransaction igniteTx;
            using (var tx = transactions.TxStart())
            {
                igniteTx = GetSingleLocalTransaction();
                Assert.IsNotNull(igniteTx);
                Assert.IsNotNull(transactions.Tx);
                Assert.AreEqual(TransactionConcurrency.Optimistic, igniteTx.Concurrency);
                Assert.AreEqual(TransactionIsolation.Serializable, igniteTx.Isolation);

                var old = cache[1];

                Task.Factory.StartNew(() =>
                        {
                            Assert.IsNull(transactions.Tx);
                            cache[1] = -1;
                        },
                        TaskCreationOptions.LongRunning)
                    .Wait();

                Assert.AreEqual(old, cache[1]);
                cache[1] = old + 1;

                var constraint = Is.TypeOf<IgniteClientException>()
                    .And.Message
                    .StartsWith(
                        "Failed to prepare transaction, read/write conflict [key=1, keyCls=java.lang.Integer, val=-1");
                Assert.Throws(constraint, () => tx.Commit());
            }

            Assert.AreEqual(-1, cache[1]);

            igniteTx.Dispose();
        }

        /// <summary>
        /// Tests client ambient optimistic transactions (with <see cref="TransactionScope"/>).
        /// </summary>
        [Test]
        public void TestAmbientOptimisticTransactionThrowsOptimisticExceptionOnConflict()
        {
            var cache = GetTransactionalCache();
            cache[1] = 1;
            var transactions = Client.GetTransactions();

            var scope = new TransactionScope();
            var old = cache[1];
            Assert.IsNotNull(transactions.Tx);

            var igniteTx = GetSingleLocalTransaction();
            Assert.AreEqual(TransactionConcurrency.Optimistic, igniteTx.Concurrency);
            Assert.AreEqual(TransactionIsolation.Serializable, igniteTx.Isolation);

            Task.Factory.StartNew(() =>
                    {
                        Assert.IsNull(transactions.Tx);
                        cache[1] = -1;
                    },
                    TaskCreationOptions.LongRunning)
                .Wait();

            Assert.AreEqual(old, cache[1]);
            cache[1] = old + 1;

            // Complete() just sets a flag, actual Commit is called from Dispose().
            scope.Complete();

            var constraint = Is.TypeOf<IgniteClientException>()
                .And.Message
                .StartsWith(
                    "Failed to prepare transaction, read/write conflict [key=1, keyCls=java.lang.Integer, val=-1");
            Assert.Throws(constraint, () => scope.Dispose());

            Assert.AreEqual(-1, cache[1]);
            Assert.IsNull(transactions.Tx);

            igniteTx.Dispose();
        }

        /** <inheritdoc /> */
        protected override IgniteClientConfiguration GetClientConfiguration()
        {
            return new IgniteClientConfiguration(base.GetClientConfiguration())
            {
                TransactionConfiguration = new TransactionClientConfiguration
                {
                    DefaultTransactionConcurrency = TransactionConcurrency.Optimistic,
                    DefaultTransactionIsolation = TransactionIsolation.Serializable
                }
            };
        }

        /// <summary>
        /// Gets or creates transactional cache
        /// </summary>
        private ICacheClient<int, int> GetTransactionalCache()
        {
            return Client.GetOrCreateCache<int, int>(new CacheClientConfiguration
            {
                Name = TestUtils.TestName,
                AtomicityMode = CacheAtomicityMode.Transactional
            });
        }

        /// <summary>
        /// Gets single transaction from Ignite.
        /// </summary>
        private static ITransaction GetSingleLocalTransaction()
        {
            return GetIgnite()
                .GetTransactions()
                .GetLocalActiveTransactions()
                .Single();
        }
    }
}
