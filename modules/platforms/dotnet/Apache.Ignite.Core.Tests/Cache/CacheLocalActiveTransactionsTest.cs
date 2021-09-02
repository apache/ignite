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
    using System;
    using System.Linq;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Impl.Transactions;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;
    using NUnit.Framework.Constraints;

    /// <summary>
    /// Tests <see cref="ITransactions.GetLocalActiveTransactions"/>.
    /// </summary>
    public class CacheLocalActiveTransactionsTest : TestBase
    {
        /// <summary>
        /// Tests that transaction properties are applied and propagated properly.
        /// </summary>
        [Test]
        public void TestTxAttributes()
        {
            var cache = Cache();
            var transactions = cache.Ignite.GetTransactions();
            var label = TestUtils.TestName;
            var timeout = TimeSpan.FromDays(1);
            var isolations = new[]
            {
                TransactionIsolation.Serializable,
                TransactionIsolation.ReadCommitted,
                TransactionIsolation.RepeatableRead
            };
            var options = new[] {TransactionConcurrency.Optimistic, TransactionConcurrency.Pessimistic};

            Assert.IsEmpty(transactions.GetLocalActiveTransactions());
            foreach (var concurrency in options)
            foreach (var isolation in isolations)
            {
                using (var tx = transactions.WithLabel(label).TxStart(concurrency, isolation, timeout, 1))
                {
                    var activeTransactions = transactions.GetLocalActiveTransactions();

                    Assert.AreEqual(1, activeTransactions.Count);
                    var txView = activeTransactions.First();
                    Assert.AreEqual(concurrency, txView.Concurrency);
                    Assert.AreEqual(isolation, txView.Isolation);
                    Assert.AreEqual(tx.NodeId, txView.NodeId);
                    Assert.AreEqual(tx.State, txView.State);
                    Assert.AreEqual(label, txView.Label);
                    Assert.AreEqual(timeout, txView.Timeout);
                    Assert.IsTrue(txView.IsRollbackOnly);
                }
            }

            Assert.IsEmpty(transactions.GetLocalActiveTransactions());
        }

        /// <summary>
        /// Test that rollbacks.
        /// </summary>
        [Test]
        [TestCase(true)]
        [TestCase(false)]
        public void TestRollbacks(bool async)
        {
            var cache = Cache();
            var transactions = cache.Ignite.GetTransactions();
            cache.Put(1, 1);
            cache.Put(2, 2);

            using (transactions.TxStart())
            {
                var local = transactions.GetLocalActiveTransactions().Single();

                cache.Put(1, 10);
                cache.Put(2, 20);

                if (async)
                {
                    local.RollbackAsync().Wait();
                }
                else
                {
                    local.Rollback();
                }

                Assert.AreEqual(1, cache.Get(1));
                Assert.AreEqual(2, cache.Get(2));
            }
            
            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
        }

        /// <summary>
        /// Test that Dispose does not end transaction.
        /// </summary>
        [Test]
        public void TestDisposeDoesNotEndTx()
        {
            var cache = Cache();
            var transactions = cache.Ignite.GetTransactions();
            cache.Put(1, 1);
            cache.Put(2, 2);

            using (transactions.TxStart())
            {
                var local = transactions.GetLocalActiveTransactions().Single();

                cache.Put(1, 10);
                cache.Put(2, 20);

                local.Dispose();

                cache.Put(2, 200);

                Assert.AreEqual(10, cache.Get(1));
                Assert.AreEqual(200, cache.Get(2));
            }

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));
        }

        /// <summary>
        /// Test that operation throws.
        /// </summary>
        [Test]
        public void TestUnsupportedOperationsThrow()
        {
            var cache = Cache();
            var transactions = cache.Ignite.GetTransactions();

            Action<object> dummy = obj => { };
            Action<ITransaction>[] actions =
            {
                t => t.Commit(),
                t => t.CommitAsync(),
                t => dummy(t.StartTime),
                t => dummy(t.ThreadId),
                t => t.AddMeta("test", "test"),
                t => t.Meta<string>("test"),
                t => t.RemoveMeta<string>("test"),
            };

            using (transactions.TxStart())
            {
                var local = transactions.GetLocalActiveTransactions().Single();
                var constraint = new ReusableConstraint(Is.TypeOf<InvalidOperationException>()
                    .And.Message.EqualTo("Operation is not supported by rollback only transaction."));
                foreach (var action in actions)
                {
                    Assert.Throws(constraint, () => action(local));
                }
            }
        }

        /// <summary>
        /// Tests that multiple rollback attempts will throw.
        /// </summary>
        [Test]
        public void TestMultipleRollbackThrows()
        {
            var cache = Cache();
            var transactions = cache.Ignite.GetTransactions();

            using (transactions.TxStart())
            {
                var local = (TransactionRollbackOnlyProxy)transactions.GetLocalActiveTransactions().Single();
                local.Rollback();
                var constraint = new ReusableConstraint(Is.TypeOf<InvalidOperationException>()
                    .And.Message.Contains("Transaction " + local.Id + " is closed"));
                Assert.Throws(constraint, () => local.Rollback());
                Assert.Throws(constraint, () => local.RollbackAsync().Wait());
            }
        }
        
        /// <summary>
        /// Gets cache.
        /// </summary>
        private ICache<int, int> Cache()
        {
            var ignite = Ignition.GetIgnite();
            return ignite.GetOrCreateCache<int, int>(new CacheConfiguration
            {
                Name = TestUtils.TestName,
                AtomicityMode = CacheAtomicityMode.Transactional
            });
        }
    }
}
