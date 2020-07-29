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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// 
    /// </summary>
    public class GetLocalActiveTransactionsTest : CacheAbstractTest
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
        public void TestRollbacks()
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

                local.Rollback();

                Assert.AreEqual(1, cache.Get(1));
                Assert.AreEqual(2, cache.Get(2));
            }
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
    }
}
