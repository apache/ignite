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
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.Transactions;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Client.Transactions;
    using Apache.Ignite.Core.Impl.Client.Transactions;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Transactional cache client tests.
    /// </summary>
    public abstract class CacheClientAbstractTxTest : ClientTestBase
    {
        /** All concurrency controls. */
        private static readonly TransactionConcurrency[] AllConcurrencyControls =
        {
            TransactionConcurrency.Optimistic, 
            TransactionConcurrency.Pessimistic
        };

        /** All isolation levels*/
        private static readonly TransactionIsolation[] AllIsolationLevels = 
        {
            TransactionIsolation.Serializable,
            TransactionIsolation.ReadCommitted,
            TransactionIsolation.RepeatableRead
        };

        protected CacheClientAbstractTxTest(int serverCount, bool enablePartitionAwareness) : base(serverCount,
            enablePartitionAwareness: enablePartitionAwareness)
        {
            // No-op.
        }

        /// <summary>
        /// Tests that custom client transactions configuration is applied.
        /// </summary>
        [Test]
        public void TestClientTransactionConfiguration()
        {
            var timeout = TransactionClientConfiguration.DefaultDefaultTimeout.Add(TimeSpan.FromMilliseconds(1000));
            var cfg = GetClientConfiguration();
            cfg.TransactionConfiguration = new TransactionClientConfiguration
            {
                DefaultTimeout = timeout
            };

            foreach (var concurrency in AllConcurrencyControls)
            {
                foreach (var isolation in AllIsolationLevels)
                {
                    cfg.TransactionConfiguration.DefaultTransactionConcurrency = concurrency;
                    cfg.TransactionConfiguration.DefaultTransactionIsolation = isolation;
                    using (var client = Ignition.StartClient(cfg))
                    {
                        using (client.GetTransactions().TxStart())
                        {
                            var tx = GetSingleLocalTransaction();
                            Assert.AreEqual(concurrency, tx.Concurrency);
                            Assert.AreEqual(isolation, tx.Isolation);
                            Assert.AreEqual(timeout, tx.Timeout);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Tests that parameters passed to TxStart are applied.
        /// </summary>
        [Test]
        public void TestTxStartPassesParameters()
        {
            var timeout = TransactionClientConfiguration.DefaultDefaultTimeout.Add(TimeSpan.FromMilliseconds(1000));
            var acts = new List<Func<ITransactionsClient>>
            {
                () => Client.GetTransactions(),
                () => Client.GetTransactions().WithLabel("label"),
            };
            foreach (var concurrency in AllConcurrencyControls)
            {
                foreach (var isolation in AllIsolationLevels)
                {
                    foreach (var act in acts)
                    {
                        using (act().TxStart(concurrency, isolation))
                        {
                            var tx = GetSingleLocalTransaction();
                            Assert.AreEqual(concurrency, tx.Concurrency);
                            Assert.AreEqual(isolation, tx.Isolation);
                        }
                        using (act().TxStart(concurrency, isolation, timeout))
                        {
                            var tx = GetSingleLocalTransaction();
                            Assert.AreEqual(concurrency, tx.Concurrency);
                            Assert.AreEqual(isolation, tx.Isolation);
                            Assert.AreEqual(timeout, tx.Timeout);
                        }
                    }
                }
            }
        }

        /// <summary>
        /// Tests that commit applies cache changes.
        /// </summary>
        [Test]
        public void TestTxCommit()
        {
            var cache = GetTransactionalCache();

            cache.Put(1, 1);
            cache.Put(2, 2);

            using (var tx = Client.GetTransactions().TxStart())
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
            var cache = GetTransactionalCache();
            cache.Put(1, 1);
            cache.Put(2, 2);

            using (var tx = Client.GetTransactions().TxStart())
            {
                cache.Put(1, 10);
                cache.Put(2, 20);

                Assert.AreEqual(10, cache.Get(1));
                Assert.AreEqual(20, cache.Get(2));
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
            var cache = GetTransactionalCache();

            cache.Put(1, 1);
            cache.Put(2, 2);

            using (Client.GetTransactions().TxStart())
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
            TestThrowsIfMultipleStarted(
                () => Client.GetTransactions().TxStart(),
                () => Client.GetTransactions().TxStart());
        }

        /// <summary>
        /// Tests that different clients can start transactions in one thread.
        /// </summary>
        [Test]
        public void TestDifferentClientsCanStartTransactions()
        {
            Assert.DoesNotThrow(() =>
            {
                using (Client.GetTransactions().TxStart())
                using (GetClient().GetTransactions().TxStart())
                {
                    // No-op.
                }
            });
        }

        /// <summary>
        /// Test Ignite thin client transaction with label.
        /// </summary>
        [Test]
        public void TestWithLabel()
        {
            const string label1 = "label1";
            const string label2 = "label2";

            var cache = GetTransactionalCache();
            cache.Put(1, 1);
            cache.Put(2, 2);

            using (Client.GetTransactions().WithLabel(label1).TxStart())
            {
                var igniteTx = GetSingleLocalTransaction();

                Assert.AreEqual(igniteTx.Label, label1);

                cache.Put(1, 10);
                cache.Put(2, 20);
            }

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));

            using (var tx = Client.GetTransactions().WithLabel(label1).TxStart())
            {
                var igniteTx = GetSingleLocalTransaction();

                Assert.AreEqual(igniteTx.Label, label1);

                cache.Put(1, 10);
                cache.Put(2, 20);
                tx.Commit();
            }

            Assert.AreEqual(10, cache.Get(1));
            Assert.AreEqual(20, cache.Get(2));

            using (Client.GetTransactions().WithLabel(label1).WithLabel(label2).TxStart())
            {
                var tx = GetSingleLocalTransaction();

                Assert.AreEqual(tx.Label, label2);
            }

            TestThrowsIfMultipleStarted(
                () => Client.GetTransactions().WithLabel(label1).TxStart(),
                () => Client.GetTransactions().TxStart());

            TestThrowsIfMultipleStarted(
                () => Client.GetTransactions().TxStart(),
                () => Client.GetTransactions().WithLabel(label1).TxStart());

            TestThrowsIfMultipleStarted(
                () => Client.GetTransactions().WithLabel(label1).TxStart(),
                () => Client.GetTransactions().WithLabel(label2).TxStart());
        }

        /// <summary>
        /// Test Ignite thin client transaction enlistment in ambient <see cref="TransactionScope"/>.
        /// </summary>
        [Test]
        public void TestTransactionScopeSingleCache()
        {
            var cache = GetTransactionalCache();

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
            var cache1 = GetTransactionalCache();
            var cache2 = GetTransactionalCache(cache1.Name + "1");

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
            var cache = GetTransactionalCache();
            var transactions = Client.GetTransactions();

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
            var cache = GetTransactionalCache();

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
            var cache = GetTransactionalCache();

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
        /// Test that ambient <see cref="TransactionScope"/> options propagate to Ignite transaction.
        /// </summary>
        [Test]
        public void TestTransactionScopeOptions()
        {
            var cache = GetTransactionalCache();
            var transactions = (ITransactionsClientInternal) Client.GetTransactions();

            var modes = new[]
            {
                Tuple.Create(IsolationLevel.Serializable, TransactionIsolation.Serializable),
                Tuple.Create(IsolationLevel.RepeatableRead, TransactionIsolation.RepeatableRead),
                Tuple.Create(IsolationLevel.ReadCommitted, TransactionIsolation.ReadCommitted),
                Tuple.Create(IsolationLevel.ReadUncommitted, TransactionIsolation.ReadCommitted),
                Tuple.Create(IsolationLevel.Snapshot, TransactionIsolation.ReadCommitted),
                Tuple.Create(IsolationLevel.Chaos, TransactionIsolation.ReadCommitted),
            };

            foreach (var mode in modes)
            {
                using (new TransactionScope(TransactionScopeOption.Required, new TransactionOptions
                {
                    IsolationLevel = mode.Item1
                }))
                {
                    cache[1] = 1;

                    var tx = GetSingleLocalTransaction();
                    Assert.AreEqual(mode.Item2, tx.Isolation);
                    Assert.AreEqual(transactions.DefaultTxConcurrency, tx.Concurrency);
                }
            }
        }

        /// <summary>
        /// Tests all synchronous transactional operations with <see cref="TransactionScope"/>.
        /// </summary>
        [Test]
        public void TestTransactionScopeAllOperationsSync()
        {
            CheckTxOp((cache, key) => cache.Put(key, -5));

            CheckReadTxOp((cache, key) => cache.Get(key));
            
            CheckReadTxOp((cache, key) =>
            {
                int value;
                Assert.IsTrue(cache.TryGet(key, out value));
            });

            CheckReadTxOp((cache, key) => cache.ContainsKey(key));

            CheckReadTxOp((cache, key) => cache.ContainsKeys(new[] {key}));

            CheckTxOp((cache, key) => cache.PutAll(new Dictionary<int, int> {{key, -7}}));

            CheckTxOp((cache, key) =>
            {
                cache.Remove(key);
                cache.PutIfAbsent(key, -10);
            });

            CheckTxOp((cache, key) => cache.GetAndPut(key, -9));

            CheckTxOp((cache, key) =>
            {
                cache.Remove(key);
                cache.GetAndPutIfAbsent(key, -10);
            });

            CheckTxOp((cache, key) => cache.GetAndRemove(key));

            CheckTxOp((cache, key) => cache.GetAndReplace(key, -11));

            CheckTxOp((cache, key) => cache.Remove(key));

            CheckTxOp((cache, key) => cache.RemoveAll(new[] {key}));

            CheckTxOp((cache, key) => cache.Replace(key, 100));

            CheckTxOp((cache, key) => cache.Replace(key, cache[key], 100));
        }

        /// <summary>
        /// Tests all transactional async operations with <see cref="TransactionScope"/>.
        /// </summary>
        [Test]
        [Ignore("Async thin client transactional operations not supported.")]
        public void TestTransactionScopeAllOperationsAsync()
        {
            CheckTxOp((cache, key) => cache.PutAsync(key, -5));
            CheckTxOp((cache, key) => cache.PutAllAsync(new Dictionary<int, int> {{key, -7}}));

            CheckTxOp((cache, key) =>
            {
                cache.Remove(key);
                cache.PutIfAbsentAsync(key, -10);
            });

            CheckTxOp((cache, key) => cache.GetAndPutAsync(key, -9));

            CheckTxOp((cache, key) =>
            {
                cache.Remove(key);
                cache.GetAndPutIfAbsentAsync(key, -10);
            });

            CheckTxOp((cache, key) => cache.GetAndRemoveAsync(key));

            CheckTxOp((cache, key) => cache.GetAndReplaceAsync(key, -11));

            CheckTxOp((cache, key) => cache.RemoveAsync(key));

            CheckTxOp((cache, key) => cache.RemoveAllAsync(new[] {key}));

            CheckTxOp((cache, key) => cache.ReplaceAsync(key, 100));

            CheckTxOp((cache, key) => cache.ReplaceAsync(key, cache[key], 100));
        }

        /// <summary>
        /// Checks that read cache operation starts ambient transaction.
        /// </summary>
        private void CheckReadTxOp(Action<ICacheClient<int, int>, int> act)
        {
            var txOpts = new TransactionOptions {IsolationLevel = IsolationLevel.RepeatableRead};
            const TransactionScopeOption scope = TransactionScopeOption.Required;

            var cache = GetTransactionalCache();
            cache[1] = 1;

            // Rollback.
            using (new TransactionScope(scope, txOpts))
            {
                act(cache, 1);

                Assert.IsNotNull(((ITransactionsClientInternal) Client.GetTransactions()).CurrentTx,
                    "Transaction has not started.");
            }
        }

        /// <summary>
        /// Checks that cache operation behaves transactionally.
        /// </summary>
        private void CheckTxOp(Action<ICacheClient<int, int>, int> act)
        {
            var isolationLevels = new[]
            {
                IsolationLevel.Serializable, IsolationLevel.RepeatableRead, IsolationLevel.ReadCommitted,
                IsolationLevel.ReadUncommitted, IsolationLevel.Snapshot, IsolationLevel.Chaos
            };

            foreach (var isolationLevel in isolationLevels)
            {
                var txOpts = new TransactionOptions {IsolationLevel = isolationLevel};
                const TransactionScopeOption scope = TransactionScopeOption.Required;

                var cache = GetTransactionalCache();

                cache[1] = 1;
                cache[2] = 2;

                // Rollback.
                using (new TransactionScope(scope, txOpts))
                {
                    act(cache, 1);

                    Assert.IsNotNull(((ITransactionsClientInternal)Client.GetTransactions()).CurrentTx,
                        "Transaction has not started.");
                }

                Assert.AreEqual(1, cache[1]);
                Assert.AreEqual(2, cache[2]);

                using (new TransactionScope(scope, txOpts))
                {
                    act(cache, 1);
                    act(cache, 2);
                }

                Assert.AreEqual(1, cache[1]);
                Assert.AreEqual(2, cache[2]);

                // Commit.
                using (var ts = new TransactionScope(scope, txOpts))
                {
                    act(cache, 1);
                    ts.Complete();
                }

                Assert.IsTrue(!cache.ContainsKey(1) || cache[1] != 1);
                Assert.AreEqual(2, cache[2]);

                using (var ts = new TransactionScope(scope, txOpts))
                {
                    act(cache, 1);
                    act(cache, 2);
                    ts.Complete();
                }

                Assert.IsTrue(!cache.ContainsKey(1) || cache[1] != 1);
                Assert.IsTrue(!cache.ContainsKey(2) || cache[2] != 2);
            }
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

        /// <summary>
        /// Tests that client can't start multiple transactions in one thread.
        /// </summary>
        private void TestThrowsIfMultipleStarted(Func<IDisposable> outer, Func<IDisposable> inner)
        {
            Assert.Throws<IgniteClientException>(() =>
                {
                    using (outer())
                    using (inner())
                    {
                        // No-op.
                    }
                },
                "A transaction has already been started by the current thread.");
        }

        /// <summary>
        /// Gets cache name.
        /// </summary>
        protected virtual string GetCacheName()
        {
            return "client_transactional";
        }

        /// <summary>
        /// Gets or creates transactional cache
        /// </summary>
        protected ICacheClient<int, int> GetTransactionalCache(string cacheName = null)
        {
            return Client.GetOrCreateCache<int, int>(new CacheClientConfiguration
            {
                Name = cacheName ?? GetCacheName(),
                AtomicityMode = CacheAtomicityMode.Transactional
            });
        }
    }
}
