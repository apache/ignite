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
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Client.Transactions;
    using Apache.Ignite.Core.Impl.Client.Transactions;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;
    using NUnit.Framework.Constraints;

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

        /// <summary>
        /// Initializes a new instance of the <see cref="CacheClientAbstractTxTest"/> class.
        /// </summary>
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
                        var transactions = client.GetTransactions();
                        Assert.AreEqual(concurrency, transactions.DefaultTransactionConcurrency);
                        Assert.AreEqual(isolation, transactions.DefaultTransactionIsolation);
                        Assert.AreEqual(timeout, transactions.DefaultTimeout);

                        ITransaction igniteTx;
                        using (var tx = transactions.TxStart())
                        {
                            Assert.AreEqual(tx, transactions.Tx);
                            Assert.AreEqual(concurrency, tx.Concurrency);
                            Assert.AreEqual(isolation, tx.Isolation);
                            Assert.AreEqual(timeout, tx.Timeout);

                            igniteTx = GetSingleLocalTransaction();
                            Assert.AreEqual(concurrency, igniteTx.Concurrency);
                            Assert.AreEqual(isolation, igniteTx.Isolation);
                            Assert.AreEqual(timeout, igniteTx.Timeout);
                        }

                        igniteTx.Dispose();
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
                        var client = act();

                        ITransaction igniteTx;
                        using (var tx = client.TxStart(concurrency, isolation))
                        {
                            Assert.AreEqual(tx, client.Tx);
                            Assert.AreEqual(concurrency, tx.Concurrency);
                            Assert.AreEqual(isolation, tx.Isolation);

                            igniteTx = GetSingleLocalTransaction();
                            Assert.AreEqual(concurrency, igniteTx.Concurrency);
                            Assert.AreEqual(isolation, igniteTx.Isolation);
                        }

                        igniteTx.Dispose();
                        using (var tx = client.TxStart(concurrency, isolation, timeout))
                        {
                            Assert.AreEqual(concurrency, tx.Concurrency);
                            Assert.AreEqual(isolation, tx.Isolation);
                            Assert.AreEqual(timeout, tx.Timeout);

                            igniteTx = GetSingleLocalTransaction();
                            Assert.AreEqual(concurrency, igniteTx.Concurrency);
                            Assert.AreEqual(isolation, igniteTx.Isolation);
                            Assert.AreEqual(timeout, igniteTx.Timeout);
                        }

                        igniteTx.Dispose();
                    }
                }
            }
        }

        /// <summary>
        /// Tests that transaction can't be committed/rollback after being already completed.
        /// </summary>
        [Test]
        public void TestThrowsIfEndAlreadyCompletedTransaction()
        {
            var tx = Client.GetTransactions().TxStart();
            tx.Commit();

            var constraint = new ReusableConstraint(Is.TypeOf<InvalidOperationException>()
                .And.Message.Contains("Transaction")
                .And.Message.Contains("is closed"));

            Assert.Throws(constraint, () => tx.Commit());
            Assert.Throws(constraint, () => tx.Rollback());

            using (tx = Client.GetTransactions().TxStart())
            {
            }

            Assert.Throws(constraint, () => tx.Commit());
            Assert.Throws(constraint, () => tx.Rollback());
        }

        /// <summary>
        /// Tests that transaction throws if timeout elapsed.
        /// </summary>
        [Test]
        public void TestTimeout()
        {
            var timeout = TimeSpan.FromMilliseconds(200);
            var cache = GetTransactionalCache();
            cache.Put(1, 1);
            using (var tx = Client.GetTransactions().TxStart(TransactionConcurrency.Pessimistic,
                TransactionIsolation.ReadCommitted,
                timeout))
            {
                Thread.Sleep(TimeSpan.FromMilliseconds(300));
                var constraint = new ReusableConstraint(Is.TypeOf<IgniteClientException>()
                    .And.Message.Contains("Cache transaction timed out"));
                Assert.Throws(constraint, () => cache.Put(1, 10));
                Assert.Throws(constraint, () => tx.Commit());
            }

            Assert.AreEqual(1, cache.Get(1));
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
            var client = Client;
            var cache = GetTransactionalCache(client);
            cache[1] = 1;
            cache[2] = 2;

            var anotherClient = GetClient();
            var anotherCache = GetTransactionalCache(anotherClient);
            var concurrency = TransactionConcurrency.Optimistic;
            var isolation = TransactionIsolation.ReadCommitted;
            using (var tx = client.GetTransactions().TxStart(concurrency, isolation))
            {
                cache[1] = 10;
                using (var anotherTx = anotherClient.GetTransactions().TxStart(concurrency, isolation))
                {
                    Assert.AreNotSame(tx, anotherTx);
                    Assert.AreSame(tx, client.GetTransactions().Tx);
                    Assert.AreSame(anotherTx, anotherClient.GetTransactions().Tx);

                    Assert.AreEqual(10, cache[1]);
                    Assert.AreEqual(1, anotherCache[1]);

                    anotherCache[2] = 20;

                    Assert.AreEqual(2, cache[2]);
                    Assert.AreEqual(20, anotherCache[2]);

                    anotherTx.Commit();

                    Assert.AreEqual(20, cache[2]);
                }
            }

            Assert.AreEqual(1, cache[1]);
            Assert.AreEqual(20, cache[2]);
            Assert.AreEqual(1, anotherCache[1]);
            Assert.AreEqual(20, anotherCache[2]);
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

            ITransaction igniteTx;
            using (Client.GetTransactions().WithLabel(label1).TxStart())
            {
                igniteTx = GetSingleLocalTransaction();

                Assert.AreEqual(igniteTx.Label, label1);

                cache.Put(1, 10);
                cache.Put(2, 20);
            }

            igniteTx.Dispose();

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));

            using (var tx = Client.GetTransactions().WithLabel(label1).TxStart())
            {
                igniteTx = GetSingleLocalTransaction();

                Assert.AreEqual(label1, igniteTx.Label);
                Assert.AreEqual(label1, tx.Label);

                cache.Put(1, 10);
                cache.Put(2, 20);
                tx.Commit();
            }

            igniteTx.Dispose();

            Assert.AreEqual(10, cache.Get(1));
            Assert.AreEqual(20, cache.Get(2));

            using (var tx = Client.GetTransactions().WithLabel(label1).WithLabel(label2).TxStart())
            {
                igniteTx = GetSingleLocalTransaction();

                Assert.AreEqual(label2, igniteTx.Label);
                Assert.AreEqual(label2, tx.Label);
            }

            igniteTx.Dispose();

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
        /// Tests that unfinished transaction does not prevent <see cref="IIgniteClient"/>>
        /// from being garbage collected.
        /// </summary>
        [Test]
        public void TestFinalizesAfterClientIsDisposed()
        {
            ConcurrentBag<WeakReference> weakRef = new ConcurrentBag<WeakReference>();
            Action<Action<IIgniteClient>> act = startTx =>
            {
                var client = GetClient();
                weakRef.Add(new WeakReference(client));
                var cache = GetTransactionalCache(client);
                startTx(client);
                cache[42] = 42;

                client.Dispose();
            };

            Action<IIgniteClient>[] txStarts =
            {
                // ReSharper disable once ObjectCreationAsStatement
                client => new TransactionScope(),
                client => client.GetTransactions().TxStart()
            };

            foreach (var txStart in txStarts)
            {
                // ReSharper disable once AccessToForEachVariableInClosure
                var tasks = Enumerable.Range(0, 3)
                    .Select(i => Task.Factory.StartNew(() => act(txStart),TaskCreationOptions.LongRunning))
                    .ToArray();

                Task.WaitAll(tasks);

                GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
                GC.WaitForPendingFinalizers();

                Assert.IsFalse(weakRef.All(wr => wr.IsAlive));
            }
        }

        /// <summary>
        /// Test that GC does not close <see cref="TransactionScope"/>'s underlying transaction.
        /// </summary>
        [Test]
        public void TestGcDoesNotCloseAmbientTx()
        {
            WeakReference weakRef = null;
            Func<TransactionScope> act = () =>
            {
                var innerScope = new TransactionScope();
                GetTransactionalCache()[42] = 42;
                weakRef = new WeakReference(Client.GetTransactions().Tx);
                return innerScope;
            };

            using (act())
            {
                GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced);
                GC.WaitForPendingFinalizers();
                Assert.IsTrue(weakRef.IsAlive);
                var tx = (ITransactionClient) weakRef.Target;
                Assert.IsNotNull(Client.GetTransactions().Tx);
                Assert.AreSame(tx, Client.GetTransactions().Tx);
            }
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
                } // Revert transaction scope.

                tx.Commit(); // Commit manual tx.
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
            var transactions = (TransactionsClient) Client.GetTransactions();

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
                ITransaction tx;
                using (new TransactionScope(TransactionScopeOption.Required, new TransactionOptions
                {
                    IsolationLevel = mode.Item1
                }))
                {
                    cache[1] = 1;

                    tx = GetSingleLocalTransaction();
                    Assert.AreEqual(mode.Item2, tx.Isolation);
                    Assert.AreEqual(transactions.DefaultTransactionConcurrency, tx.Concurrency);
                }

                tx.Dispose();
            }
        }

        /// <summary>
        /// Tests all synchronous transactional operations with <see cref="TransactionScope"/>.
        /// </summary>
        [Test]
        public void TestTransactionScopeAllOperationsSync()
        {
            CheckTxOp((cache, key) => cache.Put(key, -5));

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
        /// Tests that read operations lock keys in Serializable mode.
        /// </summary>
        [Test]
        public void TestTransactionScopeWithSerializableIsolationLocksKeysOnRead()
        {
            Action<Func<ICacheClient<int, int>, int, int>>
                test = TestTransactionScopeWithSerializableIsolationLocksKeysOnRead;

            test((cache, key) => cache[key]);
            test((cache, key) => cache.Get(key));
            test((cache, key) =>
            {
                int val;
                return cache.TryGet(key, out val) ? val : 0;
            });
            test((cache, key) => cache.GetAll(new[] {key}).Single().Value);
        }

        /// <summary>
        /// Tests that async read operations lock keys in Serializable mode.
        /// </summary>
        [Test]
        [Ignore("Async thin client transactional operations not supported.")]
        public void TestTransactionScopeWithSerializableIsolationLocksKeysOnReadAsync()
        {
            Action<Func<ICacheClient<int, int>, int, int>>
                test = TestTransactionScopeWithSerializableIsolationLocksKeysOnRead;

            test((cache, key) => cache.GetAsync(key).Result);
            test((cache, key) => cache.TryGetAsync(key).Result.Value);
            test((cache, key) => cache.GetAll(new[] {key}).Single().Value);
        }

        /// <summary>
        /// Tests that read operations lock keys in Serializable mode.
        /// </summary>
        private void TestTransactionScopeWithSerializableIsolationLocksKeysOnRead(
            Func<ICacheClient<int, int>, int, int> readOp)
        {
            var cache = GetTransactionalCache();
            cache.Put(1, 1);

            var options = new TransactionOptions {IsolationLevel = IsolationLevel.Serializable};

            using (var scope = new TransactionScope(TransactionScopeOption.Required, options))
            {
                Assert.AreEqual(1, readOp(cache, 1));
                Assert.IsNotNull(Client.GetTransactions().Tx);

                var evt = new ManualResetEventSlim();

                var task = Task.Factory.StartNew(() =>
                {
                    cache.PutAsync(1, 2);
                    evt.Set();
                });

                evt.Wait();

                Assert.AreEqual(1, readOp(cache, 1));

                scope.Complete();
                task.Wait();
            }

            TestUtils.WaitForTrueCondition(() => 2 == readOp(cache, 1));
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

                    Assert.IsNotNull(Client.GetTransactions().Tx, "Transaction has not started.");
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
            Assert.Throws(
                Is.TypeOf<IgniteClientException>()
                    .And.Message.Contains("A transaction has already been started by the current thread."),
                () =>
                {
                    using (outer())
                    using (inner())
                    {
                        // No-op.
                    }
                });
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
            return GetTransactionalCache(Client, cacheName);
        }

        /// <summary>
        /// Gets or creates transactional cache
        /// </summary>
        private ICacheClient<int, int> GetTransactionalCache(IIgniteClient client, string cacheName = null)
        {
            return client.GetOrCreateCache<int, int>(new CacheClientConfiguration
            {
                Name = cacheName ?? GetCacheName(),
                AtomicityMode = CacheAtomicityMode.Transactional
            });
        }
    }
}
