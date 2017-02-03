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
    using System.Collections.Generic;
    using System.Linq;
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Transactions;
    using NUnit.Framework;

    /// <summary>
    /// Transactional cache tests.
    /// </summary>
    public abstract class CacheAbstractTransactionalTest : CacheAbstractTest
    {
        /// <summary>
        /// Simple cache lock test (while <see cref="TestLock"/> is ignored).
        /// </summary>
        [Test]
        public void TestLockSimple()
        {
            var cache = Cache();

            const int key = 7;

            Action<ICacheLock> checkLock = lck =>
            {
                using (lck)
                {
                    Assert.Throws<InvalidOperationException>(lck.Exit); // can't exit if not entered

                    lck.Enter();

                    Assert.IsTrue(cache.IsLocalLocked(key, true));
                    Assert.IsTrue(cache.IsLocalLocked(key, false));

                    lck.Exit();

                    Assert.IsFalse(cache.IsLocalLocked(key, true));
                    Assert.IsFalse(cache.IsLocalLocked(key, false));

                    Assert.IsTrue(lck.TryEnter());

                    Assert.IsTrue(cache.IsLocalLocked(key, true));
                    Assert.IsTrue(cache.IsLocalLocked(key, false));

                    lck.Exit();
                }

                Assert.Throws<ObjectDisposedException>(lck.Enter); // Can't enter disposed lock
            };

            checkLock(cache.Lock(key));
            checkLock(cache.LockAll(new[] { key, 1, 2, 3 }));
        }

        /// <summary>
        /// Tests cache locks.
        /// </summary>
        [Test]
        [Ignore("IGNITE-835")]
        public void TestLock()
        {
            var cache = Cache();

            const int key = 7;

            // Lock
            CheckLock(cache, key, () => cache.Lock(key));

            // LockAll
            CheckLock(cache, key, () => cache.LockAll(new[] { key, 2, 3, 4, 5 }));
        }

        /// <summary>
        /// Internal lock test routine.
        /// </summary>
        /// <param name="cache">Cache.</param>
        /// <param name="key">Key.</param>
        /// <param name="getLock">Function to get the lock.</param>
        private static void CheckLock(ICache<int, int> cache, int key, Func<ICacheLock> getLock)
        {
            var sharedLock = getLock();

            using (sharedLock)
            {
                Assert.Throws<InvalidOperationException>(() => sharedLock.Exit());  // can't exit if not entered

                sharedLock.Enter();

                try
                {
                    Assert.IsTrue(cache.IsLocalLocked(key, true));
                    Assert.IsTrue(cache.IsLocalLocked(key, false));

                    EnsureCannotLock(getLock, sharedLock);

                    sharedLock.Enter();

                    try
                    {
                        Assert.IsTrue(cache.IsLocalLocked(key, true));
                        Assert.IsTrue(cache.IsLocalLocked(key, false));

                        EnsureCannotLock(getLock, sharedLock);
                    }
                    finally
                    {
                        sharedLock.Exit();
                    }

                    Assert.IsTrue(cache.IsLocalLocked(key, true));
                    Assert.IsTrue(cache.IsLocalLocked(key, false));

                    EnsureCannotLock(getLock, sharedLock);

                    Assert.Throws<SynchronizationLockException>(() => sharedLock.Dispose()); // can't dispose while locked
                }
                finally
                {
                    sharedLock.Exit();
                }

                Assert.IsFalse(cache.IsLocalLocked(key, true));
                Assert.IsFalse(cache.IsLocalLocked(key, false));

                var innerTask = new Task(() =>
                {
                    Assert.IsTrue(sharedLock.TryEnter());
                    sharedLock.Exit();

                    using (var otherLock = getLock())
                    {
                        Assert.IsTrue(otherLock.TryEnter());
                        otherLock.Exit();
                    }
                });

                innerTask.Start();
                innerTask.Wait();
            }

            Assert.IsFalse(cache.IsLocalLocked(key, true));
            Assert.IsFalse(cache.IsLocalLocked(key, false));

            var outerTask = new Task(() =>
            {
                using (var otherLock = getLock())
                {
                    Assert.IsTrue(otherLock.TryEnter());
                    otherLock.Exit();
                }
            });

            outerTask.Start();
            outerTask.Wait();

            Assert.Throws<ObjectDisposedException>(() => sharedLock.Enter());  // Can't enter disposed lock
        }

        /// <summary>
        /// Ensure that lock cannot be obtained by other threads.
        /// </summary>
        /// <param name="getLock">Get lock function.</param>
        /// <param name="sharedLock">Shared lock.</param>
        private static void EnsureCannotLock(Func<ICacheLock> getLock, ICacheLock sharedLock)
        {
            var task = new Task(() =>
            {
                Assert.IsFalse(sharedLock.TryEnter());
                Assert.IsFalse(sharedLock.TryEnter(TimeSpan.FromMilliseconds(100)));

                using (var otherLock = getLock())
                {
                    Assert.IsFalse(otherLock.TryEnter());
                    Assert.IsFalse(otherLock.TryEnter(TimeSpan.FromMilliseconds(100)));
                }
            });

            task.Start();
            task.Wait();
        }

        /// <summary>
        /// Tests that commit applies cache changes.
        /// </summary>
        [Test]
        public void TestTxCommit([Values(true, false)] bool async)
        {
            var cache = Cache();

            Assert.IsNull(Transactions.Tx);

            using (var tx = Transactions.TxStart())
            {
                cache.Put(1, 1);
                cache.Put(2, 2);

                if (async)
                {
                    var task = tx.CommitAsync();

                    task.Wait();

                    Assert.IsTrue(task.IsCompleted);
                }
                else
                    tx.Commit();
            }

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));

            Assert.IsNull(Transactions.Tx);
        }

        /// <summary>
        /// Tests that rollback reverts cache changes.
        /// </summary>
        [Test]
        public void TestTxRollback()
        {
            var cache = Cache();

            cache.Put(1, 1);
            cache.Put(2, 2);

            Assert.IsNull(Transactions.Tx);

            using (var tx = Transactions.TxStart())
            {
                cache.Put(1, 10);
                cache.Put(2, 20);

                tx.Rollback();
            }

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));

            Assert.IsNull(Transactions.Tx);
        }

        /// <summary>
        /// Tests that Dispose without Commit reverts changes.
        /// </summary>
        [Test]
        public void TestTxClose()
        {
            var cache = Cache();

            cache.Put(1, 1);
            cache.Put(2, 2);

            Assert.IsNull(Transactions.Tx);

            using (Transactions.TxStart())
            {
                cache.Put(1, 10);
                cache.Put(2, 20);
            }

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));

            Assert.IsNull(Transactions.Tx);
        }

        /// <summary>
        /// Tests all concurrency and isolation modes with and without timeout.
        /// </summary>
        [Test]
        public void TestTxAllModes([Values(true, false)] bool withTimeout)
        {
            var cache = Cache();

            int cntr = 0;

            foreach (TransactionConcurrency concurrency in Enum.GetValues(typeof(TransactionConcurrency)))
            {
                foreach (TransactionIsolation isolation in Enum.GetValues(typeof(TransactionIsolation)))
                {
                    Console.WriteLine("Test tx [concurrency=" + concurrency + ", isolation=" + isolation + "]");

                    Assert.IsNull(Transactions.Tx);

                    using (var tx = withTimeout
                        ? Transactions.TxStart(concurrency, isolation, TimeSpan.FromMilliseconds(1100), 10)
                        : Transactions.TxStart(concurrency, isolation))
                    {

                        Assert.AreEqual(concurrency, tx.Concurrency);
                        Assert.AreEqual(isolation, tx.Isolation);

                        if (withTimeout)
                            Assert.AreEqual(1100, tx.Timeout.TotalMilliseconds);

                        cache.Put(1, cntr);

                        tx.Commit();
                    }

                    Assert.IsNull(Transactions.Tx);

                    Assert.AreEqual(cntr, cache.Get(1));

                    cntr++;
                }
            }
        }

        /// <summary>
        /// Tests that transaction properties are applied and propagated properly.
        /// </summary>
        [Test]
        public void TestTxAttributes()
        {
            ITransaction tx = Transactions.TxStart(TransactionConcurrency.Optimistic,
                TransactionIsolation.RepeatableRead, TimeSpan.FromMilliseconds(2500), 100);

            Assert.IsFalse(tx.IsRollbackOnly);
            Assert.AreEqual(TransactionConcurrency.Optimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.RepeatableRead, tx.Isolation);
            Assert.AreEqual(2500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(TransactionState.Active, tx.State);
            Assert.IsTrue(tx.StartTime.Ticks > 0);
            Assert.AreEqual(tx.NodeId, GetIgnite(0).GetCluster().GetLocalNode().Id);

            DateTime startTime1 = tx.StartTime;

            tx.Commit();

            Assert.IsFalse(tx.IsRollbackOnly);
            Assert.AreEqual(TransactionState.Committed, tx.State);
            Assert.AreEqual(TransactionConcurrency.Optimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.RepeatableRead, tx.Isolation);
            Assert.AreEqual(2500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(startTime1, tx.StartTime);

            Thread.Sleep(100);

            tx = Transactions.TxStart(TransactionConcurrency.Pessimistic, TransactionIsolation.ReadCommitted,
                TimeSpan.FromMilliseconds(3500), 200);

            Assert.IsFalse(tx.IsRollbackOnly);
            Assert.AreEqual(TransactionConcurrency.Pessimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.ReadCommitted, tx.Isolation);
            Assert.AreEqual(3500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(TransactionState.Active, tx.State);
            Assert.IsTrue(tx.StartTime.Ticks > 0);
            Assert.IsTrue(tx.StartTime > startTime1);

            DateTime startTime2 = tx.StartTime;

            tx.Rollback();

            Assert.AreEqual(TransactionState.RolledBack, tx.State);
            Assert.AreEqual(TransactionConcurrency.Pessimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.ReadCommitted, tx.Isolation);
            Assert.AreEqual(3500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(startTime2, tx.StartTime);

            Thread.Sleep(100);

            tx = Transactions.TxStart(TransactionConcurrency.Optimistic, TransactionIsolation.RepeatableRead,
                TimeSpan.FromMilliseconds(2500), 100);

            Assert.IsFalse(tx.IsRollbackOnly);
            Assert.AreEqual(TransactionConcurrency.Optimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.RepeatableRead, tx.Isolation);
            Assert.AreEqual(2500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(TransactionState.Active, tx.State);
            Assert.IsTrue(tx.StartTime > startTime2);

            DateTime startTime3 = tx.StartTime;

            tx.Commit();

            Assert.IsFalse(tx.IsRollbackOnly);
            Assert.AreEqual(TransactionState.Committed, tx.State);
            Assert.AreEqual(TransactionConcurrency.Optimistic, tx.Concurrency);
            Assert.AreEqual(TransactionIsolation.RepeatableRead, tx.Isolation);
            Assert.AreEqual(2500, tx.Timeout.TotalMilliseconds);
            Assert.AreEqual(startTime3, tx.StartTime);

            // Check defaults.
            tx = Transactions.TxStart();

            Assert.AreEqual(Transactions.DefaultTransactionConcurrency, tx.Concurrency);
            Assert.AreEqual(Transactions.DefaultTransactionIsolation, tx.Isolation);
            Assert.AreEqual(Transactions.DefaultTimeout, tx.Timeout);

            tx.Commit();
        }

        /// <summary>
        /// Tests <see cref="ITransaction.IsRollbackOnly"/> flag.
        /// </summary>
        [Test]
        public void TestTxRollbackOnly()
        {
            var cache = Cache();

            cache.Put(1, 1);
            cache.Put(2, 2);

            var tx = Transactions.TxStart();

            cache.Put(1, 10);
            cache.Put(2, 20);

            Assert.IsFalse(tx.IsRollbackOnly);

            tx.SetRollbackonly();

            Assert.IsTrue(tx.IsRollbackOnly);

            Assert.AreEqual(TransactionState.MarkedRollback, tx.State);

            var ex = Assert.Throws<TransactionRollbackException>(() => tx.Commit());
            Assert.IsTrue(ex.Message.StartsWith("Invalid transaction state for prepare [state=MARKED_ROLLBACK"));

            tx.Dispose();

            Assert.AreEqual(TransactionState.RolledBack, tx.State);

            Assert.IsTrue(tx.IsRollbackOnly);

            Assert.AreEqual(1, cache.Get(1));
            Assert.AreEqual(2, cache.Get(2));

            Assert.IsNull(Transactions.Tx);
        }

        /// <summary>
        /// Tests transaction metrics.
        /// </summary>
        [Test]
        public void TestTxMetrics()
        {
            var cache = Cache();

            var startTime = DateTime.UtcNow.AddSeconds(-1);

            Transactions.ResetMetrics();

            var metrics = Transactions.GetMetrics();

            Assert.AreEqual(0, metrics.TxCommits);
            Assert.AreEqual(0, metrics.TxRollbacks);

            using (Transactions.TxStart())
            {
                cache.Put(1, 1);
            }

            using (var tx = Transactions.TxStart())
            {
                cache.Put(1, 1);
                tx.Commit();
            }

            metrics = Transactions.GetMetrics();

            Assert.AreEqual(1, metrics.TxCommits);
            Assert.AreEqual(1, metrics.TxRollbacks);

            Assert.LessOrEqual(startTime, metrics.CommitTime);
            Assert.LessOrEqual(startTime, metrics.RollbackTime);

            Assert.GreaterOrEqual(DateTime.UtcNow, metrics.CommitTime);
            Assert.GreaterOrEqual(DateTime.UtcNow, metrics.RollbackTime);
        }

        /// <summary>
        /// Tests transaction state transitions.
        /// </summary>
        [Test]
        public void TestTxStateAndExceptions()
        {
            var tx = Transactions.TxStart();

            Assert.AreEqual(TransactionState.Active, tx.State);
            Assert.AreEqual(Thread.CurrentThread.ManagedThreadId, tx.ThreadId);

            tx.AddMeta("myMeta", 42);
            Assert.AreEqual(42, tx.Meta<int>("myMeta"));
            Assert.AreEqual(42, tx.RemoveMeta<int>("myMeta"));

            tx.RollbackAsync().Wait();

            Assert.AreEqual(TransactionState.RolledBack, tx.State);

            Assert.Throws<InvalidOperationException>(() => tx.Commit());

            tx = Transactions.TxStart();

            Assert.AreEqual(TransactionState.Active, tx.State);

            tx.CommitAsync().Wait();

            Assert.AreEqual(TransactionState.Committed, tx.State);

            var task = tx.RollbackAsync();  // Illegal, but should not fail here; will fail in task

            Assert.Throws<AggregateException>(() => task.Wait());
        }

        /// <summary>
        /// Tests the transaction deadlock detection.
        /// </summary>
        [Test]
        public void TestTxDeadlockDetection()
        {
            var cache = Cache();

            var keys0 = Enumerable.Range(1, 100).ToArray();

            cache.PutAll(keys0.ToDictionary(x => x, x => x));

            var barrier = new Barrier(2);

            Action<int[]> increment = keys =>
            {
                using (var tx = Transactions.TxStart(TransactionConcurrency.Pessimistic,
                    TransactionIsolation.RepeatableRead, TimeSpan.FromSeconds(0.5), 0))
                {
                    foreach (var key in keys)
                        cache[key]++;

                    barrier.SignalAndWait(500);

                    tx.Commit();
                }
            };

            // Increment keys within tx in different order to cause a deadlock.
            var aex = Assert.Throws<AggregateException>(() =>
                Task.WaitAll(Task.Factory.StartNew(() => increment(keys0)),
                             Task.Factory.StartNew(() => increment(keys0.Reverse().ToArray()))));

            Assert.AreEqual(2, aex.InnerExceptions.Count);

            var deadlockEx = aex.InnerExceptions.OfType<TransactionDeadlockException>().First();
            Assert.IsTrue(deadlockEx.Message.Trim().StartsWith("Deadlock detected:"), deadlockEx.Message);
        }

        /// <summary>
        /// Test Ignite transaction enlistment in ambient <see cref="TransactionScope"/>.
        /// </summary>
        [Test]
        public void TestTransactionScopeSingleCache()
        {
            var cache = Cache();

            cache[1] = 1;
            cache[2] = 2;

            // Commit.
            using (var ts = new TransactionScope())
            {
                cache[1] = 10;
                cache[2] = 20;

                Assert.IsNotNull(cache.Ignite.GetTransactions().Tx);

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
        /// Test Ignite transaction enlistment in ambient <see cref="TransactionScope"/> 
        /// with multiple participating caches.
        /// </summary>
        [Test]
        public void TestTransactionScopeMultiCache()
        {
            var cache1 = Cache();

            var cache2 = GetIgnite(0).GetOrCreateCache<int, int>(new CacheConfiguration(cache1.Name + "_")
            {
                AtomicityMode = CacheAtomicityMode.Transactional
            });

            cache1[1] = 1;
            cache2[1] = 2;

            // Commit.
            using (var ts = new TransactionScope())
            {
                cache1[1] = 10;
                cache2[1] = 20;

                ts.Complete();
            }

            Assert.AreEqual(10, cache1[1]);
            Assert.AreEqual(20, cache2[1]);

            // Rollback.
            using (new TransactionScope())
            {
                cache1[1] = 100;
                cache2[1] = 200;
            }

            Assert.AreEqual(10, cache1[1]);
            Assert.AreEqual(20, cache2[1]);
        }

        /// <summary>
        /// Test Ignite transaction enlistment in ambient <see cref="TransactionScope"/> 
        /// when Ignite tx is started manually.
        /// </summary>
        [Test]
        public void TestTransactionScopeWithManualIgniteTx()
        {
            var cache = Cache();
            var transactions = cache.Ignite.GetTransactions();

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
            var cache = Cache();

            cache[1] = 1;

            using (new TransactionScope(TransactionScopeOption.Suppress))
            {
                cache[1] = 2;
            }

            // Even though transaction is not completed, the value is updated, because tx is suppressed.
            Assert.AreEqual(2, cache[1]);
        }

        /// <summary>
        /// Test Ignite transaction enlistment in ambient <see cref="TransactionScope"/> with nested scopes.
        /// </summary>
        [Test]
        public void TestNestedTransactionScope()
        {
            var cache = Cache();

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
            var cache = Cache();
            var transactions = cache.Ignite.GetTransactions();

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

                    var tx = transactions.Tx;
                    Assert.AreEqual(mode.Item2, tx.Isolation);
                    Assert.AreEqual(transactions.DefaultTransactionConcurrency, tx.Concurrency);
                }
            }
        }

        /// <summary>
        /// Tests all transactional operations with <see cref="TransactionScope"/>.
        /// </summary>
        [Test]
        public void TestTransactionScopeAllOperations()
        {
            for (var i = 0; i < 10; i++)
            {
                CheckTxOp((cache, key) => cache.Put(key, -5));
                CheckTxOp((cache, key) => cache.PutAsync(key, -5).Wait());

                CheckTxOp((cache, key) => cache.PutAll(new Dictionary<int, int> {{key, -7}}));
                CheckTxOp((cache, key) => cache.PutAllAsync(new Dictionary<int, int> {{key, -7}}).Wait());

                CheckTxOp((cache, key) =>
                {
                    cache.Remove(key);
                    cache.PutIfAbsent(key, -10);
                });
                CheckTxOp((cache, key) =>
                {
                    cache.Remove(key);
                    cache.PutIfAbsentAsync(key, -10).Wait();
                });

                CheckTxOp((cache, key) => cache.GetAndPut(key, -9));
                CheckTxOp((cache, key) => cache.GetAndPutAsync(key, -9).Wait());

                CheckTxOp((cache, key) =>
                {
                    cache.Remove(key);
                    cache.GetAndPutIfAbsent(key, -10);
                });
                CheckTxOp((cache, key) =>
                {
                    cache.Remove(key);
                    cache.GetAndPutIfAbsentAsync(key, -10).Wait();
                });

                CheckTxOp((cache, key) => cache.GetAndRemove(key));
                CheckTxOp((cache, key) => cache.GetAndRemoveAsync(key).Wait());

                CheckTxOp((cache, key) => cache.GetAndReplace(key, -11));
                CheckTxOp((cache, key) => cache.GetAndReplaceAsync(key, -11).Wait());

                CheckTxOp((cache, key) => cache.Invoke(key, new AddProcessor(), 1));
                CheckTxOp((cache, key) => cache.InvokeAsync(key, new AddProcessor(), 1).Wait());

                CheckTxOp((cache, key) => cache.InvokeAll(new[] {key}, new AddProcessor(), 1));
                CheckTxOp((cache, key) => cache.InvokeAllAsync(new[] {key}, new AddProcessor(), 1).Wait());

                CheckTxOp((cache, key) => cache.Remove(key));
                CheckTxOp((cache, key) => cache.RemoveAsync(key).Wait());

                CheckTxOp((cache, key) => cache.RemoveAll(new[] {key}));
                CheckTxOp((cache, key) => cache.RemoveAllAsync(new[] {key}).Wait());

                CheckTxOp((cache, key) => cache.Replace(key, 100));
                CheckTxOp((cache, key) => cache.ReplaceAsync(key, 100).Wait());

                CheckTxOp((cache, key) => cache.Replace(key, cache[key], 100));
                CheckTxOp((cache, key) => cache.ReplaceAsync(key, cache[key], 100).Wait());
            }
        }

        /// <summary>
        /// Checks that cache operation behaves transactionally.
        /// </summary>
        private void CheckTxOp(Action<ICache<int, int>, int> act)
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

                var cache = Cache();

                cache[1] = 1;
                cache[2] = 2;

                // Rollback.
                using (new TransactionScope(scope, txOpts))
                {
                    act(cache, 1);

                    Assert.IsNotNull(cache.Ignite.GetTransactions().Tx, "Transaction has not started.");
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

        [Serializable]
        private class AddProcessor : ICacheEntryProcessor<int, int, int, int>
        {
            public int Process(IMutableCacheEntry<int, int> entry, int arg)
            {
                entry.Value += arg;
                return arg;
            }
        }
    }
}