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
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache;
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
    }
}