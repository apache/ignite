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

namespace Apache.Ignite.Core.Tests
{
    using System;
    using System.Collections.Concurrent;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IIgniteLock"/>.
    /// </summary>
    public class IgniteLockTests : TestBase
    {
        /// <summary>
        /// Initializes a new instance of <see cref="IgniteLockTests"/> class.
        /// </summary>
        public IgniteLockTests() : base(2)
        {
            // No-op.
        }

        /// <summary>
        /// Tests state changes: unlocked -> locked -> disposed.
        /// </summary>
        [Test]
        public void TestStateChanges()
        {
            var lck = Ignite.GetOrCreateLock(TestUtils.TestName);
            var cfg = lck.Configuration;

            Assert.IsFalse(cfg.IsFailoverSafe);
            Assert.IsFalse(cfg.IsFair);
            Assert.AreEqual(TestUtils.TestName, cfg.Name);

            Assert.False(lck.IsEntered());
            Assert.False(lck.IsBroken());

            Assert.IsTrue(lck.TryEnter());

            Assert.IsTrue(lck.IsEntered());
            Assert.False(lck.IsBroken());

            lck.Exit();

            Assert.False(lck.IsEntered());
            Assert.False(lck.IsBroken());

            lck.Remove();

            Assert.IsTrue(lck.IsRemoved());
        }

        /// <summary>
        /// Tests that thread blocks on Enter until lock is released.
        /// </summary>
        [Test]
        public void TestEnterBlocksWhenLockedByAnotherThread()
        {
            long state = 0;

            var lock1 = Ignite.GetOrCreateLock(TestUtils.TestName);
            lock1.Enter();

            // ReSharper disable once AccessToModifiedClosure
            var task = Task.Factory.StartNew(() =>
            {
                var lock2 = Ignite.GetOrCreateLock(TestUtils.TestName);
                Interlocked.Increment(ref state);
                lock2.Enter();
                Interlocked.Increment(ref state);
                lock2.Exit();
                Interlocked.Increment(ref state);
            });

            TestUtils.WaitForTrueCondition(() => Interlocked.Read(ref state) == 1);
            Assert.AreEqual(1, Interlocked.Read(ref state));

            lock1.Exit();
            task.Wait();
            Assert.AreEqual(3, Interlocked.Read(ref state));
        }

        /// <summary>
        /// Tests that Exit throws a meaningful exception when lock in not entered.
        /// </summary>
        [Test]
        public void TestExitThrowsCorrectExceptionWhenNotEntered()
        {
            var lock1 = Ignite.GetOrCreateLock(TestUtils.TestName);

            var ex = Assert.Throws<SynchronizationLockException>(() => lock1.Exit());
            var innerEx = ex.InnerException as JavaException;

            Assert.IsNotNull(innerEx);
            Assert.AreEqual("java.lang.IllegalMonitorStateException", innerEx.JavaClassName);
        }

        /// <summary>
        /// Tests that TryEnter succeeds when lock is not taken.
        /// </summary>
        [Test]
        public void TestTryEnterReturnsTrueWhenUnlocked()
        {
            var lock1 = Ignite.GetOrCreateLock(TestUtils.TestName);

            Assert.IsTrue(lock1.TryEnter());
            Assert.IsTrue(lock1.TryEnter(TimeSpan.Zero));
            Assert.IsTrue(lock1.TryEnter(TimeSpan.FromMilliseconds(50)));

            lock1.Exit();
        }

        /// <summary>
        /// Tests that TryEnter fails when lock is taken by another thread.
        /// </summary>
        [Test]
        public void TestTryEnterReturnsFalseWhenLocked()
        {
            var lock1 = Ignite.GetOrCreateLock(TestUtils.TestName);
            var lock2 = Ignite.GetOrCreateLock(TestUtils.TestName);

            lock1.Enter();

            Task.Factory.StartNew(() =>
            {
                Assert.IsFalse(lock2.TryEnter());
                Assert.IsFalse(lock2.TryEnter(TimeSpan.Zero));
                Assert.IsFalse(lock2.TryEnter(TimeSpan.FromMilliseconds(50)));
            }).Wait();

            lock1.Exit();
        }

        /// <summary>
        /// Tests that lock can be entered multiple times by the same thread.
        /// </summary>
        [Test]
        public void TestReentrancy()
        {
            const int count = 10;
            var lock1 = Ignite.GetOrCreateLock(TestUtils.TestName);

            for (var i = 0; i < count; i++)
            {
                lock1.Enter();
                Assert.IsTrue(lock1.IsEntered());
            }

            for (var i = 0; i < count; i++)
            {
                Assert.IsTrue(lock1.IsEntered());
                lock1.Exit();
            }

            Assert.IsFalse(lock1.IsEntered());
        }

        /// <summary>
        /// Tests that removed lock throws correct exception.
        /// </summary>
        [Test]
        public void TestRemovedLockThrowsIgniteException()
        {
            var lock1 = Ignite.GetOrCreateLock(TestUtils.TestName);
            var lock2 = Ignite2.GetOrCreateLock(TestUtils.TestName);

            Assert.IsFalse(lock2.IsEntered());
            lock1.Remove();

            var ex = Assert.Throws<IgniteException>(() => lock2.Enter());
            Assert.AreEqual("Failed to find reentrant lock with given name: " + lock2.Configuration.Name, ex.Message);
        }

        /// <summary>
        /// Tests that removed lock throws correct exception.
        /// </summary>
        [Test]
        [Ignore("IGNITE-13128")]
        public void TestRemovedBeforeUseLockThrowsIgniteException()
        {
            var lock1 = Ignite.GetOrCreateLock(TestUtils.TestName);
            var lock2 = Ignite2.GetOrCreateLock(TestUtils.TestName);

            lock1.Remove();

            var ex = Assert.Throws<IgniteException>(() => lock2.Enter());
            Assert.AreEqual("Failed to find reentrant lock with given name: " + lock2.Configuration.Name, ex.Message);
        }

        /// <summary>
        /// Tests that entered lock can't be removed.
        /// </summary>
        [Test]
        public void TestEnteredLockThrowsOnRemove()
        {
            var cfg = new LockConfiguration
            {
                Name = TestUtils.TestName
            };

            var lck = Ignite.GetOrCreateLock(cfg, true);

            lck.Enter();
            Assert.IsTrue(lck.IsEntered());

            var ex = Assert.Throws<IgniteException>(() => lck.Remove());
            StringAssert.StartsWith("Failed to remove reentrant lock with blocked threads", ex.Message);

            lck.Exit();
            lck.Remove();

            Assert.IsNull(Ignite.GetOrCreateLock(cfg, false));
        }

        /// <summary>
        /// Tests configuration propagation.
        /// </summary>
        [Test]
        public void TestLockConfigurationCantBeModifiedAfterLockCreation()
        {
            var cfg = new LockConfiguration
            {
                Name = TestUtils.TestName,
                IsFair = true,
                IsFailoverSafe = true
            };

            var lck = Ignite.GetOrCreateLock(cfg, true);

            // Change original instance.
            cfg.Name = "y";
            cfg.IsFair = false;
            cfg.IsFailoverSafe = false;

            // Change returned instance.
            lck.Configuration.Name = "y";
            lck.Configuration.IsFair = false;
            lck.Configuration.IsFailoverSafe = false;

            // Verify: actual config has not changed.
            Assert.AreEqual(TestUtils.TestName, lck.Configuration.Name);
            Assert.IsTrue(lck.Configuration.IsFair);
            Assert.IsTrue(lck.Configuration.IsFailoverSafe);
        }

        /// <summary>
        /// Tests that null is returned when lock does not exist and create flag is false.
        /// </summary>
        [Test]
        public void TestGetOrCreateLockReturnsNullOnMissingLockWhenCreateFlagIsNotSet()
        {
            Assert.IsNull(Ignite.GetOrCreateLock(new LockConfiguration {Name = TestUtils.TestName}, false));
        }

        /// <summary>
        /// Tests that fair lock favors granting access to the longest-waiting thread
        /// </summary>
        [Test]
        public void TestFairLockGuaranteesOrder()
        {
            const int count = 50;

            var cfg = new LockConfiguration
            {
                Name = TestUtils.TestName,
                IsFair = true,
                IsFailoverSafe = true
            };

            var lck = Ignite.GetOrCreateLock(cfg, true);
            lck.Enter();

            var locks = new ConcurrentQueue<int>();
            var threads = new Thread[count];

            var evt = new AutoResetEvent(false);

            for (int i = 0; i < count; i++)
            {
                var id = i;

                var thread = new Thread(() =>
                {
                    evt.Set();
                    lck.Enter();
                    locks.Enqueue(id);
                    lck.Exit();
                });

                thread.Start();

                evt.WaitOne();

                Thread.Sleep(50);

                threads[i] = thread;
            }

            lck.Exit();

            foreach (var thread in threads)
            {
                thread.Join();
            }

            Assert.AreEqual(count, locks.Count);
            CollectionAssert.IsOrdered(locks);
        }
    }
}
