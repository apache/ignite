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
                Name = "x",
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
            Assert.AreEqual("x", lck.Configuration.Name);
            Assert.IsTrue(lck.Configuration.IsFair);
            Assert.IsTrue(lck.Configuration.IsFailoverSafe);
        }

        [Test]
        public void TestNonFailoverSafeLockThrowsExceptionOnAllNodesWhenOwnerLeaves()
        {
            // TODO
        }

        [Test]
        public void TestFailoverSafeLockIsReleasedWhenOwnerLeaves()
        {
            // TODO
        }

        [Test]
        public void TestGetOrCreateLockReturnsNullOnMissingLockWhenCreateFlagIsNotSet()
        {
            Assert.IsNull(Ignite.GetOrCreateLock(new LockConfiguration {Name = "x"}, false));
        }

        [Test]
        public void TestTryEnterReturnsTrueWhenUnlocked()
        {
            var lock1 = Ignite.GetOrCreateLock(TestUtils.TestName);

            Assert.IsTrue(lock1.TryEnter());
            Assert.IsTrue(lock1.TryEnter(TimeSpan.Zero));
            Assert.IsTrue(lock1.TryEnter(TimeSpan.FromMilliseconds(50)));

            lock1.Exit();
        }

        [Test]
        public void TestTryEnterReturnsFalseWhenLocked()
        {
            var lock1 = Ignite.GetOrCreateLock(TestUtils.TestName);
            var lock2 = Ignite.GetOrCreateLock(TestUtils.TestName);

            lock1.Enter();

            Assert.IsFalse(lock2.TryEnter());
            Assert.IsFalse(lock2.TryEnter(TimeSpan.Zero));
            Assert.IsFalse(lock2.TryEnter(TimeSpan.FromMilliseconds(50)));
        }

        [Test]
        public void TestReentrancy()
        {

        }
    }
}
