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
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests <see cref="IIgniteLock"/> failover.
    /// </summary>
    public class IgniteLockFailoverTests
    {
        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that "lock broken" exception is thrown when lock is not failover-safe and owner node leaves.
        /// </summary>
        [Test]
        public void TestNonFailoverSafeLockThrowsExceptionOnAllNodesWhenOwnerLeaves()
        {
            var lock1 = TestFailover(false);
            var ex = Assert.Throws<IgniteException>(() => lock1.Enter());

            StringAssert.StartsWith("Lock broken", ex.Message);
            Assert.IsTrue(lock1.IsBroken());
        }

        /// <summary>
        /// Tests that failover-safe lock releases when owner node leaves.
        /// </summary>
        [Test]
        public void TestFailoverSafeLockIsReleasedWhenOwnerLeaves()
        {
            var lock1 = TestFailover(true);
            lock1.Enter();

            Assert.IsTrue(lock1.IsEntered());
            Assert.IsFalse(lock1.IsBroken());
        }

        /// <summary>
        /// Tests failover scenario when lock owner node leaves.
        /// </summary>
        private static IIgniteLock TestFailover(bool isFailoverSafe)
        {
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());

            var cfg = new LockConfiguration
            {
                Name = TestUtils.TestName,
                IsFailoverSafe = isFailoverSafe
            };

            var lock1 = ignite.GetOrCreateLock(cfg, true);
            var evt = new ManualResetEventSlim(false);

            Task.Factory.StartNew(() =>
            {
                var igniteCfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    IgniteInstanceName = cfg.Name
                };

                var ignite2 = Ignition.Start(igniteCfg);

                var lock2 = ignite2.GetOrCreateLock(cfg, true);
                lock2.Enter();

                evt.Set();
                Thread.Sleep(100);

                Ignition.Stop(cfg.Name, true);
            });

            evt.Wait();

            return lock1;
        }
    }
}
