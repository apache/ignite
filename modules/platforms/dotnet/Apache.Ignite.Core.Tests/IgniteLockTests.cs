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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IIgniteLock"/>.
    /// </summary>
    public class IgniteLockTests : TestBase
    {
        [Test]
        public void TestBasicLocking()
        {
            using (var lck = Ignite.GetOrCreateLock("my-lock"))
            {
                var cfg = lck.Configuration;
                
                Assert.IsFalse(cfg.IsFailoverSafe);
                Assert.IsFalse(cfg.IsFair);
                Assert.AreEqual("my-lock", cfg.Name);
                
                Assert.False(lck.IsEntered());
                Assert.False(lck.IsBroken());
                
                Assert.IsTrue(lck.TryEnter());
                
                Assert.IsTrue(lck.IsEntered());
                Assert.False(lck.IsBroken());
                
                lck.Exit();
                
                Assert.False(lck.IsEntered());
                Assert.False(lck.IsBroken());
            }
        }

        [Test]
        public void TestDisposeExitsLock()
        {
            var cfg = new LockConfiguration
            {
                Name = "my-lock"
            };
            
            using (var lck = Ignite.GetOrCreateLock(cfg, true))
            {
                lck.Enter();
            }

            Task.Factory.StartNew(() =>
            {
                using (var lck2 = Ignite.GetOrCreateLock(cfg, false))
                {
                    lck2.Enter();
                }
            }).Wait();
        }

        [Test]
        public void TestConfiguration()
        {
            
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
        public void TestGetOrCreateLockThrowsOnMissingLockWhenCreateFlagIsNotSet()
        {
            // TODO
        }
    }
}