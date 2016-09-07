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

namespace Apache.Ignite.Core.Tests.AspNet
{
    using System;
    using Apache.Ignite.Core.Impl.AspNet;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="LockInfo"/>.
    /// </summary>
    public class LockInfoTest
    {
        /// <summary>
        /// Tests the equality.
        /// </summary>
        [Test]
        public void TestEquality()
        {
            var lock1 = new LockInfo(1, Guid.NewGuid(), DateTime.UtcNow);
            var lock2 = new LockInfo(2, Guid.NewGuid(), DateTime.UtcNow.AddDays(1));
            var lock3 = new LockInfo(lock1.LockId, lock1.LockNodeId, lock1.LockTime);

            Assert.AreEqual(lock1, lock3);
            Assert.AreEqual(lock1.GetHashCode(), lock3.GetHashCode());

            Assert.IsTrue(lock1 == lock3);
            Assert.IsFalse(lock1 != lock3);

            Assert.AreNotEqual(lock1, lock2);
            Assert.AreNotEqual(lock1.GetHashCode(), lock2.GetHashCode());

            Assert.IsFalse(lock1 == lock2);
            Assert.IsTrue(lock1 != lock2);
        }

        /// <summary>
        /// Tests the serialization.
        /// </summary>
        [Test]
        public void TestSerialization()
        {
            var lock1 = new LockInfo(2, Guid.NewGuid(), DateTime.UtcNow.AddDays(1));
            var lock2 = TestUtils.SerializeDeserialize(lock1);

            Assert.AreEqual(lock1, lock2);
        }
    }
}
