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

namespace Apache.Ignite.Core.Tests.Client.DataStructures
{
    using System;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.DataStructures;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="IAtomicLongClient"/>.
    /// </summary>
    public class AtomicLongClientTests : ClientTestBase
    {
        [Test]
        public void TestCreateSetsInitialValue()
        {
            var atomicLongClient = Client.GetAtomicLong(TestUtils.TestName, 42, true);
            var atomicLongServer = GetIgnite().GetAtomicLong(atomicLongClient.Name, 1, false);

            Assert.AreEqual(42, atomicLongClient.Read());
            Assert.AreEqual(42, atomicLongServer.Read());
        }

        [Test]
        public void TestCreateIgnoresInitialValueWhenAlreadyExists()
        {
            var atomicLong = Client.GetAtomicLong(TestUtils.TestName, 42, true);
            var atomicLong2 = Client.GetAtomicLong(TestUtils.TestName, 43, false);

            Assert.AreEqual(42, atomicLong.Read());
            Assert.AreEqual(42, atomicLong2.Read());
        }

        [Test]
        public void TestOperationsThrowExceptionWhenAtomicLongDoesNotExist()
        {
            var name = TestUtils.TestName;
            var atomicLong = Client.GetAtomicLong(name, 42, true);
            atomicLong.Close();

            Action<Action> assertDoesNotExistError = act =>
            {
                var ex = Assert.Throws<IgniteClientException>(() => act());

                StringAssert.Contains($"AtomicLong with name '{name}' does not exist.", ex.Message);
            };

            Assert.IsTrue(atomicLong.IsClosed());

            assertDoesNotExistError(() => atomicLong.Read());
            assertDoesNotExistError(() => atomicLong.Add(1));
            assertDoesNotExistError(() => atomicLong.Increment());
            assertDoesNotExistError(() => atomicLong.Decrement());
            assertDoesNotExistError(() => atomicLong.Exchange(22));
            assertDoesNotExistError(() => atomicLong.CompareExchange(22, 33));
        }

        [Test]
        public void TestIsClosed()
        {
            var atomicLong = Client.GetAtomicLong(TestUtils.TestName, 0, false);
            Assert.IsNull(atomicLong);

            atomicLong = Client.GetAtomicLong(TestUtils.TestName, 1, true);
            Assert.IsFalse(atomicLong.IsClosed());
            Assert.AreEqual(1, atomicLong.Read());

            atomicLong.Close();
            Assert.IsTrue(atomicLong.IsClosed());
        }

        [Test]
        public void TestIncrementDecrementAdd()
        {
            var atomicLong = Client.GetAtomicLong(TestUtils.TestName, 1, true);

            Assert.AreEqual(2, atomicLong.Increment());
            Assert.AreEqual(2, atomicLong.Read());

            Assert.AreEqual(1, atomicLong.Decrement());
            Assert.AreEqual(1, atomicLong.Read());

            Assert.AreEqual(101, atomicLong.Add(100));
            Assert.AreEqual(101, atomicLong.Read());
        }

        [Test]
        public void TestExchange()
        {
            var atomicLong = Client.GetAtomicLong(TestUtils.TestName, 1, true);

            Assert.AreEqual(1, atomicLong.Exchange(100));
            Assert.AreEqual(100, atomicLong.Read());
        }

        [Test]
        public void TestCompareExchange()
        {
            var atomicLong = Client.GetAtomicLong(TestUtils.TestName, 1, true);

            Assert.AreEqual(1, atomicLong.CompareExchange(3, 2));
            Assert.AreEqual(1, atomicLong.Read());

            Assert.AreEqual(1, atomicLong.CompareExchange(4, 1));
            Assert.AreEqual(4, atomicLong.Read());
        }

        [Test]
        public void TestPartitionAwareness()
        {
            // TODO: Move this to another file?
        }

        [Test]
        public void TestCustomConfigurationPropagatesToServer()
        {
            var cfg1 = new AtomicClientConfiguration
            {
                AtomicSequenceReserveSize = 32,
                Backups = 2,
                CacheMode = CacheMode.Partitioned,
                GroupName = "atomics-partitioned"
            };

            var cfg2 = new AtomicClientConfiguration
            {
                AtomicSequenceReserveSize = 33,
                Backups = 3,
                CacheMode = CacheMode.Replicated,
                GroupName = "atomics-replicated"
            };

            var name = TestUtils.TestName;

            Client.GetAtomicLong(name, cfg1, 1, true);
            Client.GetAtomicLong(name, cfg2, 2, true);
            Client.GetAtomicLong(name, 3, true);

            var cacheConfigBytes = Client.GetCompute().ExecuteJavaTask<byte[]>(
                "org.apache.ignite.platform.PlatformGetInternalCachesTask", null);

            Assert.IsNotNull(cacheConfigBytes);
        }
    }
}
