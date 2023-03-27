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

// ReSharper disable MethodHasAsyncOverload
namespace Apache.Ignite.Core.Tests.Cache
{
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests thick cache operations with async/await.
    /// </summary>
    public class CacheTestAsyncAwait : TestBase
    {
        /// <summary>
        /// Initializes a new instance of <see cref="CacheTestAsyncAwait"/> class.
        /// </summary>
        public CacheTestAsyncAwait() : base(2)
        {
            // No-op.
        }

        /// <summary>
        /// Tests that async continuations are executed on a ThreadPool thread, not on response handler thread.
        /// </summary>
        [Test]
        public async Task TestAsyncAwaitContinuationIsExecutedWithConfiguredExecutor()
        {
            var cache = Ignite.GetOrCreateCache<int, int>(TestUtils.TestName);
            var key = TestUtils.GetPrimaryKey(Ignite2, cache.Name);

            // This causes deadlock if async continuation is executed on the striped thread.
            await cache.PutAsync(key, 1);
            cache.Replace(key, 2);

            Assert.AreEqual(2, cache.Get(key));
            StringAssert.DoesNotContain("sys-stripe-", TestUtilsJni.GetJavaThreadName());
        }

        /// <summary>
        /// Tests that local operation executes synchronously and completes on the same thread.
        /// </summary>
        [Test]
        public async Task TestLocalOperationExecutesSynchronously()
        {
            var cache = Ignite.GetOrCreateCache<int, int>(TestUtils.TestName);
            var key = TestUtils.GetPrimaryKey(Ignite, cache.Name);
            var origThread = Thread.CurrentThread;

            await cache.PutAsync(key, key);

            Assert.AreEqual(origThread.ManagedThreadId, Thread.CurrentThread.ManagedThreadId);
        }

        /// <summary>
        /// Tests that explicitly configured synchronous executor runs continuations on the striped pool.
        /// </summary>
        [Test]
        public async Task TestSynchronousExecutorRunsContinuationsOnStripedPool()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration(name: "client"))
            {
                AsyncContinuationExecutor = AsyncContinuationExecutor.UnsafeSynchronous,
                ClientMode = true
            };

            using (var client = Ignition.Start(cfg))
            {
                var cache = client.GetOrCreateCache<int, int>(TestUtils.TestName);

                await cache.PutAsync(1, 1);

                StringAssert.StartsWith("sys-stripe-", TestUtilsJni.GetJavaThreadName());

                Assert.AreEqual(AsyncContinuationExecutor.UnsafeSynchronous,
                    client.GetConfiguration().AsyncContinuationExecutor);

                // Jump away from striped pool to avoid deadlock on node stop.
                await Task.Yield();
            }
        }

        /// <summary>
        /// Tests that invalid executor configuration is rejected.
        /// </summary>
        [Test]
        public void TestInvalidExecutorConfigurationFailsOnStart()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                AsyncContinuationExecutor = AsyncContinuationExecutor.Custom
            };

            var ex = Assert.Throws<IgniteException>(() => Ignition.Start(cfg));
            Assert.AreEqual("Invalid AsyncContinuationExecutor mode: 2", ex.Message);
        }
    }
}
