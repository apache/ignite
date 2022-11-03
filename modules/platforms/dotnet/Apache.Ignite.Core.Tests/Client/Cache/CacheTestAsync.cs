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
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Async cache test.
    /// </summary>
    [TestFixture]
    [Category(TestUtils.CategoryIntensive)]
    public sealed class CacheTestAsync : CacheTest
    {
        /** <inheritdoc /> */
        protected override ICacheClient<TK, TV> GetClientCache<TK, TV>(string cacheName = CacheName)
        {
            return new CacheClientAsyncWrapper<TK, TV>(base.GetClientCache<TK, TV>(cacheName));
        }

        /// <summary>
        /// Tests that async continuations are executed on a ThreadPool thread, not on response handler thread.
        /// </summary>
        [Test]
        public void TestAsyncContinuationIsExecutedOnThreadPool()
        {
            var cache = base.GetClientCache<int>();
            int? threadId1 = null;
            int? threadId2 = null;

            cache.PutAsync(1, 1).ContinueWith(_ =>
            {
                threadId1 = Thread.CurrentThread.ManagedThreadId;
                Thread.Sleep(3000);
            }, TaskContinuationOptions.ExecuteSynchronously);

            cache.PutAsync(1, 1).ContinueWith(_ =>
            {
                threadId2 = Thread.CurrentThread.ManagedThreadId;
                Thread.Sleep(3000);
            }, TaskContinuationOptions.ExecuteSynchronously);

            TestUtils.WaitForTrueCondition(
                () => threadId1 != null && threadId2 != null && threadId1.Value != threadId2.Value);
        }
    }
}
