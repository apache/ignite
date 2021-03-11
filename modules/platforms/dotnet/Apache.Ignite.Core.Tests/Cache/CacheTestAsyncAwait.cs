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
    using System.Threading.Tasks;
    using NUnit.Framework;

    /// <summary>
    /// Tests thick cache operations with async/await.
    /// </summary>
    public class CacheTestAsyncAwait : TestBase
    {
        /// <summary>
        /// Tests that async continuations are executed on a ThreadPool thread, not on response handler thread.
        /// </summary>
        [Test]
        public async Task TestAsyncAwaitContinuationIsExecutedWithConfiguredExecutor()
        {
            var cache = Ignite.GetOrCreateCache<int, int>(TestUtils.TestName);

            // This causes deadlock if async continuation is executed on the striped thread.
            await cache.PutAsync(1, 1);
            await cache.PutAsync(1, 2);

            Assert.AreEqual(2, cache.Get(1));
        }

    }
}
