/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.DotNetCore.ThinClient.Cache
{
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Tests.Client;
    using NUnit.Framework;

    /// <summary>
    /// Tests cache operations with async/await.
    /// </summary>
    public class CacheTestAsyncAwait : ClientTestBase
    {
        /// <summary>
        /// Tests that async continuations are executed on a ThreadPool thread, not on response handler thread.
        /// </summary>
        [Test]
        public async Task TestAsyncAwaitContinuationIsExecutedOnThreadPool()
        {
            var cache = GetClientCache<int>();
            await cache.PutAsync(1, 1).ConfigureAwait(false);

            // This causes deadlock if async continuation is executed on response handler thread.
            cache.PutAsync(2, 2).Wait();
            Assert.AreEqual(2, cache.Get(2));
        }
    }
}
