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

namespace Apache.Ignite.Core.Tests.DotNetCore.Cache
{
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Tests.DotNetCore.Common;
    using NUnit.Framework;

    /// <summary>
    /// Cache tests.
    /// </summary>
    public class CacheTest : TestBase
    {
        /// <summary>
        /// Tests the put / get functionality.
        /// </summary>
        [Test]
        public void TestPutGet()
        {
            async Task PutGetAsync(ICache<int, Person> cache)
            {
                await cache.PutAsync(2, new Person("Bar", 2));
                var res = await cache.GetAsync(2);
                Assert.AreEqual(2, res.Id);
            }

            using (var ignite = Start())
            {
                var cache = ignite.CreateCache<int, Person>("persons");

                // Sync.
                cache[1] = new Person("Foo", 1);
                Assert.AreEqual("Foo", cache[1].Name);

                // Async.
                PutGetAsync(cache).Wait();
            }
        }
    }
}