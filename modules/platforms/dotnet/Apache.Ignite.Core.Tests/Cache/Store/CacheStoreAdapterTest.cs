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

namespace Apache.Ignite.Core.Tests.Cache.Store
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Store;
    using NUnit.Framework;

    /// <summary>
    /// Tests for <see cref="CacheStoreAdapter{K, V}"/>.
    /// </summary>
    public class CacheStoreAdapterTest
    {
        /// <summary>
        /// Tests the load write delete.
        /// </summary>
        [Test]
        public void TestLoadWriteDelete()
        {
            var store = new Store();

            store.LoadCache(null);
            Assert.IsEmpty(store.Map);

            var data = Enumerable.Range(1, 5).ToDictionary(x => x, x => x.ToString());

            // Write.
            store.WriteAll(data);
            Assert.AreEqual(data, store.Map);

            // Load.
            CollectionAssert.AreEqual(data, store.LoadAll(data.Keys));
            CollectionAssert.AreEqual(data.Where(x => x.Key < 3).ToDictionary(x => x.Key, x => x.Value),
                store.LoadAll(data.Keys.Where(x => x < 3).ToList()));

            // Delete.
            var removed = new[] {3, 5};

            foreach (var key in removed)
                data.Remove(key);

            store.DeleteAll(removed);
            CollectionAssert.AreEqual(data, store.LoadAll(data.Keys));
        }

        /// <summary>
        /// Test store.
        /// </summary>
        private class Store : CacheStoreAdapter<int, string>
        {
            /** */
            public readonly Dictionary<int, string> Map = new Dictionary<int, string>();

            /** <inheritdoc /> */
            public override string Load(int key)
            {
                string res;
                return Map.TryGetValue(key, out res) ? res : null;
            }

            /** <inheritdoc /> */
            public override void Write(int key, string val)
            {
                Map[key] = val;
            }

            /** <inheritdoc /> */
            public override void Delete(int key)
            {
                Map.Remove(key);
            }
        }
    }
}
