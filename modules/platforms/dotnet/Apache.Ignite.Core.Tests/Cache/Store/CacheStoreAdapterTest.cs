/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
