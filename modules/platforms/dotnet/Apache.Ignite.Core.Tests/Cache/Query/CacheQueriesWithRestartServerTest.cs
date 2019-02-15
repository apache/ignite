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

namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using NUnit.Framework;

    /// <summary>
    /// Tests queries behavior with client reconnect and server restart.
    /// </summary>
    public sealed class CacheQueriesRestartServerTest
    {
        /** */
        private IIgnite _client;

        /** */
        private IIgnite _server;

        /// <summary>
        /// Sets up the fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            _server = StartGrid(0);
            _client = StartGrid(0, true);

            TestUtils.WaitForCondition(() => _server.GetCluster().GetNodes().Count == 2, 1000);
        }

        /// <summary>
        /// Tears down the fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that Scan query works after client reconnect with full cluster restart.
        /// </summary>
        [Test]
        public void Test_ScanQueryAfterClientReconnect_ReturnsResults([Values(true, false)] bool emptyFilterObject)
        {
            var cache = _client.GetOrCreateCache<int, Item>("Test");
            cache.Put(1, new Item { Id = 20, Title = "test" });

            Ignition.Stop(_server.Name, false);
            _server = StartGrid(0);
            WaitForReconnect(_client, 10000);

            cache = _client.GetOrCreateCache<int, Item>("Test");
            cache.Put(1, new Item { Id = 30, Title = "test" });

            var filter = emptyFilterObject
                ? (ICacheEntryFilter<int, Item>) new TestFilter()
                : new TestFilterWithField {TestValue = 9};

            var cursor = cache.Query(new ScanQuery<int, Item>(filter));
            var items = cursor.GetAll();

            Assert.AreEqual(30, items.Single().Value.Id);
        }

        /// <summary>
        /// Starts the grid.
        /// </summary>
        private static IIgnite StartGrid(int i, bool client = false)
        {
            return Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                ClientMode = client,
                IgniteInstanceName = client ? "client-" + i : "grid-" + i
            });
        }

        /// <summary>
        /// Waits for reconnect.
        /// </summary>
        private static void WaitForReconnect(IIgnite ignite, int timeout)
        {
            var evt = new ManualResetEventSlim(false);

            ignite.ClientReconnected += (sender, args) => evt.Set();

            var restarted = evt.Wait(timeout);
            Assert.IsTrue(restarted);
        }

        /// <summary>
        /// Test filter.
        /// </summary>
        private class TestFilter : ICacheEntryFilter<int, Item>
        {
            /** */
            public bool Invoke(ICacheEntry<int, Item> entry)
            {
                return entry.Value.Id > 10;
            }
        }

        /// <summary>
        /// Test filter with field.
        /// </summary>
        private class TestFilterWithField : ICacheEntryFilter<int, Item>
        {
            /** */
            public int TestValue { get; set; }

            /** */
            public bool Invoke(ICacheEntry<int, Item> entry)
            {
                return entry.Value.Id > TestValue;
            }
        }

        private class Item : IBinarizable
        {
            /** */
            public int Id { get; set; }

            /** */
            public string Title { get; set; }

            /** */
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteInt("Id", Id);
                writer.WriteString("Title", Title);
            }

            /** */
            public void ReadBinary(IBinaryReader reader)
            {
                Id = reader.ReadInt("Id");
                Title = reader.ReadString("Title");
            }
        }
    }
}
