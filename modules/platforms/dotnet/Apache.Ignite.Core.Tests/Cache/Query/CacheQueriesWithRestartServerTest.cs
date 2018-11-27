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

namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Query;
    using NUnit.Framework;

    public class CacheQueriesRestartServerTest
    {
        private IIgnite _client;

        private IIgnite _server;

        [SetUp]
        public void BeforeTest()
        {
            _server = StartGrid(0);
            _client = StartGrid(0, true);

            TestUtils.WaitForCondition(() => _server.GetCluster().GetNodes().Count == 2, 1000);
        }

        [Test]
        public void Test()
        {
            var cache = _client.GetOrCreateCache<int, Item>("Test");

            cache.Put(1, new Item()
            {
                Id = 10,
                Title = "test"
            });

            StopGrid(_server);

            _server = StartGrid(0);

            WaitForReconnect(_client, 5000);

            cache = _client.GetOrCreateCache<int, Item>("Test");

            cache.Put(1, new Item()
            {
                Id = 11,
                Title = "test"
            });

            var cursor = cache.Query(new ScanQuery<int, Item>(new TestFilter()));

            Assert.DoesNotThrow(() => cursor.GetAll());
        }

        [TearDown]
        public void AfterTest()
        {
            Ignition.StopAll(true);
        }

        private IIgnite StartGrid(int i, bool client = false)
        {
            return Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    NameMapper = GetNameMapper()
                },

                ClientMode = client,

                IgniteInstanceName = client ?  "client-" + i : "grid-" + i
            });
        }

        private void StopGrid(IIgnite ignite)
        {
            Ignition.Stop(ignite.Name, false);
        }


        private void WaitForReconnect(IIgnite ignite, int timeout)
        {
            bool reconnected = false;

            ignite.ClientReconnected += (sender, args) => { reconnected = true; };

            TestUtils.WaitForCondition(() => reconnected, timeout);
        }

        protected virtual IBinaryNameMapper GetNameMapper()
        {
            return new BinaryBasicNameMapper {IsSimpleName = false};
        }
    }

    public class TestFilter : ICacheEntryFilter<int, Item> {
        public bool Invoke(ICacheEntry<int, Item> entry)
        {
            return entry.Value.Id > 10;
        }
    }

    public class Item : IBinarizable
    {
        public int Id { get; set; }

        public string Title { get; set; }

        private Dictionary<string, object> _data = new Dictionary<string, object>();

        public Dictionary<string, object> Data
        {
            get { return _data; }
            set { _data = value; }
        }

        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteInt("Id", Id);
            writer.WriteString("Title", Title);
        }

        public void ReadBinary(IBinaryReader reader)
        {
            Id = reader.ReadInt("Id");
            Title = reader.ReadString("Title");
        }
    }
}
