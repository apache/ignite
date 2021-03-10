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

namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using NUnit.Framework;

    /// <summary>
    /// Affinity key tests.
    /// </summary>
    public sealed class AffinityTest
    {
        /// <summary>
        /// Test set up.
        /// </summary>
        [TestFixtureSetUp]
        public void StartGrids()
        {
            for (var i = 0; i < 3; i++)
            {
                Ignition.Start(GetConfig(i, client: i == 2));
            }
        }

        /// <summary>
        /// Tear-down routine.
        /// </summary>
        [TestFixtureTearDown]
        public void StopGrids()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Test affinity key.
        /// </summary>
        [Test]
        public void TestAffinity()
        {
            IIgnite g = Ignition.GetIgnite("grid-0");

            ICacheAffinity aff = g.GetAffinity("default");

            IClusterNode node = aff.MapKeyToNode(new AffinityTestKey(0, 1));

            for (int i = 0; i < 10; i++)
                Assert.AreEqual(node.Id, aff.MapKeyToNode(new AffinityTestKey(i, 1)).Id);
        }

        /// <summary>
        /// Tests that affinity can be retrieved from client node right after the cache has been started on server node.
        /// </summary>
        [Test]
        public void TestAffinityRetrievalForNewCache()
        {
            var server = Ignition.GetIgnite("grid-0");
            var client = Ignition.GetIgnite("grid-2");

            var serverCache = server.CreateCache<int, int>(TestUtils.TestName);
            var clientAff = client.GetAffinity(serverCache.Name);

            Assert.IsNotNull(clientAff);
        }

        /// <summary>
        /// Test affinity with binary flag.
        /// </summary>
        [Test]
        public void TestAffinityBinary()
        {
            IIgnite g = Ignition.GetIgnite("grid-0");

            ICacheAffinity aff = g.GetAffinity("default");

            IBinaryObject affKey = g.GetBinary().ToBinary<IBinaryObject>(new AffinityTestKey(0, 1));

            IClusterNode node = aff.MapKeyToNode(affKey);

            for (int i = 0; i < 10; i++)
            {
                IBinaryObject otherAffKey =
                    g.GetBinary().ToBinary<IBinaryObject>(new AffinityTestKey(i, 1));

                Assert.AreEqual(node.Id, aff.MapKeyToNode(otherAffKey).Id);
            }
        }

        /// <summary>
        /// Tests that <see cref="AffinityKeyMappedAttribute"/> works when used on a property of a type that is
        /// specified as <see cref="QueryEntity.KeyType"/> or <see cref="QueryEntity.ValueType"/> and
        /// configured in a Spring XML file.
        /// </summary>
        [Test]
        public void TestAffinityKeyMappedWithQueryEntitySpringXml()
        {
            foreach (var ignite in Ignition.GetAll())
            {
                TestAffinityKeyMappedWithQueryEntity0(ignite, "cache1");
            }
        }

        /// <summary>
        /// Tests that <see cref="AffinityKey"/> works when used as <see cref="QueryEntity.KeyType"/>.
        /// </summary>
        [Test]
        public void TestAffinityKeyWithQueryEntity()
        {
            var cacheCfg = new CacheConfiguration(TestUtils.TestName)
            {
                QueryEntities = new List<QueryEntity>
                {
                    new QueryEntity(typeof(AffinityKey), typeof(QueryEntityValue))
                }
            };

            var ignite = Ignition.GetIgnite("grid-0");
            var cache = ignite.GetOrCreateCache<AffinityKey, QueryEntityValue>(cacheCfg);
            var aff = ignite.GetAffinity(cache.Name);

            var ignite2 = Ignition.GetIgnite("grid-1");
            var cache2 = ignite2.GetOrCreateCache<AffinityKey, QueryEntityValue>(cacheCfg);
            var aff2 = ignite2.GetAffinity(cache2.Name);

            // Check mapping.
            for (var i = 0; i < 100; i++)
            {
                Assert.AreEqual(aff.GetPartition(i), aff.GetPartition(new AffinityKey("foo" + i, i)));
                Assert.AreEqual(aff2.GetPartition(i), aff2.GetPartition(new AffinityKey("bar" + i, i)));
                Assert.AreEqual(aff.GetPartition(i), aff2.GetPartition(i));
            }

            // Check put/get.
            var key = new AffinityKey("x", 123);
            var expected = new QueryEntityValue {Name = "y", AffKey = 321};
            cache[key] = expected;

            var val = cache2[key];
            Assert.AreEqual(expected.Name, val.Name);
            Assert.AreEqual(expected.AffKey, val.AffKey);
        }

        /// <summary>
        /// Tests that <see cref="AffinityKeyMappedAttribute"/> works when used on a property of a type that is
        /// specified as <see cref="QueryEntity.KeyType"/> or <see cref="QueryEntity.ValueType"/>.
        /// </summary>
        [Test]
        public void TestAffinityKeyMappedWithQueryEntity()
        {
            var cacheCfg = new CacheConfiguration(TestUtils.TestName)
            {
                QueryEntities = new List<QueryEntity>
                {
                    new QueryEntity(typeof(QueryEntityKey), typeof(QueryEntityValue))
                }
            };

            var cache = Ignition.GetIgnite("grid-0").GetOrCreateCache<QueryEntityKey, QueryEntityValue>(cacheCfg);
            var cache2 = Ignition.GetIgnite("grid-1").GetOrCreateCache<QueryEntityKey, QueryEntityValue>(cacheCfg);

            TestAffinityKeyMappedWithQueryEntity0(Ignition.GetIgnite("grid-0"), cacheCfg.Name);
            TestAffinityKeyMappedWithQueryEntity0(Ignition.GetIgnite("grid-1"), cacheCfg.Name);

            // Check put/get.
            var key = new QueryEntityKey {Data = "x", AffinityKey = 123};
            cache[key] = new QueryEntityValue {Name = "y", AffKey = 321};

            var val = cache2[key];
            Assert.AreEqual("y", val.Name);
            Assert.AreEqual(321, val.AffKey);
        }

        /// <summary>
        /// Checks affinity mapping.
        /// </summary>
        private static void TestAffinityKeyMappedWithQueryEntity0(IIgnite ignite, string cacheName)
        {
            var aff = ignite.GetAffinity(cacheName);

            var key1 = new QueryEntityKey {Data = "data1", AffinityKey = 1};
            var key2 = new QueryEntityKey {Data = "data2", AffinityKey = 1};

            var val1 = new QueryEntityValue {Name = "foo", AffKey = 100};
            var val2 = new QueryEntityValue {Name = "bar", AffKey = 100};

            Assert.AreEqual(aff.GetPartition(key1), aff.GetPartition(key2));
            Assert.AreEqual(aff.GetPartition(val1), aff.GetPartition(val2));
        }

        /// <summary>
        /// Affinity key.
        /// </summary>
        private class AffinityTestKey
        {
            /** ID. */
            private readonly int _id;

            /** Affinity key. */
            // ReSharper disable once NotAccessedField.Local
            private readonly int _affKey;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="id">ID.</param>
            /// <param name="affKey">Affinity key.</param>
            public AffinityTestKey(int id, int affKey)
            {
                _id = id;
                _affKey = affKey;
            }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                var other = obj as AffinityTestKey;

                return other != null && _id == other._id;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return _id;
            }
        }

        /// <summary>
        /// Gets Ignite config.
        /// </summary>
        private static IgniteConfiguration GetConfig(int idx, bool client = false)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = Path.Combine("Config", "native-client-test-cache-affinity.xml"),
                IgniteInstanceName = "grid-" + idx,
                ClientMode = client
            };
        }

        /// <summary>
        /// Query entity key.
        /// </summary>
        [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
        private class QueryEntityKey
        {
            /** */
            [QuerySqlField]
            public string Data { get; set; }

            /** */
            [AffinityKeyMapped]
            public long AffinityKey { get; set; }
        }

        /// <summary>
        /// Query entity key.
        /// </summary>
        [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
        private class QueryEntityValue
        {
            /** */
            [QuerySqlField]
            public string Name { get; set; }

            /** */
            [AffinityKeyMapped]
            public long AffKey { get; set; }
        }
    }
}
