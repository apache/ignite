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
            for (int i = 0; i < 3; i++)
            {
                var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    SpringConfigUrl = Path.Combine("Config", "native-client-test-cache-affinity.xml"),
                    IgniteInstanceName = "grid-" + i
                };

                Ignition.Start(cfg);
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
        /// Tests AffinityKeyMapped attribute should map to the same partitions
        /// for the same field value.
        /// </summary>
        [Test]
        public void TestCustomAffinity()
        {
            // Cause:
            // * CacheObjectBinaryProcessorImpl.java:1086 caches empty value
            // * It is called from QueryUtils.java:527 on cache start
            
            // TODO:
            // * Affinity key field name is used for queries (how?) - add a test for that as well
            //   (see where GridQueryTypeDescriptor#affinityKey is used - we should ensure it is passed correctly).
            
            // TODO: 
            // * Do we have the same issue in Java? If not, how does it work there?
            IIgnite g = Ignition.GetIgnite("grid-0");

            var cacheCfg = new CacheConfiguration("mycache")
            {
                // Without QueryEntities tests passes.
                QueryEntities = new List<QueryEntity>
                {
                    new QueryEntity(typeof(MyKey), typeof(int))
                }
            };
            g.GetOrCreateCache<MyKey, int>(cacheCfg);

            var key1 = new MyKey {Data = "data1", AffinityKey = 1};
            var key2 = new MyKey {Data = "data2", AffinityKey = 1};

            ICacheAffinity aff = g.GetAffinity(cacheCfg.Name);
            Assert.AreEqual(aff.GetPartition(key1), aff.GetPartition(key2));
        }

        [SuppressMessage("ReSharper", "UnusedAutoPropertyAccessor.Local")]
        private class MyKey
        {
            [QuerySqlField]
            public string Data { get; set; }
            
            [AffinityKeyMapped]
            public long AffinityKey { get; set; }
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
    }
}
