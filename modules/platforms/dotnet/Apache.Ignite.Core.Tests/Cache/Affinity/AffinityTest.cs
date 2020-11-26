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
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
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
