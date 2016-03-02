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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Discovery.Tcp.Static;
    using NUnit.Framework;

    /// <summary>
    /// Tests custom affinity mapping.
    /// </summary>
    public class CacheAffinityFieldTest
    {
        /** */
        private ICache<CacheKey, string> _cache1;

        /** */
        private ICache<CacheKey, string> _cache2;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var grid1 = Ignition.Start(GetConfig());
            var grid2 = Ignition.Start(GetConfig("grid2"));

            _cache1 = grid1.CreateCache<CacheKey, string>(new CacheConfiguration
            {
                CacheMode = CacheMode.Partitioned
            });
            _cache2 = grid2.GetCache<CacheKey, string>(null);
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the metadata.
        /// </summary>
        [Test]
        public void TestMetadata()
        {
            foreach (var type in new[] { typeof(CacheKey) , typeof(CacheKeyAttr), typeof(CacheKeyAttrOverride)})
            {
                Assert.AreEqual("AffinityKey", _cache1.Ignite.GetBinary().GetBinaryType(type).AffinityKeyFieldName);
                Assert.AreEqual("AffinityKey", _cache2.Ignite.GetBinary().GetBinaryType(type).AffinityKeyFieldName);
            }
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        private static IgniteConfiguration GetConfig(string gridName = null)
        {
            return new IgniteConfiguration
            {
                GridName = gridName,
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new[]
                    {
                        new BinaryTypeConfiguration(typeof (CacheKey))
                        {
                            AffinityKeyFieldName = "AffinityKey"
                        },
                        new BinaryTypeConfiguration(typeof(CacheKeyAttr)),
                        new BinaryTypeConfiguration(typeof (CacheKey))
                        {
                            AffinityKeyFieldName = "AffinityKey"
                        }
                    }
                },
                DiscoverySpi = new TcpDiscoverySpi
                {
                    IpFinder = new TcpDiscoveryStaticIpFinder
                    {
                        Endpoints = new[] {"127.0.0.1:47500", "127.0.0.1:47501"}
                    }
                }
            };
        }

        private class CacheKey
        {
            public int Key { get; set; }
            public int AffinityKey { get; set; }
        }

        private class CacheKeyAttr
        {
            public int Key { get; set; }
            [AffinityKeyMapped] public int AffinityKey { get; set; }
        }

        private class CacheKeyAttrOverride
        {
            [AffinityKeyMapped] public int Key { get; set; }
            public int AffinityKey { get; set; }
        }
    }
}
