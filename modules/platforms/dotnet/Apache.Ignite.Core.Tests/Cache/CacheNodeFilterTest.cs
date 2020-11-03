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
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cluster;
    using NUnit.Framework;

    /// <summary>
    /// Cache node filter tests.
    /// </summary>
    [TestFixture]
    public class CacheNodeFilterTest
    {
        /** */
        private const string AttrKey2 = "attr2";

        /**  */
        private const int AttrVal2 = 3;

        /** */
        private const string AttrKey3 = "my-key";

        /**  */
        private const string AttrVal3 = "my-val";

        /** Grid instances. */
        private IIgnite _grid1, _grid2, _grid3;

        /// <summary>
        ///  Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void TestFixtureSetUp()
        {
            var springConfig = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SpringConfigUrl = Path.Combine("Config", "cache-attribute-node-filter.xml"),
                IgniteInstanceName = "springGrid"
            };
            _grid1 = Ignition.Start(springConfig);

            _grid2 = Ignition.Start(GetTestConfiguration("Ignite2",
                new Dictionary<string, object>
                {
                    {AttrKey2, AttrVal2}
                }));

            _grid3 = Ignition.Start(GetTestConfiguration("Ignite3",
                new Dictionary<string, object>
                {
                    {AttrKey2, AttrVal2},
                    {AttrKey3, AttrVal3}
                }));
        }

        /// <summary>
        ///  Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void TestFixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Gets a test configuration.
        /// </summary>
        /// <param name="gridName">Grid name.</param>
        /// <param name="userAttributes">User attributes.</param>
        /// <returns></returns>
        private IgniteConfiguration GetTestConfiguration(string gridName, Dictionary<string, object> userAttributes)
        {
            IgniteConfiguration cfg = TestUtils.GetTestConfiguration(name: gridName);
            cfg.UserAttributes = userAttributes;
            return cfg;
        }

        /// <summary>
        /// Tests attribute node filter with a custom user attribute name
        /// and null value always matches.
        /// </summary>
        [Test]
        public void TestUserAttributeWithNullValueMatchesAllNodes()
        {
            const int replicatedPartitionsCount = 512;

            var cacheCfg = new CacheConfiguration
            {
                Name = Guid.NewGuid().ToString(),
                NodeFilter = new AttributeNodeFilter("my.custom.attr", null),
                CacheMode = CacheMode.Replicated,
            };
            var cache = _grid1.CreateCache<object, object>(cacheCfg);

            var affinity = _grid1.GetAffinity(cache.Name);

            Assert.AreEqual(3, _grid1.GetCluster().ForDataNodes(cache.Name).GetNodes().Count);

            var parts1 = affinity.GetAllPartitions(_grid1.GetCluster().GetLocalNode());
            var parts2 = affinity.GetAllPartitions(_grid2.GetCluster().GetLocalNode());
            var parts3 = affinity.GetAllPartitions(_grid3.GetCluster().GetLocalNode());

            Assert.AreEqual(replicatedPartitionsCount, parts1.Length);
            Assert.AreEqual(parts1, parts2);
            Assert.AreEqual(parts2, parts3);
        }

        /// <summary>
        /// Tests attribute node filter matches the specified attribute.
        /// </summary>
        [Test]
        public void TestAttributeNodeFilterMatchesCustomNode()
        {
            const int itemsCount = 10;

            var cacheCfg = new CacheConfiguration
            {
                Name = Guid.NewGuid().ToString(),
                NodeFilter = new AttributeNodeFilter(AttrKey2, AttrVal2),
                CacheMode = CacheMode.Replicated,
            };
            var cache = _grid1.CreateCache<object, object>(cacheCfg);

            for (int i = 0; i < itemsCount; i++)
            {
                cache.Put(i, i);
            }

            Assert.AreEqual(2, _grid1.GetCluster().ForDataNodes(cache.Name).GetNodes().Count);

            Assert.AreEqual(0, cache.GetLocalEntries().Count());

            var cache2 = _grid2.GetCache<object, object>(cache.Name);
            var cache3 = _grid2.GetCache<object, object>(cache.Name);

            Assert.AreEqual(itemsCount, cache2.GetLocalEntries().Count());
            Assert.AreEqual(itemsCount, cache3.GetLocalEntries().Count());
        }

        /// <summary>
        /// Tests node filter with multiple attributes matches single node.
        /// </summary>
        [Test]
        public void TestNodeFilterWithMultipleUserAttributes()
        {
            var cacheCfg = new CacheConfiguration
            {
                Name = Guid.NewGuid().ToString(),
                NodeFilter = new AttributeNodeFilter
                {
                    Attributes = new Dictionary<string, object>
                    {
                        {AttrKey2, AttrVal2},
                        {AttrKey3, AttrVal3}
                    }
                },
                CacheMode = CacheMode.Replicated,
            };
            var cache = _grid1.CreateCache<object, object>(cacheCfg);

            ICollection<IClusterNode> dataNodes = _grid1.GetCluster().ForDataNodes(cache.Name).GetNodes();
            Assert.AreEqual(1, dataNodes.Count);
            Assert.AreEqual(_grid3.GetCluster().GetLocalNode(), dataNodes.Single());
        }

        /// <summary>
        /// Tests Java and .NET nodes can utilize the same
        /// attribute node filter configuration.
        /// </summary>
        [Test]
        public void TestSpringAttributeNodeFilter()
        {
            var cache = _grid1.GetCache<object, object>("cache");
            Assert.AreEqual(2, _grid1.GetCluster().ForDataNodes(cache.Name).GetNodes().Count);

            var nodeFilter = cache.GetConfiguration().NodeFilter as AttributeNodeFilter;
            Assert.IsNotNull(nodeFilter);

            Assert.AreEqual(1, nodeFilter.Attributes.Count);

            var expected = new KeyValuePair<string, object>(AttrKey3, AttrVal3);
            Assert.AreEqual(expected, nodeFilter.Attributes.Single());
        }

        /// <summary>
        /// Tests that java node filter is not being read on .NET side.
        /// </summary>
        [Test]
        public void TestJavaNodeFilterIsNotAccessedByNetConfig()
        {
            var cache = _grid1.GetCache<object, object>("cacheWithJavaFilter");

            Assert.IsNull(cache.GetConfiguration().NodeFilter);
        }

        /// <summary>
        /// Tests that custom node filter is not supported.
        /// </summary>
        [Test]
        public void TestCustomFilterIsNotSupported()
        {
            var cacheCfg = new CacheConfiguration
            {
                Name = Guid.NewGuid().ToString(),
                CacheMode = CacheMode.Replicated,
                NodeFilter = new CustomFilter()
            };

            TestDelegate action = () => { _grid1.CreateCache<object, object>(cacheCfg); };

            var ex = Assert.Throws<NotSupportedException>(action);
            Assert.AreEqual("Unsupported CacheConfiguration.NodeFilter: " +
                            "'CustomFilter'. " +
                            "Only predefined implementations are supported: " +
                            "'AttributeNodeFilter'", ex.Message);
        }

        /// <summary>
        /// Tests that attribute node filter with <code>Null</code>
        /// Attributes value is not supported.
        /// </summary>
        [Test]
        public void TestAttributeFilterWithNullValues()
        {
            TestDelegate action = () =>
            {
                var _ = new CacheConfiguration
                {
                    NodeFilter = new AttributeNodeFilter
                    {
                        Attributes = null
                    },
                };
            };

            var ex = Assert.Throws<ArgumentNullException>(action);
            StringAssert.Contains("value", ex.Message);
        }

        /// <summary>
        /// Custom node filter.
        /// </summary>
        public class CustomFilter : IClusterNodeFilter
        {
            /// <summary>
            /// <inheritdoc cref="IClusterNodeFilter.Invoke"/>
            /// </summary>
            public bool Invoke(IClusterNode node)
            {
                return true;
            }
        }
    }
}