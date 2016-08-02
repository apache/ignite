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

// ReSharper disable UnusedAutoPropertyAccessor.Local
namespace Apache.Ignite.Core.Tests.Cache.Affinity
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Tests.Compute;
    using NUnit.Framework;

    /// <summary>
    /// Tests custom affinity mapping.
    /// </summary>
    public class AffinityFieldTest
    {
        /** */
        private ICache<object, string> _cache1;

        /** */
        private ICache<object, string> _cache2;

        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var grid1 = Ignition.Start(GetConfig());
            var grid2 = Ignition.Start(GetConfig("grid2"));

            _cache1 = grid1.CreateCache<object, string>(new CacheConfiguration
            {
                CacheMode = CacheMode.Partitioned
            });
            _cache2 = grid2.GetCache<object, string>(null);
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
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
            // Put keys to update meta
            _cache1.Put(new CacheKey(), string.Empty);
            _cache1.Put(new CacheKeyAttr(), string.Empty);
            _cache1.Put(new CacheKeyAttrOverride(), string.Empty);

            // Verify
            foreach (var type in new[] { typeof(CacheKey) , typeof(CacheKeyAttr), typeof(CacheKeyAttrOverride)})
            {
                Assert.AreEqual("AffinityKey", _cache1.Ignite.GetBinary().GetBinaryType(type).AffinityKeyFieldName);
                Assert.AreEqual("AffinityKey", _cache2.Ignite.GetBinary().GetBinaryType(type).AffinityKeyFieldName);
            }
        }

        /// <summary>
        /// Tests that keys are located properly in cache partitions.
        /// </summary>
        [Test]
        public void TestKeyLocation()
        {
            TestKeyLocation0((key, affKey) => new CacheKey {Key = key, AffinityKey = affKey});
            TestKeyLocation0((key, affKey) => new CacheKeyAttr {Key = key, AffinityKey = affKey});
            TestKeyLocation0((key, affKey) => new CacheKeyAttrOverride {Key = key, AffinityKey = affKey});
        }

        /// <summary>
        /// Tests the <see cref="AffinityKey"/> class.
        /// </summary>
        [Test]
        public void TestAffinityKeyClass()
        {
            // Check location
            TestKeyLocation0((key, affKey) => new AffinityKey(key, affKey));

            // Check meta
            Assert.AreEqual("affKey",
                _cache1.Ignite.GetBinary().GetBinaryType(typeof (AffinityKey)).AffinityKeyFieldName);
        }

        /// <summary>
        /// Tests <see cref="AffinityKey"/> class interoperability.
        /// </summary>
        [Test]
        public void TestInterop()
        {
            var affKey = _cache1.Ignite.GetCompute()
                .ExecuteJavaTask<AffinityKey>(ComputeApiTest.EchoTask, ComputeApiTest.EchoTypeAffinityKey);

            Assert.AreEqual("interopAffinityKey", affKey.Key);
        }

        /// <summary>
        /// Tests the key location.
        /// </summary>
        private void TestKeyLocation0<T>(Func<int, int, T> ctor)
        {
            var aff = _cache1.Ignite.GetAffinity(_cache1.Name);

            foreach (var cache in new[] { _cache1, _cache2 })
            {
                cache.RemoveAll();

                var localNode = cache.Ignite.GetCluster().GetLocalNode();

                var localKeys = Enumerable.Range(1, int.MaxValue)
                    .Where(x => aff.MapKeyToNode(x).Id == localNode.Id).Take(100).ToArray();

                for (int index = 0; index < localKeys.Length; index++)
                {
                    var cacheKey = ctor(index, localKeys[index]);

                    cache.Put(cacheKey, index.ToString());

                    // Verify that key is stored locally according to AffinityKeyFieldName
                    Assert.AreEqual(index.ToString(), cache.LocalPeek(cacheKey, CachePeekMode.Primary));

                    // Other cache does not have this key locally
                    var otherCache = cache == _cache1 ? _cache2 : _cache1;
                    Assert.Throws<KeyNotFoundException>(() => otherCache.LocalPeek(cacheKey, CachePeekMode.All));
                }
            }
        }

        /// <summary>
        /// Gets the configuration.
        /// </summary>
        private static IgniteConfiguration GetConfig(string gridName = null)
        {
            return new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                GridName = gridName,
                BinaryConfiguration = new BinaryConfiguration
                {
                    TypeConfigurations = new[]
                    {
                        new BinaryTypeConfiguration(typeof (CacheKey))
                        {
                            AffinityKeyFieldName = "AffinityKey"
                        },
                        new BinaryTypeConfiguration(typeof(CacheKeyAttr)),
                        new BinaryTypeConfiguration(typeof (CacheKeyAttrOverride))
                        {
                            AffinityKeyFieldName = "AffinityKey"
                        }
                    }
                },
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
