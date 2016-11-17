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
    using System.IO;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
    using Apache.Ignite.Core.Discovery.Tcp;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.SwapSpace.File;
    using NUnit.Framework;

    /// <summary>
    /// Tests the swap space.
    /// </summary>
    public class CacheSwapSpaceTest
    {
        /** */
        private readonly string _tempDir = IgniteUtils.GetTempDirectoryName();

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);

            Directory.Delete(_tempDir, true);
        }

        /// <summary>
        /// Tests that swap space is disabled by default and cache can't have EnableSwap.
        /// </summary>
        [Test]
        public void TestDisabledSwapSpace()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration());

            using (var ignite = Ignition.Start(cfg))
            {
                // NoopSwapSpaceSpi is used by default.
                Assert.IsNull(ignite.GetConfiguration().SwapSpaceSpi);

                var ex = Assert.Throws<CacheException>(
                    () => ignite.CreateCache<int, int>(new CacheConfiguration {EnableSwap = true}));

                Assert.IsTrue(ex.Message.EndsWith("has not swap SPI configured"));
            }
        }

        /// <summary>
        /// Tests the swap space.
        /// </summary>
        [Test]
        public void TestSwapSpace()
        {
            const int entrySize = 1024;

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SwapSpaceSpi = new FileSwapSpaceSpi
                {
                    BaseDirectory = _tempDir,
                    WriteBufferSize = 64
                }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                // Create cache with eviction and swap.
                var cache = ignite.CreateCache<int, byte[]>(new CacheConfiguration("cache")
                {
                    EnableSwap = true,
                    EvictionPolicy = new LruEvictionPolicy
                    {
                        MaxSize = 3
                    },
                    OffHeapMaxMemory = 5 * entrySize
                });

                // Populate to trigger eviction.
                var data = Enumerable.Range(1, entrySize).Select(x => (byte) x).ToArray();

                for (int i = 0; i < 10; i++)
                    cache[i] = data;

                // Check that swap files exist.
                var files = Directory.GetFiles(_tempDir, "*.*", SearchOption.AllDirectories);
                CollectionAssert.IsNotEmpty(files);
                
                // Wait for metrics update and check metrics.
                Thread.Sleep(((TcpDiscoverySpi) ignite.GetConfiguration().DiscoverySpi).HeartbeatFrequency);

                var metrics = cache.GetLocalMetrics();

                Assert.AreEqual(4, metrics.OffHeapEntriesCount);  // Entry takes more space than the value
                Assert.AreEqual(3, metrics.SwapEntriesCount);  // 10 - 3 - 4 = 3
                Assert.AreEqual(3, metrics.OverflowSize / entrySize);
                Assert.AreEqual(metrics.SwapSize, metrics.OverflowSize);
            }
        }
    }
}
