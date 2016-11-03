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
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Eviction;
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

            Directory.Delete(_tempDir);
        }

        /// <summary>
        /// Tests that swap space is disabled by default and cache can't have EnableSwap.
        /// </summary>
        [Test]
        public void TestDisabledSwapSpace()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration());

            Assert.IsNull(cfg.SwapSpaceSpi);

            using (var ignite = Ignition.Start(cfg))
            {
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
                var cache = ignite.CreateCache<int, byte[]>(new CacheConfiguration("cache")
                {
                    EnableSwap = true,
                    EvictionPolicy = new LruEvictionPolicy
                    {
                        MaxSize = 3
                    },
                    OffHeapMaxMemory = 5 * 1024
                });

                var data = Enumerable.Range(1, 1024).Select(x => (byte) x).ToArray();

                for (int i = 0; i < 10; i++)
                    cache[i] = data;

                var files = Directory.GetFiles(_tempDir, "*.*", SearchOption.AllDirectories);
                CollectionAssert.IsNotEmpty(files);
            }
        }
    }
}
