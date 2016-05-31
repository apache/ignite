﻿/*
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

namespace Apache.Ignite.Core.Tests.EntityFramework
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.EntityFramework;
    using NUnit.Framework;

    /// <summary>
    /// Tests the EF cache provider.
    /// </summary>
    public class SecondLevelCacheTest
    {
        /// <summary>
        /// Fixture setup.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            Environment.SetEnvironmentVariable(Classpath.EnvIgniteNativeTestClasspath, "true");
        }

        [Test]
        [SuppressMessage("ReSharper", "ObjectCreationAsStatement")]
        public void TestConfiguration()
        {
            Assert.IsNull(Ignition.TryGetIgnite());

            // Test default config (picks up app.config section)
            CheckCacheAndStop("myGrid1", IgniteDbConfiguration.DefaultCacheName, new IgniteDbConfiguration());

            // Specific config section
            CheckCacheAndStop("myGrid2", IgniteDbConfiguration.DefaultCacheName, 
                new IgniteDbConfiguration("igniteConfiguration2", "cacheName2", null));

            // Specific config section, nonexistent cache
            CheckCacheAndStop("myGrid2", IgniteDbConfiguration.DefaultCacheName, 
                new IgniteDbConfiguration("igniteConfiguration2", "newCache", null));

            // In-code configuration
            CheckCacheAndStop("myGrid3", IgniteDbConfiguration.DefaultCacheName,
                new IgniteDbConfiguration(new IgniteConfiguration
                {
                    GridName = "myGrid3",
                    CacheConfiguration = new[]
                    {
                        new CacheConfiguration
                        {
                            Name = "myCache",
                            CacheMode = CacheMode.Replicated
                        }
                    }
                }, "myCache", null), CacheMode.Replicated);

            // Existing instance
        }

        // ReSharper disable once UnusedParameter.Local
        private static void CheckCacheAndStop(string gridName, string cacheName, IgniteDbConfiguration cfg,
            CacheMode cacheMode = CacheMode.Partitioned)
        {
            Assert.IsNotNull(cfg);

            var ignite = Ignition.TryGetIgnite(gridName);
            Assert.IsNotNull(ignite);

            var cache = ignite.GetCache<object, object>(cacheName);
            Assert.IsNotNull(cache);
            Assert.AreEqual(cacheMode, cache.GetConfiguration().CacheMode);

            Ignition.StopAll(true);
        }

        [Test]
        public void TestBasic()
        {

            
        }

        [Test]
        public void TestExpiration()
        {
            
        }
    }
}
