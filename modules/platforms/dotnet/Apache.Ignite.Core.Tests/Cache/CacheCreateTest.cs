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
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests CreateCache overloads.
    /// </summary>
    public class CacheCreateTest : TestBase
    {
        /// <summary>
        /// Tests cache creation.
        /// </summary>
        [Test]
        public void TestCreateFromTemplate()
        {
            // Create a cache with random name
            var randomName = "template" + Guid.NewGuid();

            // Can't get non-existent cache with Cache method
            Assert.Throws<ArgumentException>(() => Ignite.GetCache<int, int>(randomName));
            Assert.IsFalse(Ignite.GetCacheNames().Contains(randomName));

            // Create cache and verify template setting.
            var cache = Ignite.CreateCache<int, int>(randomName);
            Assert.AreEqual(3, cache.GetConfiguration().Backups);
            Assert.IsTrue(Ignite.GetCacheNames().Contains(randomName));

            cache.Put(1, 10);
            Assert.AreEqual(10, cache.Get(1));

            // Can't create again
            Assert.Throws<IgniteException>(() => Ignite.CreateCache<int, int>(randomName));

            var cache0 = Ignite.GetCache<int, int>(randomName);
            Assert.AreEqual(10, cache0.Get(1));
        }

        /// <summary>
        /// Tests GetOrCreate.
        /// </summary>
        [Test]
        public void TestGetOrCreateFromTemplate()
        {
            // Create a cache with random name
            var randomName = "template" + Guid.NewGuid();

            // Can't get non-existent cache with Cache method
            Assert.Throws<ArgumentException>(() => Ignite.GetCache<int, int>(randomName));
            
            // Create cache and verify template setting.
            var cache = Ignite.GetOrCreateCache<int, int>(randomName);
            Assert.AreEqual(3, cache.GetConfiguration().Backups);

            cache.Put(1, 10);
            Assert.AreEqual(10, cache.Get(1));

            var cache0 = Ignite.GetOrCreateCache<int, int>(randomName);

            Assert.AreEqual(10, cache0.Get(1));

            var cache1 = Ignite.GetCache<int, int>(randomName);

            Assert.AreEqual(10, cache1.Get(1));
        }

        /// <summary>
        /// Tests dynamic template creation.
        /// </summary>
        [Test]
        public void TestDynamicTemplate()
        {
            var template = new CacheConfiguration
            {
                Name = "dynTempl*",
                Backups = 7,
                RebalanceBatchSize = 1234
            };

            // Register template.
            Ignite.AddCacheConfiguration(template);
            
            // Double registration is allowed.
            Ignite.AddCacheConfiguration(template);
            
            var cache = Ignite.CreateCache<int, int>("dynTempl1");
            Assert.AreEqual(7, cache.GetConfiguration().Backups);
            Assert.AreEqual(1234, cache.GetConfiguration().RebalanceBatchSize);
        }

        /// <summary>
        /// Tests cache destroy.
        /// </summary>
        [Test]
        public void TestDestroy()
        {
            var cacheName = "template" + Guid.NewGuid();

            var ignite = Ignite;

            var cache = ignite.CreateCache<int, int>(cacheName);

            Assert.IsNotNull(ignite.GetCache<int, int>(cacheName));
            Assert.IsTrue(Ignite.GetCacheNames().Contains(cacheName));

            ignite.DestroyCache(cache.Name);

            Assert.IsFalse(Ignite.GetCacheNames().Contains(cacheName));

            var ex = Assert.Throws<ArgumentException>(() => ignite.GetCache<int, int>(cacheName));

            Assert.IsTrue(ex.Message.StartsWith("Cache doesn't exist"));

            Assert.Throws<InvalidOperationException>(() => cache.Get(1));
        }

        /** <inheritdoc /> */
        protected override IgniteConfiguration GetConfig()
        {
            return new IgniteConfiguration(base.GetConfig())
            {
                CacheConfiguration = new []
                {
                    new CacheConfiguration
                    {
                        Name = "template*",
                        Backups = 3
                    } 
                }
            };
        }
    }
}