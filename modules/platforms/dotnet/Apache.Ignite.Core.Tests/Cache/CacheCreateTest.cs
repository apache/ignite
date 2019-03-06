/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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