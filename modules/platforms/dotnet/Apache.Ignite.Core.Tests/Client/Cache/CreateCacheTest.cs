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

namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using System;
    using System.Linq;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Expiry;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Configuration;
    using Apache.Ignite.Core.Impl.Cache.Expiry;
    using Apache.Ignite.Core.Impl.Client.Cache;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Tests.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests dynamic cache start from client nodes.
    /// </summary>
    public class CreateCacheTest : ClientTestBase
    {
        /** Template cache name. */
        private const string TemplateCacheName = "template-cache-*";

        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            DestroyCaches();
        }

        /// <summary>
        /// Destroys caches.
        /// </summary>
        private void DestroyCaches()
        {
            foreach (var cacheName in Client.GetCacheNames())
            {
                Client.DestroyCache(cacheName);
            }
        }

        /// <summary>
        /// Tests the GetCacheNames.
        /// </summary>
        [Test]
        public void TestGetCacheNames()
        {
            DestroyCaches();
            Assert.AreEqual(0, Client.GetCacheNames().Count);

            Client.CreateCache<int, int>("a");
            Assert.AreEqual("a", Client.GetCacheNames().Single());

            Client.CreateCache<int, int>("b");
            Assert.AreEqual(new[] {"a", "b"}, Client.GetCacheNames().OrderBy(x => x).ToArray());

            Client.DestroyCache("a");
            Assert.AreEqual("b", Client.GetCacheNames().Single());
        }

        /// <summary>
        /// Tests create from template.
        /// </summary>
        [Test]
        public void TestCreateFromTemplate()
        {
            // No template: default configuration.
            var cache = Client.CreateCache<int, int>("foobar");
            AssertExtensions.ReflectionEqual(new CacheClientConfiguration("foobar"), cache.GetConfiguration());

            // Create when exists.
            var ex = Assert.Throws<IgniteClientException>(() => Client.CreateCache<int, int>(cache.Name));
            Assert.AreEqual(
                "Failed to start cache (a cache with the same name is already started): foobar", ex.Message);
            Assert.AreEqual(ClientStatusCode.CacheExists, ex.StatusCode);

            // Template: custom configuration.
            cache = Client.CreateCache<int, int>(TemplateCacheName.Replace("*", "1"));
            var cfg = cache.GetConfiguration();
            Assert.AreEqual(CacheAtomicityMode.Transactional, cfg.AtomicityMode);
            Assert.AreEqual(3, cfg.Backups);
            Assert.AreEqual(CacheMode.Partitioned, cfg.CacheMode);
        }

        /// <summary>
        /// Tests getOrCreate from template.
        /// </summary>
        [Test]
        public void TestGetOrCreateFromTemplate()
        {
            // No template: default configuration.
            var cache = Client.GetOrCreateCache<int, int>("foobar");
            AssertExtensions.ReflectionEqual(new CacheClientConfiguration { Name = "foobar"}, cache.GetConfiguration());
            cache[1] = 1;

            // Create when exists.
            cache = Client.GetOrCreateCache<int, int>("foobar");
            Assert.AreEqual(1, cache[1]);

            // Template: custom configuration.
            cache = Client.GetOrCreateCache<int, int>(TemplateCacheName.Replace("*", "1"));
            var cfg = cache.GetConfiguration();
            Assert.AreEqual(CacheAtomicityMode.Transactional, cfg.AtomicityMode);
            Assert.AreEqual(3, cfg.Backups);
            Assert.AreEqual(CacheMode.Partitioned, cfg.CacheMode);

            // Create when exists.
            cache[1] = 1;
            cache = Client.GetOrCreateCache<int, int>(cache.Name);
            Assert.AreEqual(1, cache[1]);
        }

        /// <summary>
        /// Tests cache creation from configuration.
        /// </summary>
        [Test]
        public void TestCreateFromConfiguration()
        {
            // Default config.
            var cfg = new CacheClientConfiguration("a");
            var cache = Client.CreateCache<int, int>(cfg);
            AssertExtensions.ReflectionEqual(cfg, cache.GetConfiguration());

            // Create when exists.
            var ex = Assert.Throws<IgniteClientException>(() => Client.CreateCache<int, int>(cfg));
            Assert.AreEqual(
                "Failed to start cache (a cache with the same name is already started): a", ex.Message);
            Assert.AreEqual(ClientStatusCode.CacheExists, ex.StatusCode);

            // Custom config.
            cfg = GetFullCacheConfiguration("b");

            cache = Client.CreateCache<int, int>(cfg);
            AssertClientConfigsAreEqual(cfg, cache.GetConfiguration());
        }

        /// <summary>
        /// Test cache creation from configuration with expiry policy.
        /// </summary>
        [Test]
        public void TestCreateFromConfigurationWithExpiration()
        {
            var expiryPolicy = new ExpiryPolicy(
                TimeSpan.FromMilliseconds(20),
                TimeSpan.FromMilliseconds(10),
                TimeSpan.FromMilliseconds(30));

            // Default config.
            var cfg = new CacheClientConfiguration("a")
            {
                ExpiryPolicyFactory = new ExpiryPolicyFactory(expiryPolicy)
            };
            var cache = Client.CreateCache<int, int>(cfg);
            AssertExtensions.ReflectionEqual(cfg, cache.GetConfiguration());

            var remoteExpiryPolicy  = cache.GetConfiguration().ExpiryPolicyFactory.CreateInstance();
            AssertExtensions.ReflectionEqual(expiryPolicy, remoteExpiryPolicy);

            cache.Put(1, 1);

            // Wait for expiration period.
            Thread.Sleep(100);

            Assert.IsFalse(cache.ContainsKey(1));
        }

        /// <summary>
        /// Tests cache creation from partial configuration.
        /// </summary>
        [Test]
        public void TestCreateFromPartialConfiguration()
        {
            // Default config.
            var cfg = new CacheClientConfiguration("a") {Backups = 7};
            var client = (IgniteClient) Client;

            // Create cache directly through a socket with only some config properties provided.
            client.Socket.DoOutInOp<object>(ClientOp.CacheCreateWithConfiguration, ctx =>
            {
                var w = ctx.Writer;

                w.WriteInt(2 + 2 + 6 + 2 + 4);  // config length in bytes.

                w.WriteShort(2);  // 2 properties.
                
                w.WriteShort(3);  // backups opcode.
                w.WriteInt(cfg.Backups);

                w.WriteShort(0);  // name opcode.
                w.WriteString(cfg.Name);

            }, null);
            
            var cache = new CacheClient<int, int>(client, cfg.Name);

            AssertExtensions.ReflectionEqual(cfg, cache.GetConfiguration());
        }

        /// <summary>
        /// Tests cache creation from configuration.
        /// </summary>
        [Test]
        public void TestGetOrCreateFromConfiguration()
        {
            // Default configur.
            var cfg = new CacheClientConfiguration("a");
            var cache = Client.GetOrCreateCache<int, int>(cfg);
            AssertExtensions.ReflectionEqual(cfg, cache.GetConfiguration());
            cache[1] = 1;

            // Create when exists.
            cache = Client.GetOrCreateCache<int, int>("a");
            Assert.AreEqual(1, cache[1]);

            // Custom config.
            cfg = GetFullCacheConfiguration("b");

            cache = Client.GetOrCreateCache<int, int>(cfg);
            AssertClientConfigsAreEqual(cfg, cache.GetConfiguration());
        }

        /// <summary>
        /// Gets the full cache configuration.
        /// </summary>
        private static CacheClientConfiguration GetFullCacheConfiguration(string name)
        {
            return new CacheClientConfiguration(CacheConfigurationTest.GetCustomCacheConfiguration(name), true);
        }

        /** <inheritdoc /> */
        protected override IgniteConfiguration GetIgniteConfiguration()
        {
            return new IgniteConfiguration(base.GetIgniteConfiguration())
            {
                CacheConfiguration = new[]
                {
                    new CacheConfiguration(TemplateCacheName)
                    {
                        AtomicityMode = CacheAtomicityMode.Transactional,
                        Backups = 3
                    }
                },
                DataStorageConfiguration = new DataStorageConfiguration
                {
                    DataRegionConfigurations = new[]
                    {
                        new DataRegionConfiguration
                        {
                            Name = "myMemPolicy"
                        } 
                    }
                }
            };
        }
    }
}
