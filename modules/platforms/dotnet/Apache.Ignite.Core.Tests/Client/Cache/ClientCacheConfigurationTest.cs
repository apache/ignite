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
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client.Cache;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Client;
    using Apache.Ignite.Core.Impl.Client.Cache;
    using Apache.Ignite.Core.Tests.Cache;
    using NUnit.Framework;

    /// <summary>
    /// Tests client cache configuration handling.
    /// </summary>
    public class ClientCacheConfigurationTest
    {
        /// <summary>
        /// Tests the serialization/deserialization of <see cref="CacheConfiguration"/>.
        /// </summary>
        [Test]
        public void TestSerializeDeserialize()
        {
            // Empty.
            TestSerializeDeserialize(new CacheConfiguration("foo"));

            // Full config: has unsupported properties.
            var cfg = CacheConfigurationTest.GetCustomCacheConfiguration("bar");
            cfg.ReadThrough = true;
            cfg.WriteBehindEnabled = true;
            
            TestSerializeDeserializeUnspported(cfg, "AffinityFunction");
            cfg.AffinityFunction = null;

            TestSerializeDeserializeUnspported(cfg, "EvictionPolicy");
            cfg.EvictionPolicy = null;

            TestSerializeDeserializeUnspported(cfg, "ExpiryPolicyFactory");
            cfg.ExpiryPolicyFactory = null;

            TestSerializeDeserializeUnspported(cfg, "PluginConfigurations");
            cfg.PluginConfigurations = null;

            TestSerializeDeserializeUnspported(cfg, "CacheStoreFactory");
            cfg.CacheStoreFactory = null;

            TestSerializeDeserializeUnspported(cfg, "NearConfiguration");
            cfg.NearConfiguration = null;

            // Store-specific properties.
            TestSerializeDeserializeUnspported(cfg, "KeepBinaryInStore");
            cfg.KeepBinaryInStore = false;

            TestSerializeDeserializeUnspported(cfg, "LoadPreviousValue");
            cfg.LoadPreviousValue = false;

            TestSerializeDeserializeUnspported(cfg, "ReadThrough");
            cfg.ReadThrough = false;

            TestSerializeDeserializeUnspported(cfg, "WriteThrough");
            cfg.WriteThrough = false;

            TestSerializeDeserializeUnspported(cfg, "StoreConcurrentLoadAllThreshold");
            cfg.StoreConcurrentLoadAllThreshold = CacheConfiguration.DefaultStoreConcurrentLoadAllThreshold;

            TestSerializeDeserializeUnspported(cfg, "WriteBehindBatchSize");
            cfg.WriteBehindBatchSize = CacheConfiguration.DefaultWriteBehindBatchSize;

            TestSerializeDeserializeUnspported(cfg, "WriteBehindCoalescing");
            cfg.WriteBehindCoalescing = CacheConfiguration.DefaultWriteBehindCoalescing;

            TestSerializeDeserializeUnspported(cfg, "WriteBehindEnabled");
            cfg.WriteBehindEnabled = CacheConfiguration.DefaultWriteBehindEnabled;

            TestSerializeDeserializeUnspported(cfg, "WriteBehindFlushFrequency");
            cfg.WriteBehindFlushFrequency = CacheConfiguration.DefaultWriteBehindFlushFrequency;

            TestSerializeDeserializeUnspported(cfg, "WriteBehindFlushSize");
            cfg.WriteBehindFlushSize = CacheConfiguration.DefaultWriteBehindFlushSize;

            TestSerializeDeserializeUnspported(cfg, "WriteBehindFlushThreadCount");
            cfg.WriteBehindFlushThreadCount = CacheConfiguration.DefaultWriteBehindFlushThreadCount;

            // Full config without unsupported properties.
            TestSerializeDeserialize(cfg);
        }

        /// <summary>
        /// Tests <see cref="CacheConfiguration"/> to <see cref="CacheClientConfiguration"/> and reverse conversion.
        /// </summary>
        [Test]
        public void TestConfigConversion()
        {
            // Copy ctor.
            var clientCfg = new CacheClientConfiguration(
                CacheConfigurationTest.GetCustomCacheConfiguration("z"), true);

            ClientTestBase.AssertClientConfigsAreEqual(clientCfg, new CacheClientConfiguration(clientCfg));

            // Convert to server cfg.
            var serverCfg = clientCfg.ToCacheConfiguration();
            ClientTestBase.AssertClientConfigsAreEqual(clientCfg, new CacheClientConfiguration(serverCfg, false));
        }

        /// <summary>
        /// Tests the constructors.
        /// </summary>
        [Test]
        public void TestConstructors()
        {
            // Default property values.
            var clientCfg = new CacheClientConfiguration();
            var defCfg = new CacheClientConfiguration(new CacheConfiguration(), false);

            ClientTestBase.AssertClientConfigsAreEqual(defCfg, clientCfg);

            // Name.
            clientCfg = new CacheClientConfiguration("foo");
            Assert.AreEqual("foo", clientCfg.Name);

            clientCfg.Name = null;
            ClientTestBase.AssertClientConfigsAreEqual(defCfg, clientCfg);

            // Query entities.
            clientCfg = new CacheClientConfiguration("bar", typeof(QueryPerson));
            Assert.AreEqual("bar", clientCfg.Name);
            var qe = clientCfg.QueryEntities.Single();
            Assert.AreEqual(typeof(QueryPerson), qe.ValueType);
            Assert.AreEqual("Name", qe.Fields.Single().Name);

            clientCfg = new CacheClientConfiguration("baz", new QueryEntity(typeof(QueryPerson)));
            qe = clientCfg.QueryEntities.Single();
            Assert.AreEqual(typeof(QueryPerson), qe.ValueType);
            Assert.AreEqual("Name", qe.Fields.Single().Name);
        }

        /// <summary>
        /// Tests the serialization/deserialization of <see cref="CacheConfiguration"/>.
        /// </summary>
        private static void TestSerializeDeserializeUnspported(CacheConfiguration cfg, string propName)
        {
            var ex = Assert.Throws<NotSupportedException>(() => TestSerializeDeserialize(cfg));
            Assert.AreEqual(string.Format("{0}.{1} property is not supported in thin client mode.",
                typeof(CacheConfiguration).Name, propName), ex.Message);
        }

        /// <summary>
        /// Tests the serialization/deserialization of <see cref="CacheConfiguration"/>.
        /// </summary>
        private static void TestSerializeDeserialize(CacheConfiguration cfg)
        {
            var clientCfg = new CacheClientConfiguration(cfg, false);

            ClientTestBase.AssertClientConfigsAreEqual(clientCfg, SerializeDeserialize(clientCfg));
        }

        /// <summary>
        /// Serializes and deserializes the config.
        /// </summary>
        private static CacheClientConfiguration SerializeDeserialize(CacheClientConfiguration cfg)
        {
            using (var stream = new BinaryHeapStream(128))
            {
                ClientCacheConfigurationSerializer.Write(stream, cfg, ClientSocket.CurrentProtocolVersion, true);
                stream.Seek(0, SeekOrigin.Begin);
                return new CacheClientConfiguration(stream, ClientSocket.CurrentProtocolVersion);
            }
        }

        private class QueryPerson
        {
            [QuerySqlField]
            // ReSharper disable once UnusedMember.Local
            public string Name { get; set; }
        }
    }
}
