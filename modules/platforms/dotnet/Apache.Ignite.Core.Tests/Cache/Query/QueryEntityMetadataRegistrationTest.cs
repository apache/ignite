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

// ReSharper disable UnusedMember.Local
#pragma warning disable 649 // Unassigned field
namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests that <see cref="QueryEntity.KeyTypeName"/> and <see cref="QueryEntity.ValueTypeName"/>
    /// settings trigger binary metadata registration on cache start for the specified types.
    /// <para />
    /// Normally, binary metadata is registered in the cluster when an object of the given type is first serialized
    /// (for cache storage or other purposes - Services, Compute, etc).
    /// However, query engine requires metadata for key/value types on cache start, so an eager registration
    /// should be performed. 
    /// </summary>
    public class QueryEntityMetadataRegistrationTest
    {
        /** */
        private const string CacheName = "cache1";
        
        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public void StartGrids()
        {
            // TODO: Test with type names that can't be resolved
            for (int i = 0; i < 2; i++)
            {
                var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
                {
                    SpringConfigUrl = Path.Combine("Config", "query-entity-metadata-registration.xml"),
                    IgniteInstanceName = i.ToString()
                };

                Ignition.Start(cfg);
            }
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public void StopGrids()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that starting a cache from code with a <see cref="QueryEntity"/> causes binary type registration
        /// for key and value types.
        /// <para />
        /// * Start a new cache with code configuration
        /// * Check that query entity is populated correctly
        /// * Check that key and value types are registered in the cluster 
        /// </summary>
        [Test]
        public void TestCacheStartFromCodeRegistersMetaForQueryEntityTypes()
        {
            var cfg = new CacheConfiguration
            {
                Name = TestUtils.TestName,
                QueryEntities = new[]
                {
                    new QueryEntity
                    {
                        KeyType = typeof(Key1),
                        ValueType = typeof(Value1)
                    }
                }
            };

            Ignition.GetIgnite("0").CreateCache<object, object>(cfg);
            
            foreach (var ignite in Ignition.GetAll())
            {
                // Do not use GetBinaryType which always returns something.
                // Use GetBinaryTypes to make sure that types are actually registered.
                var types = ignite.GetBinary().GetBinaryTypes();
                var qryEntity = ignite.GetCache<object, object>(cfg.Name).GetConfiguration().QueryEntities.Single();

                var keyType = types.Single(t => t.TypeName == qryEntity.KeyTypeName);
                var valType = types.Single(t => t.TypeName == qryEntity.ValueTypeName);

                Assert.AreEqual(typeof(Key1).FullName, qryEntity.KeyTypeName);
                Assert.AreEqual(typeof(Value1).FullName, qryEntity.ValueTypeName);

                Assert.AreEqual("Bar", keyType.AffinityKeyFieldName);
                Assert.IsEmpty(keyType.Fields);

                Assert.IsNull(valType.AffinityKeyFieldName);
                Assert.IsEmpty(valType.Fields);
            }
        }

        /// <summary>
        /// Tests that starting a cache from Spring XML with a <see cref="QueryEntity"/> causes binary type registration
        /// for key and value types.
        /// <para />
        /// * Get the cache started from Spring XML
        /// * Check that query entity is populated correctly
        /// * Check that key and value types are registered in the cluster 
        /// </summary>
        [Test]
        public void TestCacheStartFromSpringRegistersMetaForQueryEntityTypes()
        {
            foreach (var ignite in Ignition.GetAll())
            {
                // Do not use GetBinaryType which always returns something.
                // Use GetBinaryTypes to make sure that types are actually registered.
                var types = ignite.GetBinary().GetBinaryTypes();
                var qryEntity = ignite.GetCache<object, object>(CacheName).GetConfiguration().QueryEntities.Single();

                var keyType = types.Single(t => t.TypeName == qryEntity.KeyTypeName);
                var valType = types.Single(t => t.TypeName == qryEntity.ValueTypeName);

                Assert.AreEqual(typeof(Key2).FullName, qryEntity.KeyTypeName);
                Assert.AreEqual(typeof(Value2).FullName, qryEntity.ValueTypeName);

                Assert.AreEqual("AffKey", keyType.AffinityKeyFieldName);
                Assert.IsEmpty(keyType.Fields);

                Assert.IsNull(valType.AffinityKeyFieldName);
                Assert.IsEmpty(valType.Fields);
            }
        }

        /** */
        private class Key1
        {
            /** */
            public string Foo;

            /** */
            [AffinityKeyMapped]
            public int Bar;
        }
        
        /** */
        private class Value1
        {
            /** */
            public string Name { get; set; }
            
            /** */
            public long Value { get; set; }
        }
        
        /** */
        private class Key2
        {
            /** */
            public string Baz;

            /** */
            [AffinityKeyMapped]
            public long AffKey;
        }
        
        /** */
        private class Value2
        {
            /** */
            public string Name { get; set; }
            
            /** */
            public decimal Price { get; set; }
        }
    }
}