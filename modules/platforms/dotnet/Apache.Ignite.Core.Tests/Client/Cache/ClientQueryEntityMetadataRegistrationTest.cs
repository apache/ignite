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
#pragma warning disable 649
namespace Apache.Ignite.Core.Tests.Client.Cache
{
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Cache;
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
    public class ClientQueryEntityMetadataRegistrationTest
    {
        /// <summary>
        /// Fixture set up.
        /// </summary>
        [TestFixtureSetUp]
        public virtual void FixtureSetUp()
        {
            Ignition.Start(TestUtils.GetTestConfiguration());
        }

        /// <summary>
        /// Fixture tear down.
        /// </summary>
        [TestFixtureTearDown]
        public virtual void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests that starting a cache from thin client with a <see cref="QueryEntity"/>
        /// causes binary type registration for key and value types.
        /// <para />
        /// * Connect .NET thin client to a Java-only node
        /// * Start a new cache with code configuration from thin client
        /// * Check that key and value types are registered in the cluster correctly
        /// </summary>
        [Test]
        public void TestCacheStartFromThinClientRegistersMetaForQueryEntityTypes()
        {
            var cfg = new CacheClientConfiguration
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

            using (var client = Ignition.StartClient(new IgniteClientConfiguration("localhost:10800..10801")))
            {
                client.CreateCache<Key1, Value1>(cfg);

                var type = client.GetBinary().GetBinaryType(typeof(Key1));

                Assert.AreEqual("Bar", type.AffinityKeyFieldName);
            }
        }

        /** */
        private class Key1
        {
            /** */
            [QuerySqlField]
            public string Foo;

            /** */
            [AffinityKeyMapped]
            public int Bar;
        }

        /** */
        private class Value1
        {
            /** */
            [QuerySqlField]
            public string Name { get; set; }

            /** */
            [QuerySqlField]
            public long Value { get; set; }
        }
    }
}
