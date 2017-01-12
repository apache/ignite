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
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl;
    using NUnit.Framework;

    /// <summary>
    /// Tests the cache processor.
    /// </summary>
    public class CacheProcessorTest
    {
        [Test]
        public void TestCacheInvoke()
        {
            using (var ignite1 = Ignition.Start(GetConfig("grid1")))
            using (var ignite2 = Ignition.Start(GetConfig("grid2")))
            {
                var cache = ignite1.GetOrCreateCache<int, PortablePerson>(null);

                const int count = 200;

                for (var i = 0; i < count; i++)
                    cache.Put(i, new PortablePerson { Name = "Person_" + i, Age = i });

                var keys = Enumerable.Range(0, 200).ToArray();

                var results = cache.InvokeAll(keys, new CacheEntryProcessor(), "ProcessedPerson");

                // Check cache state
                foreach (var key in keys)
                {
                    var result = cache.Get(key);

                    Assert.AreEqual(key, result.Age, key.ToString());
                    Assert.AreEqual("ProcessedPerson", result.Name, key.ToString());
                }

                // Check processing results
                foreach (var result in results)
                {
                    Assert.AreEqual(result.Key, result.Value.Result.Age);
                    Assert.AreEqual("ProcessedPerson", result.Value.Result.Name);
                }

            }
        }

        private static IgniteConfiguration GetConfig(string name)
        {
            return new IgniteConfigurationEx
            {
                GridName = name,
                SpringConfigUrl = @"config\cache-processor.xml",
                JvmClasspath = TestUtils.CreateTestClasspath(),
                JvmOptions = TestUtils.TestJavaOptions(),
                BinaryConfiguration =
                    new BinaryConfiguration
                    {
                        TypeConfigurations = new[] {new BinaryTypeConfiguration(typeof (PortablePerson))}
                    }
            };
        }

        /// <summary>
        /// Cache entry processor.
        /// </summary>
        [Serializable]
        public class CacheEntryProcessor : ICacheEntryProcessor<int, PortablePerson, string, PortablePerson>
        {
            /** <inheritdoc /> */
            public PortablePerson Process(IMutableCacheEntry<int, PortablePerson> entry, string arg)
            {
                entry.Value = new PortablePerson { Name = arg, Age = entry.Key };

                return entry.Value;
            }
        }

        /// <summary>
        /// Grid portable person.
        /// </summary>
        public class PortablePerson : IBinarizable
        {
            /// <summary>
            /// Gets or sets the name.
            /// </summary>
            public string Name { get; set; }

            /// <summary>
            /// Gets or sets the age.
            /// </summary>
            public int Age { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteString("name", Name);
                writer.WriteInt("age", Age);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Name = reader.ReadString("name");
                Age = reader.ReadInt("age");
            }
        }
    }
}
