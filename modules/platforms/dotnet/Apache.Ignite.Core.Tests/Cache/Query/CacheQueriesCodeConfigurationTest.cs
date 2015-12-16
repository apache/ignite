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

namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Configuration;
    using NUnit.Framework;

    /// <summary>
    /// Tests queries with in-code configuration.
    /// </summary>
    public class CacheQueriesCodeConfigurationTest
    {
        /// <summary>
        /// Tests the SQL query.
        /// </summary>
        [Test]
        public void TestSqlQuery()
        {
            var cacheName = "personCache";

            var cfg = new IgniteConfiguration
            {
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath(),
                BinaryConfiguration = new BinaryConfiguration(typeof (QueryPerson)),
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = cacheName,
                        QueryEntities = new[]
                        {
                            new QueryEntity
                            {
                                KeyType = typeof (int),
                                ValueType = typeof (QueryPerson),
                                Fields = new[]
                                {
                                    new QueryField("Name", typeof (string)),
                                    new QueryField("Age", typeof (int))
                                },
                                Indexes = new[]
                                {
                                    new QueryIndex("Name", QueryIndexType.FullText), new QueryIndex("Age")
                                }
                            }
                        }
                    }
                }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var cache = ignite.GetCache<int, QueryPerson>(cacheName);

                Assert.IsNotNull(cache);

                cache[1] = new QueryPerson("Arnold", 10);
                cache[2] = new QueryPerson("John", 20);

                using (var cursor = cache.Query(new SqlQuery(typeof (QueryPerson), "age > 10")))
                {
                    Assert.AreEqual(2, cursor.GetAll().Single().Key);
                }

                using (var cursor = cache.Query(new TextQuery(typeof (QueryPerson), "Ar*")))
                {
                    Assert.AreEqual(1, cursor.GetAll().Single().Key);
                }
            }
        }
    }
}
