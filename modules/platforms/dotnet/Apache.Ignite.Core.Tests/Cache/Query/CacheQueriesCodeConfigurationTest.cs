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

// ReSharper disable MemberCanBePrivate.Local
// ReSharper disable UnusedAutoPropertyAccessor.Local
// ReSharper disable UnusedMember.Local
namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System;
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
        const string CacheName = "personCache";

        /// <summary>
        /// Tests the SQL query.
        /// </summary>
        [Test]
        public void TestQueryEntityConfiguration()
        {

            var cfg = new IgniteConfiguration
            {
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath(),
                BinaryConfiguration = new BinaryConfiguration(typeof (QueryPerson)),
                CacheConfiguration = new[]
                {
                    new CacheConfiguration
                    {
                        Name = CacheName,
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
                                    new QueryIndex("Name", false, QueryIndexType.FullText), new QueryIndex("Age")
                                }
                            }
                        }
                    }
                }
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var cache = ignite.GetCache<int, QueryPerson>(CacheName);

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

        [Test]
        public void TestAttributeConfiguration()
        {
            // ReSharper disable once ObjectCreationAsStatement
            Assert.Throws<InvalidOperationException>(() => new QueryEntity {ValueType = typeof (RecursiveQuery)});

            var qe = new QueryEntity {ValueType = typeof(AttributeTest) };

            Assert.AreEqual(typeof(AttributeTest), qe.ValueType);

            var fields = qe.Fields.ToArray();

            CollectionAssert.AreEquivalent(new[]
            {
                "SqlField", "IndexedField1", "FullTextField", "Inner", "Inner.Foo",
                "GroupIndex1", "GroupIndex2", "GroupIndex3"
            }, fields.Select(x => x.Name));

            var idx = qe.Indexes.ToArray();

            Assert.AreEqual(QueryIndexType.FullText, idx[0].IndexType);
            Assert.AreEqual(QueryIndexType.FullText, idx[1].IndexType);
            Assert.AreEqual(QueryIndexType.Sorted, idx[2].IndexType);
            Assert.AreEqual(QueryIndexType.FullText, idx[3].IndexType);

            CollectionAssert.AreEquivalent(new[] {"GroupIndex1", "GroupIndex2"}, idx[0].Fields.Select(f => f.Name));
            CollectionAssert.AreEquivalent(new[] {"GroupIndex1", "GroupIndex3"}, idx[1].Fields.Select(f => f.Name));
            CollectionAssert.AreEquivalent(new[] {"IndexedField1"}, idx[2].Fields.Select(f => f.Name));
            CollectionAssert.AreEquivalent(new[] {"FullTextField"}, idx[3].Fields.Select(f => f.Name));
        }

        [Test]
        public void TestAttributeConfigurationQuery()
        {
            var cfg = new IgniteConfiguration
            {
                JvmOptions = TestUtils.TestJavaOptions(),
                JvmClasspath = TestUtils.CreateTestClasspath(),
                BinaryConfiguration = new BinaryConfiguration(
                    typeof (AttributeQueryPerson), typeof (AttributeQueryAddress)),
                CacheConfiguration = new[] {new CacheConfiguration(CacheName, typeof (AttributeQueryPerson))}
            };

            using (var ignite = Ignition.Start(cfg))
            {
                var cache = ignite.GetOrCreateCache<int, AttributeQueryPerson>(CacheName);

                Assert.IsNotNull(cache);

                cache[1] = new AttributeQueryPerson("Arnold", 10)
                {
                    Address = new AttributeQueryAddress {Country = "USA", Street = "Pine Tree road"}
                };

                cache[2] = new AttributeQueryPerson("John", 20);

                using (var cursor = cache.Query(new SqlQuery(typeof(AttributeQueryPerson), "age > ?", 10)))
                {
                    Assert.AreEqual(2, cursor.GetAll().Single().Key);
                }

                using (var cursor = cache.Query(new SqlQuery(typeof(AttributeQueryPerson), "Country = ?", "USA")))
                {
                    Assert.AreEqual(1, cursor.GetAll().Single().Key);
                }

                using (var cursor = cache.Query(new TextQuery(typeof(AttributeQueryPerson), "Ar*")))
                {
                    Assert.AreEqual(1, cursor.GetAll().Single().Key);
                }
            }
        }

        private class AttributeQueryPerson
        {
            public AttributeQueryPerson(string name, int age)
            {
                Name = name;
                Age = age;
            }

            [QueryField(IsIndexed = true, IndexType = QueryIndexType.FullText)]
            public string Name { get; set; }

            [QueryField]
            public int Age { get; set; }

            [QueryField]
            public AttributeQueryAddress Address { get; set; }
        }

        private class AttributeQueryAddress
        {
            [QueryField]
            public string Country { get; set; }

            [QueryField]
            public string Street { get; set; }
        }

        private class RecursiveQuery
        {
            [QueryField]
            public RecursiveQuery Inner { get; set; }
        }

        private class AttributeTest
        {
            [QueryField]
            public double SqlField { get; set; }

            [QueryField(IsIndexed = true, Name = "IndexedField1")]
            public int IndexedField { get; set; }

            [QueryField(IndexType = QueryIndexType.FullText, IsIndexed = true)]
            public string FullTextField { get; set; }

            [QueryField]
            public AttributeTestInner Inner { get; set; }

            [QueryField(IsIndexed = true, IndexGroups = new [] {"group1", "group2"}, 
                IndexType = QueryIndexType.FullText)]
            public string GroupIndex1 { get; set; }

            [QueryField(IsIndexed = true, IndexGroups = new [] {"group1"}, IndexType = QueryIndexType.FullText)]
            public string GroupIndex2 { get; set; }

            [QueryField(IsIndexed = true, IndexGroups = new [] {"group2"}, IndexType = QueryIndexType.FullText)]
            public string GroupIndex3 { get; set; }
        }

        private class AttributeTestInner
        {
            [QueryField]
            public string Foo { get; set; }
        }
    }
}
