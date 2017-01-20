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

// ReSharper disable UnusedAutoPropertyAccessor.Local
namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests Data Manipulation Language queries.
    /// </summary>
    public class CacheDmlQueriesTest
    {
        /// <summary>
        /// Sets up test fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(Foo))
                {
                    TypeConfigurations =
                    {
                        new BinaryTypeConfiguration(typeof(Key))
                        {
                            EqualityComparer = new BinaryArrayEqualityComparer()
                        },
                        new BinaryTypeConfiguration(typeof(Key2))
                        {
                            EqualityComparer = new BinaryFieldEqualityComparer("Hi", "Lo")
                        }
                    }
                }
            };

            Ignition.Start(cfg);
        }

        /// <summary>
        /// Tears down test fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests primitive key.
        /// </summary>
        [Test]
        public void TestPrimitiveKey()
        {
            var cfg = new CacheConfiguration("primitive_key", new QueryEntity(typeof(int), typeof(Foo)));
            var cache = Ignition.GetIgnite().CreateCache<int, Foo>(cfg);

            // Test insert.
            var res = cache.QueryFields(new SqlFieldsQuery("insert into foo(_key, id, name) " +
                                                           "values (?, ?, ?), (?, ?, ?)",
                1, 2, "John", 3, 4, "Mary")).GetAll();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(1, res[0].Count);
            Assert.AreEqual(2, res[0][0]);  // 2 affected rows

            var foos = cache.OrderBy(x => x.Key).ToArray();

            Assert.AreEqual(2, foos.Length);

            Assert.AreEqual(1, foos[0].Key);
            Assert.AreEqual(2, foos[0].Value.Id);
            Assert.AreEqual("John", foos[0].Value.Name);

            Assert.AreEqual(3, foos[1].Key);
            Assert.AreEqual(4, foos[1].Value.Id);
            Assert.AreEqual("Mary", foos[1].Value.Name);

            // Test key existence.
            Assert.IsTrue(cache.ContainsKey(1));
            Assert.IsTrue(cache.ContainsKey(3));
        }

        /// <summary>
        /// Tests composite key (which requires QueryField.IsKeyField).
        /// </summary>
        [Test]
        public void TestCompositeKeyArrayEquality()
        {
            var cfg = new CacheConfiguration("composite_key_arr", new QueryEntity(typeof(Key), typeof(Foo)));
            var cache = Ignition.GetIgnite().CreateCache<Key, Foo>(cfg);

            // Test insert.
            var res = cache.QueryFields(new SqlFieldsQuery("insert into foo(hi, lo, id, name) " +
                                               "values (1, 2, 3, 'John'), (4, 5, 6, 'Mary')")).GetAll();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(1, res[0].Count);
            Assert.AreEqual(2, res[0][0]);  // 2 affected rows

            var foos = cache.OrderBy(x => x.Key.Lo).ToArray();

            Assert.AreEqual(2, foos.Length);

            Assert.AreEqual(1, foos[0].Key.Hi);
            Assert.AreEqual(2, foos[0].Key.Lo);
            Assert.AreEqual(3, foos[0].Value.Id);
            Assert.AreEqual("John", foos[0].Value.Name);

            Assert.AreEqual(4, foos[1].Key.Hi);
            Assert.AreEqual(5, foos[1].Key.Lo);
            Assert.AreEqual(6, foos[1].Value.Id);
            Assert.AreEqual("Mary", foos[1].Value.Name);

            // Existence tests check that hash codes are consistent.
            var binary = cache.Ignite.GetBinary();
            var binCache = cache.WithKeepBinary<IBinaryObject, IBinaryObject>();

            Assert.IsTrue(cache.ContainsKey(new Key(2, 1)));
            Assert.IsTrue(cache.ContainsKey(foos[0].Key));
            Assert.IsTrue(binCache.ContainsKey(
                binary.GetBuilder(typeof(Key)).SetField("hi", 1).SetField("lo", 2).Build()));

            Assert.IsTrue(cache.ContainsKey(new Key(5, 4)));
            Assert.IsTrue(cache.ContainsKey(foos[1].Key));
            Assert.IsTrue(binCache.ContainsKey(
                binary.GetBuilder(typeof(Key)).SetField("hi", 4).SetField("lo", 5).Build()));
        }

        /// <summary>
        /// Tests composite key (which requires QueryField.IsKeyField).
        /// </summary>
        [Test]
        public void TestCompositeKeyFieldEquality()
        {
            var cfg = new CacheConfiguration("composite_key_fld", new QueryEntity(typeof(Key2), typeof(Foo)));
            var cache = Ignition.GetIgnite().CreateCache<Key2, Foo>(cfg);

            // Test insert.
            var res = cache.QueryFields(new SqlFieldsQuery("insert into foo(hi, lo, str, id, name) " +
                                               "values (1, 2, 'Foo', 3, 'John'), (4, 5, 'Bar', 6, 'Mary')")).GetAll();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(1, res[0].Count);
            Assert.AreEqual(2, res[0][0]);  // 2 affected rows

            var foos = cache.OrderBy(x => x.Key.Lo).ToArray();

            Assert.AreEqual(2, foos.Length);

            Assert.AreEqual(1, foos[0].Key.Hi);
            Assert.AreEqual(2, foos[0].Key.Lo);
            Assert.AreEqual("Foo", foos[0].Key.Str);
            Assert.AreEqual(3, foos[0].Value.Id);
            Assert.AreEqual("John", foos[0].Value.Name);

            Assert.AreEqual(4, foos[1].Key.Hi);
            Assert.AreEqual(5, foos[1].Key.Lo);
            Assert.AreEqual("Bar", foos[1].Key.Str);
            Assert.AreEqual(6, foos[1].Value.Id);
            Assert.AreEqual("Mary", foos[1].Value.Name);

            // Existence tests check that hash codes are consistent.
            Assert.IsTrue(cache.ContainsKey(new Key2(2, 1, "Foo")));
            Assert.IsTrue(cache.ContainsKey(foos[0].Key));

            Assert.IsTrue(cache.ContainsKey(new Key2(5, 4, "Bar")));
            Assert.IsTrue(cache.ContainsKey(foos[1].Key));
        }

        /// <summary>
        /// Tests the composite key without IsKeyField configuration.
        /// </summary>
        [Test]
        public void TestInvalidCompositeKey()
        {
            var cfg = new CacheConfiguration("invalid_composite_key", new QueryEntity
            {
                KeyTypeName = "Key",
                ValueTypeName = "Foo",
                Fields = new[]
                {
                    new QueryField("Lo", typeof(int)),
                    new QueryField("Hi", typeof(int)),
                    new QueryField("Id", typeof(int)),
                    new QueryField("Name", typeof(string))
                }
            });

            var cache = Ignition.GetIgnite().CreateCache<Key, Foo>(cfg);

            var ex = Assert.Throws<IgniteException>(
                () => cache.QueryFields(new SqlFieldsQuery("insert into foo(lo, hi, id, name) " +
                                                           "values (1, 2, 3, 'John'), (4, 5, 6, 'Mary')")));

            Assert.AreEqual("Ownership flag not set for binary property. Have you set 'keyFields' " +
                            "property of QueryEntity in programmatic or XML configuration?", ex.Message);
        }

        /// <summary>
        /// Tests DML with pure binary cache mode, without classes.
        /// </summary>
        [Test]
        public void TestBinaryMode()
        {
            var cfg = new CacheConfiguration("binary_only", new QueryEntity
            {
                KeyTypeName = "CarKey",
                ValueTypeName = "Car",
                Fields = new[]
                {
                    new QueryField("VIN", typeof(string)) {IsKeyField = true},
                    new QueryField("Id", typeof(int)) {IsKeyField = true},
                    new QueryField("Make", typeof(string)),
                    new QueryField("Year", typeof(int))
                }
            });

            var cache = Ignition.GetIgnite().CreateCache<object, object>(cfg)
                .WithKeepBinary<IBinaryObject, IBinaryObject>();

            var res = cache.QueryFields(new SqlFieldsQuery("insert into car(VIN, Id, Make, Year) " +
                                                           "values ('DLRDMC', 88, 'DeLorean', 1982)")).GetAll();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(1, res[0].Count);
            Assert.AreEqual(1, res[0][0]);

            var car = cache.Single();
            Assert.AreEqual("CarKey", car.Key.GetBinaryType().TypeName);
            Assert.AreEqual("Car", car.Value.GetBinaryType().TypeName);
        }

        /// <summary>
        /// Key.
        /// </summary>
        private struct Key
        {
            public Key(int lo, int hi) : this()
            {
                Lo = lo;
                Hi = hi;
            }

            [QuerySqlField] public int Lo { get; private set; }
            [QuerySqlField] public int Hi { get; private set; }
        }

        /// <summary>
        /// Key.
        /// </summary>
        private struct Key2
        {
            public Key2(int lo, int hi, string str) : this()
            {
                Lo = lo;
                Hi = hi;
                Str = str;
            }

            [QuerySqlField] public int Lo { get; private set; }
            [QuerySqlField] public int Hi { get; private set; }
            [QuerySqlField] public string Str { get; private set; }
        }

        /// <summary>
        /// Value.
        /// </summary>
        private class Foo
        {
            [QuerySqlField] public int Id { get; set; }
            [QuerySqlField] public string Name { get; set; }
        }
    }
}
