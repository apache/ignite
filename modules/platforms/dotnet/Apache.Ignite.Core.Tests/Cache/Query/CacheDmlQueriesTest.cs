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
    using System;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Common;
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
                // Type registration is required because we call DML first and cache.Get after.
                BinaryConfiguration = new BinaryConfiguration(typeof(Foo), typeof(Key), typeof(Key2), typeof(KeyAll))
                {
                    NameMapper = GetNameMapper()
                }
            };

            Ignition.Start(cfg);
        }

        /// <summary>
        /// Gets the name mapper.
        /// </summary>
        protected virtual IBinaryNameMapper GetNameMapper()
        {
            return new BinaryBasicNameMapper {IsSimpleName = false};
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
            var res = cache.Query(new SqlFieldsQuery("insert into foo(_key, id, name) " +
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
        /// Tests the NotNull constraint.
        /// </summary>
        [Test]
        public void TestNotNull()
        {
            var cfg = new CacheConfiguration("not_null", new QueryEntity(typeof(int), typeof(Foo))
            {
                Fields = new[]
                {
                    new QueryField("id", typeof(int)) {NotNull = true}, 
                    new QueryField("name", typeof(string)) 
                }
            });

            var cache = Ignition.GetIgnite().CreateCache<int, Foo>(cfg);

            var ex = Assert.Throws<IgniteException>(() => cache.Query(new SqlFieldsQuery(
                "insert into foo(_key, name) values (?, ?)", 1, "bar")).GetAll());

            Assert.AreEqual("Null value is not allowed for column 'ID'", ex.Message);
        }

        /// <summary>
        /// Tests the NotNull constraint.
        /// </summary>
        [Test]
        public void TestNotNullAttribute()
        {
            var cfg = new CacheConfiguration("not_null_attr", new QueryEntity(typeof(int), typeof(Foo)));
            var cache = Ignition.GetIgnite().CreateCache<int, Foo>(cfg);

            var ex = Assert.Throws<IgniteException>(() => cache.Query(new SqlFieldsQuery(
                "insert into foo(_key, id) values (?, ?)", 1, 2)).GetAll());

            Assert.AreEqual("Null value is not allowed for column 'NAME'", ex.Message);
        }

        /// <summary>
        /// Tests all primitive key types.
        /// </summary>
        [Test]
        public void TestPrimitiveKeyAllTypes()
        {
            TestKey(byte.MinValue, byte.MaxValue);
            TestKey(sbyte.MinValue, sbyte.MaxValue);
            TestKey(short.MinValue, short.MaxValue);
            TestKey(int.MinValue, int.MaxValue);
            TestKey(uint.MinValue, uint.MaxValue);
            TestKey(long.MinValue, long.MaxValue);
            TestKey(ulong.MinValue, ulong.MaxValue);
            TestKey(float.MinValue, float.MaxValue);
            TestKey(double.MinValue, double.MaxValue);
            TestKey(decimal.MinValue, decimal.MaxValue);
            TestKey(Guid.NewGuid());
        }

        /// <summary>
        /// Tests the key.
        /// </summary>
        private static void TestKey<T>(params T[] vals)
        {
            var cfg = new CacheConfiguration("primitive_key_dotnet_" + typeof(T), 
                new QueryEntity(typeof(T), typeof(string)));
            var cache = Ignition.GetIgnite().CreateCache<T, string>(cfg);

            foreach (var val in vals)
            {
                var res = cache.Query(new SqlFieldsQuery(
                    "insert into string(_key, _val) values (?, ?)", val, val.ToString())).GetAll();

                Assert.AreEqual(1, res.Count);
                Assert.AreEqual(1, res[0].Count);
                Assert.AreEqual(1, res[0][0]);

                Assert.AreEqual(val.ToString(), cache[val]);
            }
        }

        /// <summary>
        /// Tests composite key (which requires QueryField.IsKeyField).
        /// </summary>
        [Test]
        public void TestCompositeKey()
        {
            var cfg = new CacheConfiguration("composite_key_arr", new QueryEntity(typeof(Key), typeof(Foo)));
            var cache = Ignition.GetIgnite().CreateCache<Key, Foo>(cfg);

            // Test insert.
            var res = cache.Query(new SqlFieldsQuery("insert into foo(hi, lo, id, name) " +
                                               "values (-1, 65500, 3, 'John'), (255, 128, 6, 'Mary')")).GetAll();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(1, res[0].Count);
            Assert.AreEqual(2, res[0][0]);  // 2 affected rows

            var foos = cache.OrderByDescending(x => x.Key.Lo).ToArray();

            Assert.AreEqual(2, foos.Length);

            Assert.AreEqual(-1, foos[0].Key.Hi);
            Assert.AreEqual(65500, foos[0].Key.Lo);
            Assert.AreEqual(3, foos[0].Value.Id);
            Assert.AreEqual("John", foos[0].Value.Name);

            Assert.AreEqual(255, foos[1].Key.Hi);
            Assert.AreEqual(128, foos[1].Key.Lo);
            Assert.AreEqual(6, foos[1].Value.Id);
            Assert.AreEqual("Mary", foos[1].Value.Name);

            // Existence tests check that hash codes are consistent.
            var binary = cache.Ignite.GetBinary();
            var binCache = cache.WithKeepBinary<IBinaryObject, IBinaryObject>();

            Assert.IsTrue(cache.ContainsKey(new Key(65500, -1)));
            Assert.IsTrue(cache.ContainsKey(foos[0].Key));
            Assert.IsTrue(binCache.ContainsKey(
                binary.GetBuilder(typeof(Key)).SetField("hi", -1).SetField("lo", 65500).Build()));
            Assert.IsTrue(binCache.ContainsKey(  // Fields are sorted.
                binary.GetBuilder(typeof(Key)).SetField("lo", 65500).SetField("hi", -1).Build()));

            Assert.IsTrue(cache.ContainsKey(new Key(128, 255)));
            Assert.IsTrue(cache.ContainsKey(foos[1].Key));
            Assert.IsTrue(binCache.ContainsKey(
                binary.GetBuilder(typeof(Key)).SetField("hi", 255).SetField("lo", 128).Build()));
            Assert.IsTrue(binCache.ContainsKey(  // Fields are sorted.
                binary.GetBuilder(typeof(Key)).SetField("lo", 128).SetField("hi", 255).Build()));
        }

        /// <summary>
        /// Tests composite key (which requires QueryField.IsKeyField).
        /// </summary>
        [Test]
        public void TestCompositeKey2()
        {
            var cfg = new CacheConfiguration("composite_key_fld", new QueryEntity(typeof(Key2), typeof(Foo)));
            var cache = Ignition.GetIgnite().CreateCache<Key2, Foo>(cfg);

            // Test insert.
            var res = cache.Query(new SqlFieldsQuery("insert into foo(hi, lo, str, id, name) " +
                                               "values (1, 2, 'Фу', 3, 'John'), (4, 5, 'Бар', 6, 'Mary')")).GetAll();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(1, res[0].Count);
            Assert.AreEqual(2, res[0][0]);  // 2 affected rows

            var foos = cache.OrderBy(x => x.Key.Lo).ToArray();

            Assert.AreEqual(2, foos.Length);

            Assert.AreEqual(1, foos[0].Key.Hi);
            Assert.AreEqual(2, foos[0].Key.Lo);
            Assert.AreEqual("Фу", foos[0].Key.Str);
            Assert.AreEqual(3, foos[0].Value.Id);
            Assert.AreEqual("John", foos[0].Value.Name);

            Assert.AreEqual(4, foos[1].Key.Hi);
            Assert.AreEqual(5, foos[1].Key.Lo);
            Assert.AreEqual("Бар", foos[1].Key.Str);
            Assert.AreEqual(6, foos[1].Value.Id);
            Assert.AreEqual("Mary", foos[1].Value.Name);

            // Existence tests check that hash codes are consistent.
            Assert.IsTrue(cache.ContainsKey(new Key2(2, 1, "Фу")));
            Assert.IsTrue(cache.ContainsKey(foos[0].Key));

            Assert.IsTrue(cache.ContainsKey(new Key2(5, 4, "Бар")));
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
                    // Next two fields belong to the <see cref="Key"/> object, so should have been marked with <see cref="QueryField.IsKeyField"/>
                    // But if we forgot to do this - all fields are treated as value fields. Key fields have default values and second insert fails.
                    new QueryField("Lo", typeof(int)),
                    new QueryField("Hi", typeof(int)),

                    new QueryField("Id", typeof(int)),
                    new QueryField("Name", typeof(string))
                }
            });

            var cache = Ignition.GetIgnite().CreateCache<Key, Foo>(cfg);

            var ex = Assert.Throws<IgniteException>(
                () => cache.Query(new SqlFieldsQuery("insert into foo(lo, hi, id, name) " +
                                                           "values (1, 2, 3, 'John'), (4, 5, 6, 'Mary')")));

            StringAssert.StartsWith("Failed to INSERT some keys because they are already in cache", ex.Message);
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

            var res = cache.Query(new SqlFieldsQuery("insert into car(VIN, Id, Make, Year) " +
                                                           "values ('DLRDMC', 88, 'DeLorean', 1982)")).GetAll();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(1, res[0].Count);
            Assert.AreEqual(1, res[0][0]);

            var car = cache.Single();
            Assert.AreEqual("CarKey", car.Key.GetBinaryType().TypeName);
            Assert.AreEqual("Car", car.Value.GetBinaryType().TypeName);
        }

        /// <summary>
        /// Tests the composite key with fields of all data types.
        /// </summary>
        [Test]
        public void TestCompositeKeyAllDataTypes()
        {
            var cfg = new CacheConfiguration("composite_key_all", new QueryEntity(typeof(KeyAll), typeof(string)));
            var cache = Ignition.GetIgnite().CreateCache<KeyAll, string>(cfg);

            var key = new KeyAll
            {
                Byte = byte.MaxValue,
                SByte = sbyte.MaxValue,
                Short = short.MaxValue,
                UShort = ushort.MaxValue,
                Int = int.MaxValue,
                UInt = uint.MaxValue,
                Long = long.MaxValue,
                ULong = ulong.MaxValue,
                Float = float.MaxValue,
                Double = double.MaxValue,
                Decimal = decimal.MaxValue,
                Guid = Guid.NewGuid(),
                String = "привет",
                Key = new Key(255, 65555)
            };

            // Test insert.
            var res = cache.Query(new SqlFieldsQuery(
                    "insert into string(byte, sbyte, short, ushort, int, uint, long, ulong, float, double, decimal, " +
                    "guid, string, key, _val) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    key.Byte, key.SByte, key.Short, key.UShort, key.Int, key.UInt, key.Long, key.ULong, key.Float,
                    key.Double, key.Decimal, key.Guid, key.String, key.Key, "VALUE"))
                .GetAll();

            Assert.AreEqual(1, res.Count);
            Assert.AreEqual(1, res[0].Count);
            Assert.AreEqual(1, res[0][0]);

            // Compare resulting keys.
            Assert.AreEqual(key, cache.Single().Key);

            // Compare keys in binary form.
            var binKey = cache.Ignite.GetBinary().ToBinary<IBinaryObject>(key);
            var binKeyRes = cache.WithKeepBinary<IBinaryObject, string>().Single().Key;

            Assert.AreEqual(binKey, binKeyRes);

            // Get by key to verify identity.
            Assert.AreEqual("VALUE", cache[key]);
        }

        /// <summary>
        /// Tests the QueryField.DefaultValue functionality.
        /// </summary>
        [Test]
        public void TestDefaultValue()
        {
            // Attribute-based config.
            var cfg = new CacheConfiguration("def_value_attr", new QueryEntity(typeof(int), typeof(Foo)));
            Assert.AreEqual(-1, cfg.QueryEntities.Single().Fields.Single(x => x.Name == "Id").DefaultValue);
            
            var cache = Ignition.GetIgnite().CreateCache<int, Foo>(cfg);
            Assert.AreEqual(-1,
                cache.GetConfiguration().QueryEntities.Single().Fields.Single(x => x.Name == "Id").DefaultValue);

            cache.Query(new SqlFieldsQuery("insert into foo(_key, id, name) values (?, ?, ?)", 1, 2, "John")).GetAll();
            cache.Query(new SqlFieldsQuery("insert into foo(_key, name) values (?, ?)", 3, "Mary")).GetAll();

            Assert.AreEqual(2, cache[1].Id);
            Assert.AreEqual(-1, cache[3].Id);

            // QueryEntity-based config.
            cfg = new CacheConfiguration("def_value_binary", new QueryEntity
            {
                KeyType = typeof(int),
                ValueTypeName = "DefValTest",
                Fields = new[]
                {
                    new QueryField("Name", typeof(string)) {DefaultValue = "foo"}
                }
            });

            var cache2 = Ignition.GetIgnite().CreateCache<int, int>(cfg).WithKeepBinary<int, IBinaryObject>();

            cache2.Query(new SqlFieldsQuery("insert into DefValTest(_key, name) values (?, ?)", 1, "John")).GetAll();
            cache2.Query(new SqlFieldsQuery("insert into DefValTest(_key) values (?)", 2)).GetAll();

            Assert.AreEqual("John", cache2[1].GetField<string>("Name"));
            Assert.AreEqual("foo", cache2[2].GetField<string>("Name"));
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
            [QuerySqlField(DefaultValue = -1)] public int Id { get; set; }
            [QuerySqlField(NotNull = true)] public string Name { get; set; }
        }

        /// <summary>
        /// Key with all kinds of fields.
        /// </summary>
        private class KeyAll
        {
            [QuerySqlField] public byte Byte { get; set; }
            [QuerySqlField] public sbyte SByte { get; set; }
            [QuerySqlField] public short Short { get; set; }
            [QuerySqlField] public ushort UShort { get; set; }
            [QuerySqlField] public int Int { get; set; }
            [QuerySqlField] public uint UInt { get; set; }
            [QuerySqlField] public long Long { get; set; }
            [QuerySqlField] public ulong ULong { get; set; }
            [QuerySqlField] public float Float { get; set; }
            [QuerySqlField] public double Double { get; set; }
            [QuerySqlField] public decimal Decimal { get; set; }
            [QuerySqlField] public Guid Guid { get; set; }
            [QuerySqlField] public string String { get; set; }
            [QuerySqlField] public Key Key { get; set; }

            private bool Equals(KeyAll other)
            {
                return Byte == other.Byte && SByte == other.SByte && Short == other.Short && 
                    UShort == other.UShort && Int == other.Int && UInt == other.UInt && Long == other.Long && 
                    ULong == other.ULong && Float.Equals(other.Float) && Double.Equals(other.Double) && 
                    Decimal == other.Decimal && Guid.Equals(other.Guid) && string.Equals(String, other.String) && 
                    Key.Equals(other.Key);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;
                return Equals((KeyAll) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    var hashCode = Byte.GetHashCode();
                    hashCode = (hashCode * 397) ^ SByte.GetHashCode();
                    hashCode = (hashCode * 397) ^ Short.GetHashCode();
                    hashCode = (hashCode * 397) ^ UShort.GetHashCode();
                    hashCode = (hashCode * 397) ^ Int;
                    hashCode = (hashCode * 397) ^ (int) UInt;
                    hashCode = (hashCode * 397) ^ Long.GetHashCode();
                    hashCode = (hashCode * 397) ^ ULong.GetHashCode();
                    hashCode = (hashCode * 397) ^ Float.GetHashCode();
                    hashCode = (hashCode * 397) ^ Double.GetHashCode();
                    hashCode = (hashCode * 397) ^ Decimal.GetHashCode();
                    hashCode = (hashCode * 397) ^ Guid.GetHashCode();
                    hashCode = (hashCode * 397) ^ String.GetHashCode();
                    hashCode = (hashCode * 397) ^ Key.GetHashCode();
                    return hashCode;
                }
            }
        }
    }
}
