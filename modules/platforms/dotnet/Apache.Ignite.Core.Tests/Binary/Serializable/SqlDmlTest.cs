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
// ReSharper disable UnusedParameter.Local
namespace Apache.Ignite.Core.Tests.Binary.Serializable
{
    using System;
    using System.IO;
    using System.Linq;
    using System.Runtime.Serialization;
    using System.Text;
    using System.Threading;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Linq;
    using NUnit.Framework;

    /// <summary>
    /// Tests SQL and DML with Serializable types.
    /// </summary>
    public class SqlDmlTest
    {
        /** */
        private IIgnite _ignite;

        /** */
        private StringBuilder _outSb;

        /// <summary>
        /// Sets up the test fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            _outSb = new StringBuilder();
            Console.SetError(new StringWriter(_outSb));

            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration(typeof(SimpleSerializable))
                {
                    NameMapper = new BinaryBasicNameMapper { IsSimpleName = true }
                }
            };

            _ignite = Ignition.Start(cfg);
        }

        /// <summary>
        /// Tears down the test fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests the simple serializable.
        /// </summary>
        [Test]
        public void TestSimpleSerializable()
        {
            var cache = _ignite.CreateCache<int, SimpleSerializable>(
                new CacheConfiguration("simple", new QueryEntity(typeof(int), typeof(SimpleSerializable))));

            cache[1] = new SimpleSerializable
            {
                String = "abc"
            };
            cache[2] = new SimpleSerializable
            {
                Byte = 25,
                Bool = true,
                Short = 66,
                Int = 2,
                Long = 98,
                Float = 2.25f,
                Double = 1.123,
                Decimal = 5.67m,
                Guid = Guid.NewGuid(),
                String = "bar2"
            };

            // Test SQL.
#pragma warning disable 618
            var res = cache.Query(new SqlQuery(typeof(SimpleSerializable), "where Int = 2")).GetAll().Single();
#pragma warning restore 618

            Assert.AreEqual(2, res.Key);
            Assert.AreEqual(2, res.Value.Int);
            Assert.AreEqual("bar2", res.Value.String);

            // Test DML.
            var guid = Guid.NewGuid();
            var insertRes = cache.Query(new SqlFieldsQuery(
                "insert into SimpleSerializable(_key, Byte, Bool, Short, Int, Long, Float, Double, " +
                "Decimal, Guid, String) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                3, 45, true, 43, 33, 99, 4.5f, 6.7, 9.04m, guid, "bar33")).GetAll();

            Assert.AreEqual(1, insertRes.Count);
            Assert.AreEqual(1, insertRes[0][0]);

            var dmlRes = cache[3];
            Assert.AreEqual(45, dmlRes.Byte);
            Assert.AreEqual(true, dmlRes.Bool);
            Assert.AreEqual(43, dmlRes.Short);
            Assert.AreEqual(33, dmlRes.Int);
            Assert.AreEqual(99, dmlRes.Long);
            Assert.AreEqual(4.5f, dmlRes.Float);
            Assert.AreEqual(6.7, dmlRes.Double);
            Assert.AreEqual(9.04m, dmlRes.Decimal);
            Assert.AreEqual(guid, dmlRes.Guid);
            Assert.AreEqual("bar33", dmlRes.String);
        }

        /// <summary>
        /// Tests the .NET specific serializable.
        /// </summary>
        [Test]
        public void TestDotNetSpecificSerializable()
        {
            var cache = _ignite.CreateCache<int, DotNetSpecificSerializable>(new CacheConfiguration("dotnet-ser",
                new QueryEntity(typeof(int), typeof(DotNetSpecificSerializable))));

            cache[1] = new DotNetSpecificSerializable(uint.MaxValue);
            Assert.AreEqual(uint.MaxValue, cache[1].Uint);

            // Test SQL.
            var sqlRes = cache.Query(new SqlFieldsQuery(
                "select uint from DotNetSpecificSerializable where uint <> 0")).GetAll();

            Assert.AreEqual(1, sqlRes.Count);
            Assert.AreEqual(uint.MaxValue, (uint) (int) sqlRes[0][0]);

            // Test LINQ.
            var linqRes = cache.AsCacheQueryable().Select(x => x.Value.Uint).Single();
            Assert.AreEqual(uint.MaxValue, linqRes);

            // Test DML.
            var dmlRes = cache.Query(new SqlFieldsQuery(
                "insert into DotNetSpecificSerializable(_key, uint) values (?, ?), (?, ?)",
                2, uint.MaxValue, 3, 88)).GetAll();
            Assert.AreEqual(1, dmlRes.Count);

            Assert.AreEqual(88, cache[3].Uint);  // Works when value is in int range.

            var ex = Assert.Throws<OverflowException>(() => cache.Get(2));  // Fails when out of int range.
            Assert.AreEqual("Value was either too large or too small for a UInt32.", ex.Message);
        }

#if !NETCOREAPP2_0 && !NETCOREAPP2_1 && !NETCOREAPP3_0// Console redirect issues on .NET Core
        /// <summary>
        /// Tests the log warning.
        /// </summary>
        [Test]
        public void TestLogWarning()
        {
            Thread.Sleep(10);  // Wait for logger update.

            var expected =
                string.Format("[WARN ][main][Marshaller] Type '{0}' implements '{1}'. " +
                              "It will be written in Ignite binary format, however, " +
                              "the following limitations apply: DateTime fields would not work in SQL; " +
                              "sbyte, ushort, uint, ulong fields would not work in DML.",
                    typeof(SimpleSerializable), typeof(ISerializable));

            Assert.IsTrue(_outSb.ToString().Contains(expected));
        }
#endif

        /// <summary>
        /// Serializable with Java-compatible fields.
        /// </summary>
        private class SimpleSerializable : ISerializable
        {
            [QuerySqlField]
            public byte Byte { get; set; }

            [QuerySqlField]
            public bool Bool { get; set; }

            [QuerySqlField]
            public short Short { get; set; }

            [QuerySqlField]
            public int Int { get; set; }

            [QuerySqlField]
            public long Long { get; set; }

            [QuerySqlField]
            public float Float { get; set; }

            [QuerySqlField]
            public double Double { get; set; }

            [QuerySqlField]
            public decimal Decimal { get; set; }

            [QuerySqlField]
            public Guid Guid { get; set; }

            [QuerySqlField]
            public string String { get; set; }

            public SimpleSerializable()
            {
                // No-op.
            }

            public SimpleSerializable(SerializationInfo info, StreamingContext context)
            {
                Byte = info.GetByte("Byte");
                Bool = info.GetBoolean("Bool");
                Short = info.GetInt16("Short");
                Int = info.GetInt32("Int");
                Long = info.GetInt64("Long");
                Float = info.GetSingle("Float");
                Double = info.GetDouble("Double");
                Decimal = info.GetDecimal("Decimal");
                Guid = (Guid) info.GetValue("Guid", typeof(Guid));
                String = info.GetString("String");
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("Byte", Byte);
                info.AddValue("Bool", Bool);
                info.AddValue("Short", Short);
                info.AddValue("Int", Int);
                info.AddValue("Long", Long);
                info.AddValue("Float", Float);
                info.AddValue("Double", Double);
                info.AddValue("Decimal", Decimal);
                info.AddValue("Guid", Guid);
                info.AddValue("String", String);
            }
        }

        /// <summary>
        /// Serializable with incompatible fields.
        /// </summary>
        private class DotNetSpecificSerializable : ISerializable
        {
            /// <summary>
            /// Uint is not supported in Java.
            /// </summary>
            [QuerySqlField]
            public uint Uint { get; set; }

            public DotNetSpecificSerializable(uint u)
            {
                Uint = u;
            }

            public DotNetSpecificSerializable(SerializationInfo info, StreamingContext context)
            {
                Uint = info.GetUInt32("uint");
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                info.AddValue("uint", Uint);
            }
        }
    }
}
