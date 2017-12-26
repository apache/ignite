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

namespace Apache.Ignite.Core.Tests.Binary
{
    using System;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests binary footer integrity (field offsets).
    /// Writes objects of various sizes to test schema compaction 
    /// (where field offsets can be stored as 1, 2 or 4 bytes).
    /// </summary>
    public class BinaryFooterTest
    {
        /// <summary>
        /// Tears down the test.
        /// </summary>
        [TearDown]
        public void TearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Tests full footers in offline mode.
        /// </summary>
        [Test]
        public void TestFullFooterOffline()
        {
            // Register type to avoid unregistered type name in binary object.
            // Use new marshaller to read and write to avoid schema caching.

            TestOffsets(() => new Marshaller(new BinaryConfiguration(typeof(OffsetTest))
            {
                // Compact footers do not work in offline mode.
                CompactFooter = false
            }));
        }

        /// <summary>
        /// Tests the full footer online.
        /// </summary>
        [Test]
        public void TestFullFooterOnline()
        {
            var ignite = Ignition.Start(new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                BinaryConfiguration = new BinaryConfiguration
                {
                    CompactFooter = false
                }
            });

            TestOffsets(() => ((Ignite) ignite).Marshaller);
        }

        /// <summary>
        /// Tests the full footer online.
        /// </summary>
        [Test]
        public void TestCompactFooterOnline()
        {
            var ignite = Ignition.Start(TestUtils.GetTestConfiguration());

            TestOffsets(() => ((Ignite) ignite).Marshaller);
        }

        /// <summary>
        /// Tests the offsets.
        /// </summary>
        private static void TestOffsets(Func<Marshaller> getMarsh)
        {
            // Corner cases are byte/sbyte/short/ushort max values.
            foreach (var i in new[] {1, sbyte.MaxValue, byte.MaxValue, short.MaxValue, ushort.MaxValue})
            {
                foreach (var j in new[] {-1, 0, 1})
                {
                    var arrSize = i + j;

                    var dt = new OffsetTest
                    {
                        Arr = Enumerable.Range(0, arrSize).Select(x => (byte) x).ToArray(),
                        Int = arrSize
                    };

                    var bytes = getMarsh().Marshal(dt);

                    var res = getMarsh().Unmarshal<OffsetTest>(bytes);
                    var binRes = getMarsh().Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);
                    var binFieldRes = new OffsetTest
                    {
                        Arr = binRes.GetField<byte[]>("arr"),
                        Int = binRes.GetField<int>("int")
                    };

                    foreach (var r in new[] {res, binRes.Deserialize<OffsetTest>(), binFieldRes})
                    {
                        Assert.AreEqual(dt.Arr, r.Arr);
                        Assert.AreEqual(dt.Int, r.Int);
                    }

                    TestSql(dt, getMarsh());
                }
            }
        }

        /// <summary>
        /// Tests SQL query, which verifies Java side of things.
        /// </summary>
        private static void TestSql(OffsetTest dt, Marshaller marsh)
        {
            var ignite = marsh.Ignite;

            if (ignite == null)
            {
                return;
            }

            var cache = ignite.GetIgnite().GetOrCreateCache<int, OffsetTest>(
                    new CacheConfiguration("offs", new QueryEntity(typeof(int), typeof(OffsetTest))));

            // Cache operation.
            cache[1] = dt;
            Assert.AreEqual(dt.Int, cache[1].Int);
            Assert.AreEqual(dt.Arr, cache[1].Arr);

            // SQL: read field on Java side to ensure correct offset handling.
            var res = cache.Query(new SqlFieldsQuery("select int from OffsetTest")).GetAll()[0][0];
            Assert.AreEqual(dt.Int, (int) res);
        }

        /// <summary>
        /// Offset test.
        /// </summary>
        private class OffsetTest : IBinarizable
        {
            [QuerySqlField]
            public byte[] Arr;  // Array to enforce field offset.

            [QuerySqlField]
            public int Int;     // Value at offset.

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteByteArray("arr", Arr);
                writer.WriteInt("int", Int);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                // Read in different order to enforce full schema scan.
                Int = reader.ReadInt("int");
                Arr = reader.ReadByteArray("arr");
            }
        }
    }
}
