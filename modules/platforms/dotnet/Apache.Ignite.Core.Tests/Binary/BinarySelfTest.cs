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

// ReSharper disable NonReadonlyMemberInGetHashCode
// ReSharper disable CompareOfFloatsByEqualityOperator
// ReSharper disable PossibleInvalidOperationException
// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedMember.Local
namespace Apache.Ignite.Core.Tests.Binary
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.IO;
    using System.Linq;
    using System.Reflection;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using NUnit.Framework;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;

    /// <summary>
    /// Binary tests.
    /// </summary>
    [TestFixture]
    public class BinarySelfTest { 
        /** */
        private Marshaller _marsh;

        /** */
        public static readonly string[] SpecialStrings =
        {
            new string(new[] {(char) 0xD800, '的', (char) 0xD800, (char) 0xD800, (char) 0xDC00, (char) 0xDFFF}),
            "ascii0123456789",
            "的的abcdкириллица",
            new string(new[] {(char) 0xD801, (char) 0xDC37})
        };

        /// <summary>
        /// 
        /// </summary>
        [TestFixtureSetUp]
        public void BeforeTest()
        {
            _marsh = new Marshaller(new BinaryConfiguration
            {
                CompactFooter = GetCompactFooter(),
                NameMapper = GetNameMapper()
            });
        }

        /// <summary>
        /// Gets the binary configuration.
        /// </summary>
        protected virtual bool GetCompactFooter()
        {
            return true;
        }

        /// <summary>
        /// Gets the name mapper.
        /// </summary>
        protected virtual IBinaryNameMapper GetNameMapper()
        {
            return BinaryBasicNameMapper.FullNameInstance;
        }
        
        /**
         * <summary>Check write of primitive boolean.</summary>
         */
        [Test]
        public void TestWritePrimitiveBool()
        {
            Assert.AreEqual(_marsh.Unmarshal<bool>(_marsh.Marshal(false)), false);
            Assert.AreEqual(_marsh.Unmarshal<bool>(_marsh.Marshal(true)), true);

            Assert.AreEqual(_marsh.Unmarshal<bool?>(_marsh.Marshal((bool?)false)), false);
            Assert.AreEqual(_marsh.Unmarshal<bool?>(_marsh.Marshal((bool?)null)), null);
        }

        /**
         * <summary>Check write of primitive boolean array.</summary>
         */
        [Test]
        public void TestWritePrimitiveBoolArray()
        {
            bool[] vals = { true, false };

            Assert.AreEqual(_marsh.Unmarshal<bool[]>(_marsh.Marshal(vals)), vals);

            bool?[] vals2 = { true, false };

            Assert.AreEqual(_marsh.Unmarshal<bool?[]>(_marsh.Marshal(vals2)), vals2);
        }

        /**
         * <summary>Check write of primitive sbyte.</summary>
         */
        [Test]
        public void TestWritePrimitiveSbyte()
        {
            Assert.AreEqual(_marsh.Unmarshal<sbyte>(_marsh.Marshal((sbyte)1)), 1);
            Assert.AreEqual(_marsh.Unmarshal<sbyte>(_marsh.Marshal(sbyte.MinValue)), sbyte.MinValue);
            Assert.AreEqual(_marsh.Unmarshal<sbyte>(_marsh.Marshal(sbyte.MaxValue)), sbyte.MaxValue);

            Assert.AreEqual(_marsh.Unmarshal<sbyte?>(_marsh.Marshal((sbyte?)1)), (sbyte?)1);
            Assert.AreEqual(_marsh.Unmarshal<sbyte?>(_marsh.Marshal((sbyte?)null)), null);
        }

        /**
         * <summary>Check write of primitive sbyte array.</summary>
         */
        [Test]
        public void TestWritePrimitiveSbyteArray()
        {
            sbyte[] vals = { sbyte.MinValue, 0, 1, sbyte.MaxValue };
            sbyte[] newVals = _marsh.Unmarshal<sbyte[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive byte.</summary>
         */
        [Test]
        public void TestWritePrimitiveByte()
        {
            Assert.AreEqual(_marsh.Unmarshal<byte>(_marsh.Marshal((byte)1)), 1);
            Assert.AreEqual(_marsh.Unmarshal<byte>(_marsh.Marshal(byte.MinValue)), byte.MinValue);
            Assert.AreEqual(_marsh.Unmarshal<byte>(_marsh.Marshal(byte.MaxValue)), byte.MaxValue);

            Assert.AreEqual(_marsh.Unmarshal<byte?>(_marsh.Marshal((byte?)1)), (byte?)1);
            Assert.AreEqual(_marsh.Unmarshal<byte?>(_marsh.Marshal((byte?)null)), null);
        }

        /**
         * <summary>Check write of primitive byte array.</summary>
         */
        [Test]
        public void TestWritePrimitiveByteArray()
        {
            byte[] vals = { byte.MinValue, 0, 1, byte.MaxValue };
            byte[] newVals = _marsh.Unmarshal<byte[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive short.</summary>
         */
        [Test]
        public void TestWritePrimitiveShort()
        {
            Assert.AreEqual(_marsh.Unmarshal<short>(_marsh.Marshal((short)1)), 1);
            Assert.AreEqual(_marsh.Unmarshal<short>(_marsh.Marshal(short.MinValue)), short.MinValue);
            Assert.AreEqual(_marsh.Unmarshal<short>(_marsh.Marshal(short.MaxValue)), short.MaxValue);

            Assert.AreEqual(_marsh.Unmarshal<short?>(_marsh.Marshal((short?)1)), (short?)1);
            Assert.AreEqual(_marsh.Unmarshal<short?>(_marsh.Marshal((short?)null)), null);
        }

        /**
         * <summary>Check write of primitive short array.</summary>
         */
        [Test]
        public void TestWritePrimitiveShortArray()
        {
            short[] vals = { short.MinValue, 0, 1, short.MaxValue };
            short[] newVals = _marsh.Unmarshal<short[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive ushort.</summary>
         */
        [Test]
        public void TestWritePrimitiveUshort()
        {
            Assert.AreEqual(_marsh.Unmarshal<ushort>(_marsh.Marshal((ushort)1)), 1);
            Assert.AreEqual(_marsh.Unmarshal<ushort>(_marsh.Marshal(ushort.MinValue)), ushort.MinValue);
            Assert.AreEqual(_marsh.Unmarshal<ushort>(_marsh.Marshal(ushort.MaxValue)), ushort.MaxValue);

            Assert.AreEqual(_marsh.Unmarshal<ushort?>(_marsh.Marshal((ushort?)1)), (ushort?)1);
            Assert.AreEqual(_marsh.Unmarshal<ushort?>(_marsh.Marshal((ushort?)null)), null);
        }

        /**
         * <summary>Check write of primitive short array.</summary>
         */
        [Test]
        public void TestWritePrimitiveUshortArray()
        {
            ushort[] vals = { ushort.MinValue, 0, 1, ushort.MaxValue };
            ushort[] newVals = _marsh.Unmarshal<ushort[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive char.</summary>
         */
        [Test]
        public void TestWritePrimitiveChar()
        {
            Assert.AreEqual(_marsh.Unmarshal<char>(_marsh.Marshal((char)1)), (char)1);
            Assert.AreEqual(_marsh.Unmarshal<char>(_marsh.Marshal(char.MinValue)), char.MinValue);
            Assert.AreEqual(_marsh.Unmarshal<char>(_marsh.Marshal(char.MaxValue)), char.MaxValue);

            Assert.AreEqual(_marsh.Unmarshal<char?>(_marsh.Marshal((char?)1)), (char?)1);
            Assert.AreEqual(_marsh.Unmarshal<char?>(_marsh.Marshal((char?)null)), null);
        }

        /**
         * <summary>Check write of primitive uint array.</summary>
         */
        [Test]
        public void TestWritePrimitiveCharArray()
        {
            char[] vals = { char.MinValue, (char)0, (char)1, char.MaxValue };
            char[] newVals = _marsh.Unmarshal<char[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive int.</summary>
         */
        [Test]
        public void TestWritePrimitiveInt()
        {
            Assert.AreEqual(_marsh.Unmarshal<int>(_marsh.Marshal(1)), 1);
            Assert.AreEqual(_marsh.Unmarshal<int>(_marsh.Marshal(int.MinValue)), int.MinValue);
            Assert.AreEqual(_marsh.Unmarshal<int>(_marsh.Marshal(int.MaxValue)), int.MaxValue);

            Assert.AreEqual(_marsh.Unmarshal<int?>(_marsh.Marshal((int?)1)), (int?)1);
            Assert.AreEqual(_marsh.Unmarshal<int?>(_marsh.Marshal((int?)null)), null);
        }

        /**
         * <summary>Check write of primitive uint array.</summary>
         */
        [Test]
        public void TestWritePrimitiveIntArray()
        {
            int[] vals = { int.MinValue, 0, 1, int.MaxValue };
            int[] newVals = _marsh.Unmarshal<int[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive uint.</summary>
         */
        [Test]
        public void TestWritePrimitiveUint()
        {
            Assert.AreEqual(_marsh.Unmarshal<uint>(_marsh.Marshal((uint)1)), 1);
            Assert.AreEqual(_marsh.Unmarshal<uint>(_marsh.Marshal(uint.MinValue)), uint.MinValue);
            Assert.AreEqual(_marsh.Unmarshal<uint>(_marsh.Marshal(uint.MaxValue)), uint.MaxValue);

            Assert.AreEqual(_marsh.Unmarshal<uint?>(_marsh.Marshal((uint?)1)), (int?)1);
            Assert.AreEqual(_marsh.Unmarshal<uint?>(_marsh.Marshal((uint?)null)), null);
        }

        /**
         * <summary>Check write of primitive uint array.</summary>
         */
        [Test]
        public void TestWritePrimitiveUintArray()
        {
            uint[] vals = { uint.MinValue, 0, 1, uint.MaxValue };
            uint[] newVals = _marsh.Unmarshal<uint[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive long.</summary>
         */
        [Test]
        public void TestWritePrimitiveLong()
        {
            Assert.AreEqual(_marsh.Unmarshal<long>(_marsh.Marshal((long)1)), 1);
            Assert.AreEqual(_marsh.Unmarshal<long>(_marsh.Marshal(long.MinValue)), long.MinValue);
            Assert.AreEqual(_marsh.Unmarshal<long>(_marsh.Marshal(long.MaxValue)), long.MaxValue);

            Assert.AreEqual(_marsh.Unmarshal<long?>(_marsh.Marshal((long?)1)), (long?)1);
            Assert.AreEqual(_marsh.Unmarshal<long?>(_marsh.Marshal((long?)null)), null);
        }

        /**
         * <summary>Check write of primitive long array.</summary>
         */
        [Test]
        public void TestWritePrimitiveLongArray()
        {
            long[] vals = { long.MinValue, 0, 1, long.MaxValue };
            long[] newVals = _marsh.Unmarshal<long[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive ulong.</summary>
         */
        [Test]
        public void TestWritePrimitiveUlong()
        {
            Assert.AreEqual(_marsh.Unmarshal<ulong>(_marsh.Marshal((ulong)1)), 1);
            Assert.AreEqual(_marsh.Unmarshal<ulong>(_marsh.Marshal(ulong.MinValue)), ulong.MinValue);
            Assert.AreEqual(_marsh.Unmarshal<ulong>(_marsh.Marshal(ulong.MaxValue)), ulong.MaxValue);

            Assert.AreEqual(_marsh.Unmarshal<ulong?>(_marsh.Marshal((ulong?)1)), (ulong?)1);
            Assert.AreEqual(_marsh.Unmarshal<ulong?>(_marsh.Marshal((ulong?)null)), null);
        }

        /**
         * <summary>Check write of primitive ulong array.</summary>
         */
        [Test]
        public void TestWritePrimitiveUlongArray()
        {
            ulong[] vals = { ulong.MinValue, 0, 1, ulong.MaxValue };
            ulong[] newVals = _marsh.Unmarshal<ulong[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive float.</summary>
         */
        [Test]
        public void TestWritePrimitiveFloat()
        {
            Assert.AreEqual(_marsh.Unmarshal<float>(_marsh.Marshal((float)1)), (float)1);
            Assert.AreEqual(_marsh.Unmarshal<float>(_marsh.Marshal(float.MinValue)), float.MinValue);
            Assert.AreEqual(_marsh.Unmarshal<float>(_marsh.Marshal(float.MaxValue)), float.MaxValue);

            Assert.AreEqual(_marsh.Unmarshal<float?>(_marsh.Marshal((float?)1)), (float?)1);
            Assert.AreEqual(_marsh.Unmarshal<float?>(_marsh.Marshal((float?)null)), null);
        }

        /**
         * <summary>Check write of primitive float array.</summary>
         */
        [Test]
        public void TestWritePrimitiveFloatArray()
        {
            float[] vals = { float.MinValue, 0, 1, float.MaxValue };
            float[] newVals = _marsh.Unmarshal<float[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive double.</summary>
         */
        [Test]
        public void TestWritePrimitiveDouble()
        {
            Assert.AreEqual(_marsh.Unmarshal<double>(_marsh.Marshal((double)1)), (double)1);
            Assert.AreEqual(_marsh.Unmarshal<double>(_marsh.Marshal(double.MinValue)), double.MinValue);
            Assert.AreEqual(_marsh.Unmarshal<double>(_marsh.Marshal(double.MaxValue)), double.MaxValue);

            Assert.AreEqual(_marsh.Unmarshal<double?>(_marsh.Marshal((double?)1)), (double?)1);
            Assert.AreEqual(_marsh.Unmarshal<double?>(_marsh.Marshal((double?)null)), null);
        }

        /**
         * <summary>Check write of primitive double array.</summary>
         */
        [Test]
        public void TestWritePrimitiveDoubleArray()
        {
            double[] vals = { double.MinValue, 0, 1, double.MaxValue };
            double[] newVals = _marsh.Unmarshal<double[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of decimal.</summary>
         */
        [Test]
        public void TestWritePrimitiveDecimal()
        {
            decimal val;

            // Test positibe and negative.
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Zero)), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = new decimal(1, 0, 0, false, 0))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = new decimal(1, 0, 0, true, 0))), val);

            // Test 32, 64 and 96 bits + mixed.
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = new decimal(0, 1, 0, false, 0))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = new decimal(0, 1, 0, true, 0))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = new decimal(0, 0, 1, false, 0))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = new decimal(0, 0, 1, true, 0))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = new decimal(1, 1, 1, false, 0))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = new decimal(1, 1, 1, true, 0))), val);

            // Test extremes.
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("65536"))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("-65536"))), val);

            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("4294967296"))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("-4294967296"))), val);

            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("281474976710656"))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("-281474976710656"))), val);

            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("18446744073709551616"))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("-18446744073709551616"))), val);

            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("1208925819614629174706176"))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("-1208925819614629174706176"))), val);

            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.MaxValue)), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.MinValue)), val);

            // Test scale.
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("11,12"))), val);
            Assert.AreEqual(_marsh.Unmarshal<decimal>(_marsh.Marshal(val = decimal.Parse("-11,12"))), val);

            // Test null.
            Assert.AreEqual(_marsh.Unmarshal<decimal?>(_marsh.Marshal((decimal?)null)), null);
        }

        /**
         * <summary>Check write of decimal array.</summary>
         */
        [Test]
        public void TestWritePrimitiveDecimalArray()
        {
            decimal?[] vals = { decimal.One, decimal.Parse("11,12") };
            var newVals = _marsh.Unmarshal<decimal?[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of string.</summary>
         */
        [Test]
        public void TestWriteString()
        {
            Assert.AreEqual(_marsh.Unmarshal<string>(_marsh.Marshal("str")), "str");
            Assert.AreEqual(_marsh.Unmarshal<string>(_marsh.Marshal((string) null)), null);
        }

        /// <summary>
        /// Tests special characters.
        /// </summary>
        [Test]
        public void TestWriteSpecialString()
        {
            if (BinaryUtils.UseStringSerializationVer2)
            {
                foreach (var test in SpecialStrings)
                {
                    Assert.AreEqual(_marsh.Unmarshal<string>(_marsh.Marshal(test)), test);
                }
            }
        }

        /**
         * <summary>Check write of string array.</summary>
         */
        [Test]
        public void TestWriteStringArray()
        {
            string[] vals = { "str1", null, "", "str2", null};
            string[] newVals = _marsh.Unmarshal<string[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of Guid.</summary>
         */
        [Test]
        public void TestWriteGuid()
        {
            Guid guid = Guid.NewGuid();
            Guid? nGuid = guid;

            Assert.AreEqual(_marsh.Unmarshal<Guid>(_marsh.Marshal(guid)), guid);
            Assert.AreEqual(_marsh.Unmarshal<Guid?>(_marsh.Marshal(nGuid)), nGuid);

            nGuid = null;

            // ReSharper disable once ExpressionIsAlwaysNull
            Assert.AreEqual(_marsh.Unmarshal<Guid?>(_marsh.Marshal(nGuid)), null);
        }

        /**
         * <summary>Check write of string array.</summary>
         */
        [Test]
        public void TestWriteGuidArray()
        {
            Guid?[] vals = { Guid.NewGuid(), null, Guid.Empty, Guid.NewGuid(), null };
            Guid?[] newVals = _marsh.Unmarshal<Guid?[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /// <summary>
        /// Checks that both methods produce identical results.
        /// </summary>
        [Test]
        public void TestGuidSlowFast()
        {
            var stream = new BinaryHeapStream(128);

            var guid = Guid.NewGuid();

            BinaryUtils.WriteGuidFast(guid, stream);

            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(guid, BinaryUtils.ReadGuidFast(stream));

            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(guid, BinaryUtils.ReadGuidSlow(stream));


            stream.Seek(0, SeekOrigin.Begin);
            BinaryUtils.WriteGuidFast(guid, stream);

            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(guid, BinaryUtils.ReadGuidFast(stream));

            stream.Seek(0, SeekOrigin.Begin);
            Assert.AreEqual(guid, BinaryUtils.ReadGuidSlow(stream));
        }

        /**
        * <summary>Check write of enum.</summary>
        */
        [Test]
        [SuppressMessage("ReSharper", "ExpressionIsAlwaysNull")]
        public void TestWriteEnum()
        {
            TestEnum val = TestEnum.Val1;

            Assert.AreEqual(_marsh.Unmarshal<TestEnum>(_marsh.Marshal(val)), val);

            TestEnum? val2 = TestEnum.Val1;
            Assert.AreEqual(_marsh.Unmarshal<TestEnum?>(_marsh.Marshal(val2)), val2);

            val2 = null;
            Assert.AreEqual(_marsh.Unmarshal<TestEnum?>(_marsh.Marshal(val2)), val2);
        }

        /// <summary>
        /// Tests the write of registered enum.
        /// </summary>
        [Test]
        public void TestWriteEnumRegistered()
        {
            var marsh =
                new Marshaller(new BinaryConfiguration
                {
                    TypeConfigurations = new[] { new BinaryTypeConfiguration(typeof(TestEnum)) }
                });

            TestEnum val = TestEnum.Val1;

            var data = marsh.Marshal(val);

            Assert.AreEqual(marsh.Unmarshal<TestEnum>(data), val);

            var binEnum = marsh.Unmarshal<IBinaryObject>(data, BinaryMode.ForceBinary);

            Assert.AreEqual(val, (TestEnum) binEnum.EnumValue);
        }

        /**
        * <summary>Check write of enum.</summary>
        */
        [Test]
        public void TestWriteEnumArray()
        {
            TestEnum[] vals = { TestEnum.Val2, TestEnum.Val3 };
            TestEnum[] newVals = _marsh.Unmarshal<TestEnum[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /// <summary>
        /// Tests the write of registered enum array.
        /// </summary>
        [Test]
        public void TestWriteEnumArrayRegistered()
        {
            var marsh =
                new Marshaller(new BinaryConfiguration
                {
                    TypeConfigurations = new[] { new BinaryTypeConfiguration(typeof(TestEnum)) }
                });

            TestEnum[] vals = { TestEnum.Val2, TestEnum.Val3 };
            TestEnum[] newVals = marsh.Unmarshal<TestEnum[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }
        
        /// <summary>
        /// Test object with dates.
        /// </summary>
        [Test]
        public void TestDateObject()
        {
            ICollection<BinaryTypeConfiguration> typeCfgs =
                new List<BinaryTypeConfiguration>();

            typeCfgs.Add(new BinaryTypeConfiguration(typeof(DateTimeType)));

            BinaryConfiguration cfg = new BinaryConfiguration {TypeConfigurations = typeCfgs};

            Marshaller marsh = new Marshaller(cfg);

            DateTime now = DateTime.Now;

            DateTimeType obj = new DateTimeType(now);

            DateTimeType otherObj = marsh.Unmarshal<DateTimeType>(marsh.Marshal(obj));

            Assert.AreEqual(obj.Utc, otherObj.Utc);
            Assert.AreEqual(obj.UtcNull, otherObj.UtcNull);            
            Assert.AreEqual(obj.UtcArr, otherObj.UtcArr);

            Assert.AreEqual(obj.UtcRaw, otherObj.UtcRaw);
            Assert.AreEqual(obj.UtcNullRaw, otherObj.UtcNullRaw);
            Assert.AreEqual(obj.UtcArrRaw, otherObj.UtcArrRaw);
        }

        /// <summary>
        /// Tests the DateTime marshalling.
        /// </summary>
        [Test]
        public void TestDateTime()
        {
            var time = DateTime.Now;
            Assert.AreEqual(_marsh.Unmarshal<DateTime>(_marsh.Marshal(time)), time);

            var timeUtc = DateTime.UtcNow;
            Assert.AreEqual(_marsh.Unmarshal<DateTime>(_marsh.Marshal(timeUtc)), timeUtc);

            // Check exception with non-UTC date
            var stream = new BinaryHeapStream(128);
            var writer = _marsh.StartMarshal(stream);
            Assert.Throws<BinaryObjectException>(() => writer.WriteTimestamp(DateTime.Now));
        }

        /**
         * <summary>Check generic collections.</summary>
         */
        [Test]
        public void TestGenericCollections()
        {
            var list = new List<string> {"1"};

            var data = _marsh.Marshal(list);

            var newObjList = _marsh.Unmarshal<IList<string>>(data);

            CollectionAssert.AreEquivalent(list, newObjList);
        }

        /// <summary>
        /// Tests marshal aware type with generic collections.
        /// </summary>
        [Test]
        public void TestGenericCollectionsType()
        {
            var marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations = new List<BinaryTypeConfiguration>
                {
                    new BinaryTypeConfiguration(typeof (PrimitiveFieldType)),
                    new BinaryTypeConfiguration(typeof (GenericCollectionsType<PrimitiveFieldType, SerializableObject>))
                },
                CompactFooter = GetCompactFooter()
            });

            var obj = new GenericCollectionsType<PrimitiveFieldType, SerializableObject>
            {
                Keys = new[] {new PrimitiveFieldType(), new PrimitiveFieldType()},
                Values =
                    new List<SerializableObject>
                    {
                        new SerializableObject {Foo = 1},
                        new SerializableObject {Foo = 5}
                    },
                Pairs = new Dictionary<PrimitiveFieldType, SerializableObject>
                {
                    {new PrimitiveFieldType(), new SerializableObject {Foo = 10}},
                    {new PrimitiveFieldType {PByte = 10}, new SerializableObject {Foo = 20}}
                },
                Objects = new object[] {1, 2, "3", 4.4}
            };
            
            var data = marsh.Marshal(obj);

            var result = marsh.Unmarshal<GenericCollectionsType<PrimitiveFieldType, SerializableObject>>(data);

            CollectionAssert.AreEquivalent(obj.Keys, result.Keys);
            CollectionAssert.AreEquivalent(obj.Values, result.Values);
            CollectionAssert.AreEquivalent(obj.Pairs, result.Pairs);
            CollectionAssert.AreEquivalent(obj.Objects, result.Objects);
        }

        /// <summary>
        /// Tests the circular reference handling with List.
        /// </summary>
        [Test]
        public void TestListCircularReference()
        {
            var list1 = new List<object> {1};
            var list2 = new List<object> {2};

            list1.Add(list2);
            list2.Add(list1);

            var data = _marsh.Marshal(list1);

            var resList1 = _marsh.Unmarshal<List<object>>(data);
            Assert.AreEqual(1, resList1[0]);

            var resList2 = (List<object>) resList1[1];
            Assert.AreEqual(2, resList2[0]);
            Assert.AreEqual(resList1, resList2[1]);
        }

        /// <summary>
        /// Tests the circular reference handling with Dictionary.
        /// This test checks proper handle support in combination with OnDeserialization callback,
        /// which has to be called after entire graph is deserialized.
        /// </summary>
        [Test]
        public void TestDictionaryCircularReference()
        {
            var dict1 = new Dictionary<object, object> {{0, 1}};
            var dict2 = new Dictionary<object, object> {{0, 2}};

            dict1[1] = dict2;
            dict2[1] = dict1;

            var data = _marsh.Marshal(dict1);

            var resDict1 = _marsh.Unmarshal<Dictionary<object, object>>(data);
            Assert.AreEqual(1, resDict1[0]);

            var resDict2 = (Dictionary<object, object>) resDict1[1];
            Assert.AreEqual(2, resDict2[0]);
            Assert.AreEqual(resDict1, resDict2[1]);
        }

        /**
         * <summary>Check property read.</summary>
         */
        [Test]
        public void TestProperty()
        {
            ICollection<BinaryTypeConfiguration> typeCfgs = 
                new List<BinaryTypeConfiguration>();

            typeCfgs.Add(new BinaryTypeConfiguration(typeof(PropertyType)));

            BinaryConfiguration cfg = new BinaryConfiguration {TypeConfigurations = typeCfgs};

            Marshaller marsh = new Marshaller(cfg);

            PropertyType obj = new PropertyType
            {
                Field1 = 1,
                Field2 = 2
            };

            byte[] data = marsh.Marshal(obj);

            PropertyType newObj = marsh.Unmarshal<PropertyType>(data);

            Assert.AreEqual(obj.Field1, newObj.Field1);
            Assert.AreEqual(obj.Field2, newObj.Field2);

            IBinaryObject portNewObj = marsh.Unmarshal<IBinaryObject>(data, BinaryMode.ForceBinary);

            Assert.IsTrue(portNewObj.HasField("field1"));
            Assert.IsTrue(portNewObj.HasField("field2"));
            Assert.IsFalse(portNewObj.HasField("field3"));

            Assert.AreEqual(obj.Field1, portNewObj.GetField<int>("field1"));
            Assert.AreEqual(obj.Field2, portNewObj.GetField<int>("Field2"));
            Assert.AreEqual(0, portNewObj.GetField<int>("field3"));
        }

        /**
         * <summary>Check write of primitive fields through reflection.</summary>
         */
        [Test]
        public void TestPrimitiveFieldsReflective([Values(false, true)] bool raw)
        {
            var serializer = new BinaryReflectiveSerializer {RawMode = raw};

            Assert.AreEqual(raw, serializer.RawMode);

            var marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations = new[]
                {
                    new BinaryTypeConfiguration(typeof (PrimitiveFieldType))
                    {
                        Serializer = serializer
                    }
                },
                CompactFooter = GetCompactFooter()
            });

            // Use utc date fields because reflective serializer writes [QuerySqlField] fields as timestamp
            var obj = new PrimitiveFieldType
            {
                PDate = DateTime.UtcNow,
                PnDate = DateTime.UtcNow
            };

            CheckPrimitiveFields(marsh, obj);
        }

        /// <summary>
        /// Check write of primitive fields through binary interface.
        /// </summary>
        [Test]
        public void TestPrimitiveFieldsBinary()
        {
            var cfg = new BinaryConfiguration
            {
                TypeConfigurations = new[] { new BinaryTypeConfiguration(typeof(PrimitiveFieldBinaryType)) },
                CompactFooter = GetCompactFooter()
            };

            Marshaller marsh = new Marshaller(cfg);

            PrimitiveFieldBinaryType obj = new PrimitiveFieldBinaryType();

            CheckPrimitiveFields(marsh, obj);
        }

        /// <summary>
        /// Check write of primitive fields through binary interface.
        /// </summary>
        [Test]
        public void TestPrimitiveFieldsRawBinary()
        {
            var marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations = new[] { new BinaryTypeConfiguration(typeof(PrimitiveFieldRawBinaryType)) },
                CompactFooter = GetCompactFooter()
            });

            var obj = new PrimitiveFieldRawBinaryType();

            CheckPrimitiveFields(marsh, obj);
        }

        /// <summary>
        /// Check write of primitive fields through binary interface.
        /// </summary>
        [Test]
        public void TestPrimitiveFieldsSerializer()
        {
            var cfg = new BinaryConfiguration
            {
                TypeConfigurations = new[]
                {
                    new BinaryTypeConfiguration(typeof(PrimitiveFieldType))
                    {
                        Serializer = new PrimitiveFieldsSerializer(),
                    }
                },
                CompactFooter = GetCompactFooter()
            };

            Marshaller marsh = new Marshaller(cfg);

            PrimitiveFieldType obj = new PrimitiveFieldType();

            CheckPrimitiveFields(marsh, obj);
        }

        /**
         * <summary>Check decimals.</summary>
         */
        [Test]
        public void TestDecimalFields()
        {
            BinaryConfiguration cfg = new BinaryConfiguration
            {
                TypeConfigurations = new List<BinaryTypeConfiguration>
                {
                    new BinaryTypeConfiguration(typeof (DecimalReflective)),
                    new BinaryTypeConfiguration(typeof (DecimalMarshalAware))
                }
            };

            Marshaller marsh = new Marshaller(cfg);

            // 1. Test reflective stuff.
            DecimalReflective obj1 = new DecimalReflective
            {
                Val = decimal.Zero,
                ValArr = new decimal?[] {decimal.One, decimal.MinusOne}
            };

            IBinaryObject portObj = marsh.Unmarshal<IBinaryObject>(marsh.Marshal(obj1), BinaryMode.ForceBinary);

            Assert.AreEqual(obj1.Val, portObj.GetField<decimal>("val"));
            Assert.AreEqual(obj1.ValArr, portObj.GetField<decimal?[]>("valArr"));

            Assert.AreEqual(obj1.Val, portObj.Deserialize<DecimalReflective>().Val);
            Assert.AreEqual(obj1.ValArr, portObj.Deserialize<DecimalReflective>().ValArr);

            // 2. Test marshal aware stuff.
            DecimalMarshalAware obj2 = new DecimalMarshalAware();

            obj2.Val = decimal.Zero;
            obj2.ValArr = new decimal?[] { decimal.One, decimal.MinusOne };
            obj2.RawVal = decimal.MaxValue;
            obj2.RawValArr = new decimal?[] { decimal.MinusOne, decimal.One} ;

            portObj = marsh.Unmarshal<IBinaryObject>(marsh.Marshal(obj2), BinaryMode.ForceBinary);

            Assert.AreEqual(obj2.Val, portObj.GetField<decimal>("val"));
            Assert.AreEqual(obj2.ValArr, portObj.GetField<decimal?[]>("valArr"));

            Assert.AreEqual(obj2.Val, portObj.Deserialize<DecimalMarshalAware>().Val);
            Assert.AreEqual(obj2.ValArr, portObj.Deserialize<DecimalMarshalAware>().ValArr);
            Assert.AreEqual(obj2.RawVal, portObj.Deserialize<DecimalMarshalAware>().RawVal);
            Assert.AreEqual(obj2.RawValArr, portObj.Deserialize<DecimalMarshalAware>().RawValArr);
        }

        /// <summary>
        /// Check write of primitive fields through raw serializer.
        /// </summary>
        [Test]
        public void TestPrimitiveFieldsRawSerializer()
        {
            Marshaller marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations = new[]
                {
                    new BinaryTypeConfiguration(typeof(PrimitiveFieldType))
                    {
                        Serializer = new PrimitiveFieldsRawSerializer()
                    }
                },
                CompactFooter = GetCompactFooter()
            });

            CheckPrimitiveFields(marsh, new PrimitiveFieldType());
        }

        private void CheckPrimitiveFields(Marshaller marsh, PrimitiveFieldType obj)
        {
            CheckPrimitiveFieldsSerialization(marsh, obj);
        }

        private static void CheckPrimitiveFieldsSerialization(Marshaller marsh, PrimitiveFieldType obj)
        {
            byte[] bytes = marsh.Marshal(obj);

            IBinaryObject portObj = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            PrimitiveFieldType newObj = portObj.Deserialize<PrimitiveFieldType>();

            Assert.AreEqual(obj, newObj);
        }

        /**
         * <summary>Check write of object with enums.</summary>
         */
        [Test]
        public void TestEnumsReflective([Values(false, true)] bool raw)
        {
            Marshaller marsh =
                new Marshaller(new BinaryConfiguration
                {
                    TypeConfigurations = new[]
                    {
                        new BinaryTypeConfiguration(typeof (EnumType))
                        {
                            Serializer = new BinaryReflectiveSerializer {RawMode = raw}
                        },
                        new BinaryTypeConfiguration(typeof (TestEnum))
                        {
                            Serializer = new BinaryReflectiveSerializer {RawMode = raw}
                        }
                    }
                });

            EnumType obj = new EnumType
            {
                PEnum = TestEnum.Val1,
                PEnumArray = new[] {TestEnum.Val2, TestEnum.Val3}
            };

            byte[] bytes = marsh.Marshal(obj);

            IBinaryObject portObj = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            if (!raw)
            {
                // Test enum field in binary form
                var binEnum = portObj.GetField<IBinaryObject>("PEnum");
                Assert.AreEqual(obj.PEnum.GetHashCode(), binEnum.GetHashCode());
                Assert.AreEqual((int) obj.PEnum, binEnum.EnumValue);
                Assert.AreEqual(obj.PEnum, binEnum.Deserialize<TestEnum>());
                Assert.AreEqual(obj.PEnum, binEnum.Deserialize<object>());
                Assert.AreEqual(typeof (TestEnum), binEnum.Deserialize<object>().GetType());
                Assert.AreEqual(null, binEnum.GetField<object>("someField"));
                Assert.IsFalse(binEnum.HasField("anyField"));

                var binEnumArr = portObj.GetField<IBinaryObject[]>("PEnumArray");
                Assert.IsTrue(binEnumArr.Select(x => x.Deserialize<TestEnum>()).SequenceEqual(obj.PEnumArray));
            }
            else
            {
                Assert.IsFalse(portObj.HasField("PEnum"));
                Assert.IsFalse(portObj.HasField("PEnumArray"));
            }

            EnumType newObj = portObj.Deserialize<EnumType>();

            Assert.AreEqual(obj.PEnum, newObj.PEnum);
            Assert.AreEqual(obj.PEnumArray, newObj.PEnumArray);
        }

        /**
         * <summary>Check write of object with collections.</summary>
         */
        [Test]
        public void TestCollectionsReflective([Values(false, true)] bool raw)
        {
            var marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations = new[]
                {
                    new BinaryTypeConfiguration(typeof (CollectionsType))
                    {
                        Serializer = new BinaryReflectiveSerializer {RawMode = raw}
                    },
                    new BinaryTypeConfiguration(typeof (InnerObjectType))
                    {
                        Serializer = new BinaryReflectiveSerializer {RawMode = raw}
                    }
                },
                CompactFooter = GetCompactFooter()
            });
            
            var obj = new CollectionsType
            {
                Hashtable = new Hashtable {{1, 2}, {3, 4}},
                LinkedList = new LinkedList<int>(new[] {1, 2, 3}),
                SortedDict = new SortedDictionary<string, int> {{"1", 2}},
                Dict = new Dictionary<int, string> {{1, "2"}},
                Arr = new[] {new InnerObjectType()}
            };

            var list = new ArrayList
            {
                true,
                (byte) 1,
                (short) 2,
                'a',
                3,
                (long) 4,
                (float) 5,
                (double) 6,
                "string",
                Guid.NewGuid(),
                new InnerObjectType
                {
                    PInt1 = 1,
                    PInt2 = 2
                }
            };

            obj.Col1 = list;

            byte[] bytes = marsh.Marshal(obj);

            IBinaryObject portObj = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            CollectionsType newObj = portObj.Deserialize<CollectionsType>();

            Assert.AreEqual(obj, newObj);

            obj.Col1 = null;

            Assert.AreEqual(obj, marsh.Unmarshal<CollectionsType>(marsh.Marshal(obj)));

            obj.Col1 = list;
            obj.Col2 = list;

            Assert.AreEqual(obj, marsh.Unmarshal<CollectionsType>(marsh.Marshal(obj)));

            obj.Col2 = new TestList();
            obj.Hashtable = new TestHashTable();

            Assert.AreEqual(obj, marsh.Unmarshal<CollectionsType>(marsh.Marshal(obj)));

            // Test custom collections.
            obj.Col3 = new TestList {1, "2"};
            obj.Hashtable2 = new TestHashTable {{1, "2"}};

            Assert.AreEqual(obj, marsh.Unmarshal<CollectionsType>(marsh.Marshal(obj)));
        }

        /**
         * <summary>Check write of object fields through reflective serializer.</summary>
         */
        [Test]
        public void TestObjectReflective([Values(false, true)] bool raw)
        {
            var marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations = new[]
                {
                    new BinaryTypeConfiguration(typeof (OuterObjectType))
                    {
                        Serializer = new BinaryReflectiveSerializer {RawMode = raw}
                    },
                    new BinaryTypeConfiguration(typeof (InnerObjectType))
                    {
                        Serializer = new BinaryReflectiveSerializer {RawMode = raw}
                    }
                }
            });

            CheckObject(marsh, new OuterObjectType(), new InnerObjectType());
        }

        [Test]
        public void TestStructsReflective([Values(false, true)] bool raw)
        {
            var marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations =
                    new[]
                    {
                        new BinaryTypeConfiguration(typeof (ReflectiveStruct))
                        {
                            Serializer = new BinaryReflectiveSerializer {RawMode = raw}
                        }
                    }
            });

            var obj = new ReflectiveStruct(15, 28.8);
            var res = marsh.Unmarshal<ReflectiveStruct>(marsh.Marshal(obj));

            Assert.AreEqual(res, obj);
        }

        /**
         * <summary>Test handles.</summary>
         */
        [Test]
        public void TestHandles()
        {
            ICollection<BinaryTypeConfiguration> typeCfgs =
                new List<BinaryTypeConfiguration>();

            typeCfgs.Add(new BinaryTypeConfiguration(typeof(HandleInner)));
            typeCfgs.Add(new BinaryTypeConfiguration(typeof(HandleOuter)));

            BinaryConfiguration cfg = new BinaryConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            Marshaller marsh = new Marshaller(cfg);

            HandleOuter outer = new HandleOuter();

            outer.Before = "outBefore";
            outer.After = "outAfter";
            outer.RawBefore = "outRawBefore";
            outer.RawAfter = "outRawAfter";

            HandleInner inner = new HandleInner();

            inner.Before = "inBefore";
            inner.After = "inAfter";
            inner.RawBefore = "inRawBefore";
            inner.RawAfter = "inRawAfter";

            outer.Inner = inner;
            outer.RawInner = inner;

            inner.Outer = outer;
            inner.RawOuter = outer;

            byte[] bytes = marsh.Marshal(outer);

            IBinaryObject outerObj = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            HandleOuter newOuter = outerObj.Deserialize<HandleOuter>();
            HandleInner newInner = newOuter.Inner;

            CheckHandlesConsistency(outer, inner, newOuter, newInner);

            // Get inner object by field.
            IBinaryObject innerObj = outerObj.GetField<IBinaryObject>("inner");

            newInner = innerObj.Deserialize<HandleInner>();
            newOuter = newInner.Outer;

            CheckHandlesConsistency(outer, inner, newOuter, newInner);

            // Get outer object from inner object by handle.
            outerObj = innerObj.GetField<IBinaryObject>("outer");

            newOuter = outerObj.Deserialize<HandleOuter>();
            newInner = newOuter.Inner;

            CheckHandlesConsistency(outer, inner, newOuter, newInner);
        }

        /**
         * <summary>Test handles with exclusive writes.</summary>
         */
        [Test]
        public void TestHandlesExclusive([Values(true, false)] bool detached, [Values(true, false)] bool asbinary)
        {
            var marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations = new List<BinaryTypeConfiguration>
                {
                    new BinaryTypeConfiguration(typeof (HandleInner)),
                    new BinaryTypeConfiguration(typeof (HandleOuterExclusive))
                }
            });

            var inner = new HandleInner
            {
                Before = "inBefore",
                After = "inAfter",
                RawBefore = "inRawBefore",
                RawAfter = "inRawAfter"
            };

            var outer = new HandleOuterExclusive
            {
                Before = "outBefore",
                After = "outAfter",
                RawBefore = "outRawBefore",
                RawAfter = "outRawAfter",
                Inner = inner,
                RawInner = inner
            };

            inner.Outer = outer;
            inner.RawOuter = outer;

            var bytes = asbinary
                ? marsh.Marshal(new Binary(marsh).ToBinary<IBinaryObject>(outer))
                : marsh.Marshal(outer);

            IBinaryObject outerObj;

            if (detached)
            {
                var reader = new BinaryReader(marsh, new BinaryHeapStream(bytes), BinaryMode.ForceBinary, null);

                outerObj = reader.DetachNext().Deserialize<IBinaryObject>();
            }
            else
                outerObj = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            HandleOuter newOuter = outerObj.Deserialize<HandleOuter>();

            Assert.IsFalse(newOuter == newOuter.RawInner.RawOuter);
            Assert.IsFalse(newOuter == newOuter.RawInner.RawOuter);

            Assert.IsFalse(newOuter.Inner == newOuter.RawInner);

            Assert.IsTrue(newOuter.Inner.Outer == newOuter.Inner.RawOuter);
            Assert.IsTrue(newOuter.RawInner.Outer == newOuter.RawInner.RawOuter);

            Assert.IsTrue(newOuter.Inner == newOuter.Inner.Outer.Inner);
            Assert.IsTrue(newOuter.RawInner == newOuter.RawInner.Outer.Inner);
            Assert.IsTrue(newOuter.RawInner == newOuter.RawInner.Outer.RawInner);
        }

        [Test]
        public void TestHandlesCollections()
        {
            var marsh = new Marshaller(new BinaryConfiguration
            {
                TypeConfigurations = new[]
                {
                    new BinaryTypeConfiguration(typeof (HandleCollection))
                },
                CompactFooter = GetCompactFooter()
            });

            // Collection in collection dependency loop
            var collection = new ArrayList {1, 2};
            collection.Add(collection);

            var collectionRaw = new ArrayList(collection);
            collectionRaw.Add(collectionRaw);

            var collectionObj = new ArrayList(collectionRaw);
            collectionObj.Add(collectionObj);

            var dict = new Hashtable { { 1, 1 }, { 2, 2 } };
            dict.Add(3, dict);

            var arr = collectionObj.ToArray();
            arr[1] = arr;

            object entry = new DictionaryEntry(1, 2);
            var dictionaryEntryValSetter = DelegateConverter.CompileFieldSetter(typeof (DictionaryEntry)
                .GetField("_value", BindingFlags.Instance | BindingFlags.NonPublic));
            dictionaryEntryValSetter(entry, entry);  // modify boxed copy to create reference loop

            var data = new HandleCollection
            {
                Collection = collection,
                CollectionRaw = collectionRaw,
                Object = collectionObj,
                Dictionary = dict,
                Array = arr,
                DictionaryEntry = (DictionaryEntry) entry
            };

            var res = marsh.Unmarshal<HandleCollection>(marsh.Marshal(data));

            var resCollection = (ArrayList) res.Collection;
            Assert.AreEqual(collection[0], resCollection[0]);
            Assert.AreEqual(collection[1], resCollection[1]);
            Assert.AreSame(resCollection, resCollection[2]);

            var resCollectionRaw = (ArrayList) res.CollectionRaw;
            Assert.AreEqual(collectionRaw[0], resCollectionRaw[0]);
            Assert.AreEqual(collectionRaw[1], resCollectionRaw[1]);
            Assert.AreSame(resCollection, resCollectionRaw[2]);
            Assert.AreSame(resCollectionRaw, resCollectionRaw[3]);

            var resCollectionObj = (ArrayList) res.Object;
            Assert.AreEqual(collectionObj[0], resCollectionObj[0]);
            Assert.AreEqual(collectionObj[1], resCollectionObj[1]);
            Assert.AreSame(resCollection, resCollectionObj[2]);
            Assert.AreSame(resCollectionRaw, resCollectionObj[3]);
            Assert.AreSame(resCollectionObj, resCollectionObj[4]);

            var resDict = (Hashtable) res.Dictionary;
            Assert.AreEqual(1, resDict[1]);
            Assert.AreEqual(2, resDict[2]);
            Assert.AreSame(resDict, resDict[3]);

            var resArr = res.Array;
            Assert.AreEqual(arr[0], resArr[0]);
            Assert.AreSame(resArr, resArr[1]);
            Assert.AreSame(resCollection, resArr[2]);
            Assert.AreSame(resCollectionRaw, resArr[3]);
            Assert.AreSame(resCollectionObj, resArr[4]);

            var resEntry = res.DictionaryEntry;
            var innerEntry = (DictionaryEntry) resEntry.Value;
            Assert.AreEqual(1, resEntry.Key);
            Assert.AreEqual(1, innerEntry.Key);
            Assert.IsTrue(ReferenceEquals(innerEntry.Value, ((DictionaryEntry) innerEntry.Value).Value));
        }

        ///
        /// <summary>Test KeepSerialized property</summary>
        ///
        [Test]
        public void TestKeepSerializedDefault()
        {
            CheckKeepSerialized(new BinaryConfiguration(), true);
        }

        ///
        /// <summary>Test KeepSerialized property</summary>
        ///
        [Test]
        public void TestKeepSerializedDefaultFalse()
        {
            BinaryConfiguration cfg = new BinaryConfiguration();

            cfg.KeepDeserialized = false;

            CheckKeepSerialized(cfg, false);
        }

        ///
        /// <summary>Test KeepSerialized property</summary>
        ///
        [Test]
        public void TestKeepSerializedTypeCfgFalse()
        {
            BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(typeof(PropertyType));

            typeCfg.KeepDeserialized = false;

            BinaryConfiguration cfg = new BinaryConfiguration();

            cfg.TypeConfigurations = new List<BinaryTypeConfiguration> { typeCfg };

            CheckKeepSerialized(cfg, false);
        }

        ///
        /// <summary>Test KeepSerialized property</summary>
        ///
        [Test]
        public void TestKeepSerializedTypeCfgTrue()
        {
            BinaryTypeConfiguration typeCfg = new BinaryTypeConfiguration(typeof(PropertyType));
            typeCfg.KeepDeserialized = true;

            BinaryConfiguration cfg = new BinaryConfiguration();
            cfg.KeepDeserialized = false;

            cfg.TypeConfigurations = new List<BinaryTypeConfiguration> { typeCfg };

            CheckKeepSerialized(cfg, true);
        }

        /// <summary>
        /// Test correct serialization/deserialization of arrays of special types.
        /// </summary>
        [Test]
        public void TestSpecialArrays()
        {
            ICollection<BinaryTypeConfiguration> typeCfgs =
                new List<BinaryTypeConfiguration>();

            typeCfgs.Add(new BinaryTypeConfiguration(typeof(SpecialArray)));
            typeCfgs.Add(new BinaryTypeConfiguration(typeof(SpecialArrayMarshalAware)));

            BinaryConfiguration cfg = new BinaryConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            Marshaller marsh = new Marshaller(cfg);

            Guid[] guidArr = { Guid.NewGuid() };
            Guid?[] nGuidArr = { Guid.NewGuid() };
            DateTime[] dateArr = { DateTime.Now.ToUniversalTime() };
            DateTime?[] nDateArr = { DateTime.Now.ToUniversalTime() };

            // Use special object.
            SpecialArray obj1 = new SpecialArray
            {
                GuidArr = guidArr,
                NGuidArr = nGuidArr,
                DateArr = dateArr,
                NDateArr = nDateArr
            };

            byte[] bytes = marsh.Marshal(obj1);

            IBinaryObject portObj = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            Assert.IsNotNull(portObj.Deserialize<SpecialArray>());

            Assert.AreEqual(guidArr, portObj.GetField<Guid[]>("guidArr"));
            Assert.AreEqual(nGuidArr, portObj.GetField<Guid?[]>("nGuidArr"));
            Assert.AreEqual(dateArr, portObj.GetField<IBinaryObject[]>("dateArr")
                .Select(x => x.Deserialize<DateTime>()));
            Assert.AreEqual(nDateArr, portObj.GetField<IBinaryObject[]>("nDateArr")
                .Select(x => x.Deserialize<DateTime?>()));

            obj1 = portObj.Deserialize<SpecialArray>();

            Assert.AreEqual(guidArr, obj1.GuidArr);
            Assert.AreEqual(nGuidArr, obj1.NGuidArr);
            Assert.AreEqual(dateArr, obj1.DateArr);
            Assert.AreEqual(nDateArr, obj1.NDateArr);

            // Use special with IGridbinaryMarshalAware.
            SpecialArrayMarshalAware obj2 = new SpecialArrayMarshalAware
            {
                GuidArr = guidArr,
                NGuidArr = nGuidArr,
                DateArr = dateArr,
                NDateArr = nDateArr
            };

            bytes = marsh.Marshal(obj2);

            portObj = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            Assert.AreEqual(guidArr, portObj.GetField<Guid[]>("a"));
            Assert.AreEqual(nGuidArr, portObj.GetField<Guid?[]>("b"));
            Assert.AreEqual(dateArr, portObj.GetField<IBinaryObject[]>("c").Select(x => x.Deserialize<DateTime>()));
            Assert.AreEqual(nDateArr, portObj.GetField<IBinaryObject[]>("d").Select(x => x.Deserialize<DateTime?>()));

            obj2 = portObj.Deserialize<SpecialArrayMarshalAware>();

            Assert.AreEqual(guidArr, obj2.GuidArr);
            Assert.AreEqual(nGuidArr, obj2.NGuidArr);
            Assert.AreEqual(dateArr, obj2.DateArr);
            Assert.AreEqual(nDateArr, obj2.NDateArr);
        }

        /// <summary>
        /// Tests the jagged arrays.
        /// </summary>
        [Test]
        public void TestJaggedArrays()
        {
            int[][] ints = {new[] {1, 2, 3}, new[] {4, 5, 6}};
            Assert.AreEqual(ints, TestUtils.SerializeDeserialize(ints));

            uint[][][] uints = {new[] {new uint[] {1, 2, 3}, new uint[] {4, 5}}};
            Assert.AreEqual(uints, TestUtils.SerializeDeserialize(uints));

            PropertyType[][][] objs = {new[] {new[] {new PropertyType {Field1 = 42}}}};
            Assert.AreEqual(42, TestUtils.SerializeDeserialize(objs)[0][0][0].Field1);

            var obj = new MultidimArrays { JaggedInt = ints, JaggedUInt = uints };
            var resObj = TestUtils.SerializeDeserialize(obj);
            Assert.AreEqual(obj.JaggedInt, resObj.JaggedInt);
            Assert.AreEqual(obj.JaggedUInt, resObj.JaggedUInt);

            var obj2 = new MultidimArraysBinarizable { JaggedInt = ints, JaggedUInt = uints };
            var resObj2 = TestUtils.SerializeDeserialize(obj);
            Assert.AreEqual(obj2.JaggedInt, resObj2.JaggedInt);
            Assert.AreEqual(obj2.JaggedUInt, resObj2.JaggedUInt);
        }

        /// <summary>
        /// Tests the multidimensional arrays.
        /// </summary>
        [Test]
        public void TestMultidimensionalArrays()
        {
            int[,] ints = {{1, 2, 3}, {4, 5, 6}};
            Assert.AreEqual(ints, TestUtils.SerializeDeserialize(ints));

            uint[,,] uints = {{{1, 2, 3}, {4, 5, 6}}, {{7, 8, 9}, {10, 11, 12}}};
            Assert.AreEqual(uints, TestUtils.SerializeDeserialize(uints));

            PropertyType[,] objs = {{new PropertyType {Field1 = 123}}};
            Assert.AreEqual(123, TestUtils.SerializeDeserialize(objs)[0, 0].Field1);
            
            var obj = new MultidimArrays { MultidimInt = ints, MultidimUInt = uints };
            var resObj = TestUtils.SerializeDeserialize(obj);
            Assert.AreEqual(obj.MultidimInt, resObj.MultidimInt);
            Assert.AreEqual(obj.MultidimUInt, resObj.MultidimUInt);

            var obj2 = new MultidimArraysBinarizable { MultidimInt = ints, MultidimUInt = uints };
            var resObj2 = TestUtils.SerializeDeserialize(obj);
            Assert.AreEqual(obj2.MultidimInt, resObj2.MultidimInt);
            Assert.AreEqual(obj2.MultidimUInt, resObj2.MultidimUInt);
        }

        /// <summary>
        /// Tests pointer types.
        /// </summary>
        [Test]
        public unsafe void TestPointers([Values(false, true)] bool raw)
        {
            // Values.
            var vals = new[] {IntPtr.Zero, new IntPtr(long.MinValue), new IntPtr(long.MaxValue)};
            foreach (var intPtr in vals)
            {
                Assert.AreEqual(intPtr, TestUtils.SerializeDeserialize(intPtr));
            }

            var uvals = new[] {UIntPtr.Zero, new UIntPtr(long.MaxValue), new UIntPtr(ulong.MaxValue)};
            foreach (var uintPtr in uvals)
            {
                Assert.AreEqual(uintPtr, TestUtils.SerializeDeserialize(uintPtr));
            }

            // Type fields.
            var ptrs = new Pointers
            {
                ByteP = (byte*) 123,
                IntP = (int*) 456,
                VoidP = (void*) 789,
                IntPtr = new IntPtr(long.MaxValue),
                UIntPtr = new UIntPtr(ulong.MaxValue),
                IntPtrs = new[] {new IntPtr(long.MinValue)},
                UIntPtrs = new[] {new UIntPtr(long.MaxValue), new UIntPtr(ulong.MaxValue)}
            };

            var res = TestUtils.SerializeDeserialize(ptrs, raw);
            
            Assert.IsTrue(ptrs.ByteP == res.ByteP);
            Assert.IsTrue(ptrs.IntP == res.IntP);
            Assert.IsTrue(ptrs.VoidP == res.VoidP);

            Assert.AreEqual(ptrs.IntPtr, res.IntPtr);
            Assert.AreEqual(ptrs.IntPtrs, res.IntPtrs);
            Assert.AreEqual(ptrs.UIntPtr, res.UIntPtr);
            Assert.AreEqual(ptrs.UIntPtrs, res.UIntPtrs);
        }

        /// <summary>
        /// Tests the compact footer setting.
        /// </summary>
        [Test]
        public void TestCompactFooterSetting()
        {
            Assert.AreEqual(GetCompactFooter(), _marsh.CompactFooter);
        }

        /// <summary>
        /// Tests serializing/deserializing objects with IBinaryObject fields.
        /// </summary>
        [Test]
        public void TestBinaryField()
        {
            byte[] dataInner = _marsh.Marshal(new BinaryObjectWrapper());

            IBinaryObject innerObject = _marsh.Unmarshal<IBinaryObject>(dataInner, BinaryMode.ForceBinary);
            BinaryObjectWrapper inner = innerObject.Deserialize<BinaryObjectWrapper>();
            
            Assert.NotNull(inner);

            byte[] dataOuter = _marsh.Marshal(new BinaryObjectWrapper() { Val = innerObject });

            IBinaryObject outerObject = _marsh.Unmarshal<IBinaryObject>(dataOuter, BinaryMode.ForceBinary);
            BinaryObjectWrapper outer = outerObject.Deserialize<BinaryObjectWrapper>();
            
            Assert.NotNull(outer);
            Assert.IsTrue(outer.Val.Equals(innerObject));
        }

        private static void CheckKeepSerialized(BinaryConfiguration cfg, bool expKeep)
        {
            if (cfg.TypeConfigurations == null)
            {
                cfg.TypeConfigurations = new List<BinaryTypeConfiguration>
                {
                    new BinaryTypeConfiguration(typeof(PropertyType))
                };
            }

            Marshaller marsh = new Marshaller(cfg);

            byte[] data = marsh.Marshal(new PropertyType());

            IBinaryObject portNewObj = marsh.Unmarshal<IBinaryObject>(data, BinaryMode.ForceBinary);

            PropertyType deserialized1 = portNewObj.Deserialize<PropertyType>();
            PropertyType deserialized2 = portNewObj.Deserialize<PropertyType>();

            Assert.NotNull(deserialized1);

            Assert.AreEqual(expKeep, deserialized1 == deserialized2);
        }

        private void CheckHandlesConsistency(HandleOuter outer, HandleInner inner, HandleOuter newOuter, 
            HandleInner newInner)
        {
            Assert.True(newOuter != null);
            Assert.AreEqual(outer.Before, newOuter.Before);
            Assert.True(newOuter.Inner == newInner);
            Assert.AreEqual(outer.After, newOuter.After);
            Assert.AreEqual(outer.RawBefore, newOuter.RawBefore);
            Assert.True(newOuter.RawInner == newInner);
            Assert.AreEqual(outer.RawAfter, newOuter.RawAfter);

            Assert.True(newInner != null);
            Assert.AreEqual(inner.Before, newInner.Before);
            Assert.True(newInner.Outer == newOuter);
            Assert.AreEqual(inner.After, newInner.After);
            Assert.AreEqual(inner.RawBefore, newInner.RawBefore);
            Assert.True(newInner.RawOuter == newOuter);
            Assert.AreEqual(inner.RawAfter, newInner.RawAfter);            
        }

        private static void CheckObject(Marshaller marsh, OuterObjectType outObj, InnerObjectType inObj)
        {
            inObj.PInt1 = 1;
            inObj.PInt2 = 2;

            outObj.InObj = inObj;

            byte[] bytes = marsh.Marshal(outObj);

            IBinaryObject portOutObj = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

            OuterObjectType newOutObj = portOutObj.Deserialize<OuterObjectType>();

            Assert.AreEqual(outObj, newOutObj);
        }

        public class OuterObjectType
        {
            public InnerObjectType InObj { get; set; }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                var type = obj as OuterObjectType;
                
                return type != null && Equals(InObj, type.InObj);
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return InObj != null ? InObj.GetHashCode() : 0;
            }
        }

        [Serializable]
        public class InnerObjectType
        {
            public int PInt1 { get; set; }

            public int PInt2 { get; set; }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                var that = obj as InnerObjectType;

                return that != null && (PInt1 == that.PInt1 && PInt2 == that.PInt2);
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return 31 * PInt1 + PInt2;
            }

            /** <inheritdoc /> */
            public override string ToString()
            {
                return "InnerObjectType[pInt1=" + PInt1 + ", pInt2=" + PInt2 + ']';
            }
        }

        public class CollectionsType
        {
            public ICollection Col1 { get; set; }

            public ArrayList Col2 { get; set; }
            
            public TestList Col3 { get; set; }

            public Hashtable Hashtable { get; set; }

            public TestHashTable Hashtable2 { get; set; }

            public Dictionary<int, string> Dict { get; set; }

            public InnerObjectType[] Arr { get; set; }

            public SortedDictionary<string, int> SortedDict { get; set; }

            public LinkedList<int> LinkedList { get; set; }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                var that = obj as CollectionsType;

                return that != null 
                    && CompareCollections(Col1, that.Col1) 
                    && CompareCollections(Col2, that.Col2)
                    && CompareCollections(Hashtable, that.Hashtable)
                    && CompareCollections(Dict, that.Dict)
                    && CompareCollections(Arr, that.Arr)
                    && CompareCollections(SortedDict, that.SortedDict)
                    && CompareCollections(LinkedList, that.LinkedList);
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                int res = 0;

                foreach (var col in new object[] {Col1, Col2, Hashtable, Dict, Arr, SortedDict, LinkedList})
                    res = 31*res + (col != null ? col.GetHashCode() : 0);

                return res;
            }
        }

        public class GenericCollectionsType<TKey, TValue> : IBinarizable
        {
            public ICollection<TKey> Keys { get; set; }

            public ICollection<TValue> Values { get; set; }

            public IDictionary<TKey, TValue> Pairs { get; set; }

            public ICollection<object> Objects { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteObject("Keys", Keys);
                writer.WriteObject("Values", Values);
                writer.WriteObject("Pairs", Pairs);
                writer.WriteObject("Objects", Objects);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Keys = (ICollection<TKey>) reader.ReadObject<object>("Keys");
                Values = (ICollection<TValue>) reader.ReadObject<object>("Values");
                Pairs = (IDictionary<TKey, TValue>) reader.ReadObject<object>("Pairs");
                Objects = (ICollection<object>) reader.ReadObject<object>("Objects");
            }
        }

        public class TestList : ArrayList
        {
            // No-op.
        }

        public class TestHashTable : Hashtable
        {
            public TestHashTable()
            {
                // No-op.
            }

            // ReSharper disable once UnusedMember.Global
            protected TestHashTable(SerializationInfo info, StreamingContext context) : base(info, context)
            {
                // No-op.
            }
        }

        private static bool CompareCollections(ICollection col1, ICollection col2)
        {
            if (col1 == null && col2 == null)
                return true;
            if (col1 == null || col2 == null)
                return false;

            return col1.OfType<object>().SequenceEqual(col2.OfType<object>());
        }

        public class PrimitiveArrayFieldType
        {
            public bool[] PBool { get; set; }

            public sbyte[] PSbyte { get; set; }

            public byte[] PByte { get; set; }

            public short[] PShort { get; set; }

            public ushort[] PUshort { get; set; }

            public char[] PChar { get; set; }

            public int[] PInt { get; set; }

            public uint[] PUint { get; set; }

            public long[] PLong { get; set; }

            public ulong[] PUlong { get; set; }

            public float[] PFloat { get; set; }

            public double[] PDouble { get; set; }

            public string[] PString { get; set; }

            public Guid?[] PGuid { get; set; }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                var other = obj as PrimitiveArrayFieldType;

                return other != null && (PBool == other.PBool &&
                                         PByte == other.PByte &&
                                         PSbyte == other.PSbyte &&
                                         PShort == other.PShort &&
                                         PUshort == other.PUshort &&
                                         PInt == other.PInt &&
                                         PUint == other.PUint &&
                                         PLong == other.PLong &&
                                         PUlong == other.PUlong &&
                                         PChar == other.PChar &&
                                         PFloat == other.PFloat &&
                                         PDouble == other.PDouble &&
                                         PString == other.PString &&
                                         PGuid == other.PGuid);
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return PInt != null && PInt.Length > 0 ? PInt[0].GetHashCode() : 0;
            }
        }

        public class SpecialArray
        {
            public Guid[] GuidArr;
            public Guid?[] NGuidArr;
            public DateTime[] DateArr;
            public DateTime?[] NDateArr;
        }

        public class SpecialArrayMarshalAware : SpecialArray, IBinarizable
        {
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteObject("a", GuidArr);
                writer.WriteObject("b", NGuidArr);
                writer.WriteObject("c", DateArr);
                writer.WriteObject("d", NDateArr);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                GuidArr = reader.ReadObject<Guid[]>("a");
                NGuidArr = reader.ReadObject<Guid?[]>("b");
                DateArr = reader.ReadObject<DateTime[]>("c");
                NDateArr = reader.ReadObject<DateTime?[]>("d");
            }
        }

        public class EnumType
        {
            public TestEnum PEnum { get; set; }

            public TestEnum[] PEnumArray { get; set; }
        }

        [Serializable]
        public class PrimitiveFieldType 
        {
            public PrimitiveFieldType()
            {
                PBool = true;
                PByte = 2;
                PSbyte = 3;
                PShort = 4;
                PUshort = 5;
                PInt = 6;
                PUint = 7;
                PLong = 8;
                PUlong = 9;
                PChar = 'a';
                PFloat = 10;
                PDouble = 11;
                PString = "abc";
                PGuid = Guid.NewGuid();
                PnGuid = Guid.NewGuid();
                PDate = DateTime.UtcNow;
                PnDate = DateTime.UtcNow;
                IgniteGuid = new IgniteGuid(Guid.NewGuid(), 123);
            }

            // Mark all fields with QuerySqlField because reflective serializer takes it into account
            [QuerySqlField]
            public bool PBool { get; set; }

            [QuerySqlField]
            public sbyte PSbyte { get; set; }

            [QuerySqlField]
            public byte PByte { get; set; }

            [QuerySqlField]
            public short PShort { get; set; }

            [QuerySqlField]
            public ushort PUshort { get; set; }

            [QuerySqlField]
            public char PChar { get; set; }

            [QuerySqlField]
            public int PInt { get; set; }

            [QuerySqlField]
            public uint PUint { get; set; }

            [QuerySqlField]
            public long PLong { get; set; }

            [QuerySqlField]
            public ulong PUlong { get; set; }

            [QuerySqlField]
            public float PFloat { get; set; }

            [QuerySqlField]
            public double PDouble { get; set; }

            [QuerySqlField]
            public string PString { get; set; }

            [QuerySqlField]
            public Guid PGuid { get; set; }

            [QuerySqlField]
            public Guid? PnGuid { get; set; }

            [QuerySqlField]
            public DateTime PDate { get; set; }

            [QuerySqlField]
            public DateTime? PnDate { get; set; }

            [QuerySqlField]
            public IgniteGuid IgniteGuid { get; set; }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                var that = obj as PrimitiveFieldType;
                if (that == null)
                    return false;

                return PBool == that.PBool &&
                       PByte == that.PByte &&
                       PSbyte == that.PSbyte &&
                       PShort == that.PShort &&
                       PUshort == that.PUshort &&
                       PInt == that.PInt &&
                       PUint == that.PUint &&
                       PLong == that.PLong &&
                       PUlong == that.PUlong &&
                       PChar == that.PChar &&
                       PFloat == that.PFloat &&
                       PDouble == that.PDouble &&
                       PString == that.PString &&
                       PGuid == that.PGuid &&
                       PnGuid == that.PnGuid &&
                       IgniteGuid == that.IgniteGuid &&
                       PDate == that.PDate &&
                       PnDate == that.PnDate;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return PInt;
            }
        }

        public class PrimitiveFieldBinaryType : PrimitiveFieldType, IBinarizable
        {
            public unsafe void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteBoolean("bool", PBool);
                writer.WriteByte("byte", PByte);
                writer.WriteShort("short", PShort);
                writer.WriteInt("int", PInt);
                writer.WriteLong("long", PLong);
                writer.WriteChar("char", PChar);
                writer.WriteFloat("float", PFloat);
                writer.WriteDouble("double", PDouble);

                sbyte sByte = PSbyte;
                ushort uShort = PUshort;
                uint uInt = PUint;
                ulong uLong = PUlong;

                writer.WriteByte("sbyte", *(byte*)&sByte);
                writer.WriteShort("ushort", *(short*)&uShort);
                writer.WriteInt("uint", *(int*)&uInt);
                writer.WriteLong("ulong", *(long*)&uLong);

                writer.WriteString("string", PString);
                writer.WriteGuid("guid", PGuid);
                writer.WriteGuid("nguid", PnGuid);

                writer.WriteObject("date", PDate);
                writer.WriteObject("ndate", PnDate);

                writer.WriteObject("iguid", IgniteGuid);
            }

            public unsafe void ReadBinary(IBinaryReader reader)
            {
                PBool = reader.ReadBoolean("bool");
                PByte = reader.ReadByte("byte");
                PShort = reader.ReadShort("short");
                PInt = reader.ReadInt("int");

                PLong = reader.ReadLong("long");
                PChar = reader.ReadChar("char");
                PFloat = reader.ReadFloat("float");
                PDouble = reader.ReadDouble("double");

                byte sByte = reader.ReadByte("sbyte");
                short uShort = reader.ReadShort("ushort");
                int uInt = reader.ReadInt("uint");
                long uLong = reader.ReadLong("ulong");

                PSbyte = *(sbyte*)&sByte;
                PUshort = *(ushort*)&uShort;
                PUint = *(uint*)&uInt;
                PUlong = *(ulong*)&uLong;

                PString = reader.ReadString("string");
                PGuid = reader.ReadObject<Guid>("guid");
                PnGuid = reader.ReadGuid("nguid");

                PDate = reader.ReadObject<DateTime>("date");
                PnDate = reader.ReadObject<DateTime?>("ndate");

                IgniteGuid = reader.ReadObject<IgniteGuid>("iguid");
            }
        }

        public class PrimitiveFieldRawBinaryType : PrimitiveFieldType, IBinarizable
        {
            public unsafe void WriteBinary(IBinaryWriter writer)
            {
                IBinaryRawWriter rawWriter = writer.GetRawWriter();

                rawWriter.WriteBoolean(PBool);
                rawWriter.WriteByte(PByte);
                rawWriter.WriteShort(PShort);
                rawWriter.WriteInt(PInt);
                rawWriter.WriteLong(PLong);
                rawWriter.WriteChar(PChar);
                rawWriter.WriteFloat(PFloat);
                rawWriter.WriteDouble(PDouble);

                sbyte sByte = PSbyte;
                ushort uShort = PUshort;
                uint uInt = PUint;
                ulong uLong = PUlong;

                rawWriter.WriteByte(*(byte*)&sByte);
                rawWriter.WriteShort(*(short*)&uShort);
                rawWriter.WriteInt(*(int*)&uInt);
                rawWriter.WriteLong(*(long*)&uLong);

                rawWriter.WriteString(PString);
                rawWriter.WriteGuid(PGuid);
                rawWriter.WriteGuid(PnGuid);

                rawWriter.WriteObject(PDate);
                rawWriter.WriteObject(PnDate);

                rawWriter.WriteObject(IgniteGuid);
            }

            public unsafe void ReadBinary(IBinaryReader reader)
            {
                IBinaryRawReader rawReader = reader.GetRawReader();

                PBool = rawReader.ReadBoolean();
                PByte = rawReader.ReadByte();
                PShort = rawReader.ReadShort();
                PInt = rawReader.ReadInt();

                PLong = rawReader.ReadLong();
                PChar = rawReader.ReadChar();
                PFloat = rawReader.ReadFloat();
                PDouble = rawReader.ReadDouble();

                byte sByte = rawReader.ReadByte();
                short uShort = rawReader.ReadShort();
                int uInt = rawReader.ReadInt();
                long uLong = rawReader.ReadLong();

                PSbyte = *(sbyte*)&sByte;
                PUshort = *(ushort*)&uShort;
                PUint = *(uint*)&uInt;
                PUlong = *(ulong*)&uLong;

                PString = rawReader.ReadString();
                PGuid = rawReader.ReadGuid().Value;
                PnGuid = rawReader.ReadGuid();

                PDate = rawReader.ReadObject<DateTime>();
                PnDate = rawReader.ReadObject<DateTime?>();

                IgniteGuid = rawReader.ReadObject<IgniteGuid>();
            }
        }

        public class PrimitiveFieldsSerializer : IBinarySerializer
        {
            public unsafe void WriteBinary(object obj, IBinaryWriter writer)
            {
                PrimitiveFieldType obj0 = (PrimitiveFieldType)obj;

                writer.WriteBoolean("bool", obj0.PBool);
                writer.WriteByte("byte", obj0.PByte);
                writer.WriteShort("short", obj0.PShort);
                writer.WriteInt("int", obj0.PInt);
                writer.WriteLong("long", obj0.PLong);
                writer.WriteChar("char", obj0.PChar);
                writer.WriteFloat("float", obj0.PFloat);
                writer.WriteDouble("double", obj0.PDouble);

                sbyte sByte = obj0.PSbyte;
                ushort uShort = obj0.PUshort;
                uint uInt = obj0.PUint;
                ulong uLong = obj0.PUlong;

                writer.WriteByte("sbyte", *(byte*)&sByte);
                writer.WriteShort("ushort", *(short*)&uShort);
                writer.WriteInt("uint", *(int*)&uInt);
                writer.WriteLong("ulong", *(long*)&uLong);

                writer.WriteString("string", obj0.PString);
                writer.WriteGuid("guid", obj0.PGuid);
                writer.WriteGuid("nguid", obj0.PnGuid);

                writer.WriteObject("date", obj0.PDate);
                writer.WriteObject("ndate", obj0.PnDate);

                writer.WriteObject("iguid", obj0.IgniteGuid);
            }

            public unsafe void ReadBinary(object obj, IBinaryReader reader)
            {
                PrimitiveFieldType obj0 = (PrimitiveFieldType)obj;

                obj0.PBool = reader.ReadBoolean("bool");
                obj0.PByte = reader.ReadByte("byte");
                obj0.PShort = reader.ReadShort("short");
                obj0.PInt = reader.ReadInt("int");

                obj0.PLong = reader.ReadLong("long");
                obj0.PChar = reader.ReadChar("char");
                obj0.PFloat = reader.ReadFloat("float");
                obj0.PDouble = reader.ReadDouble("double");

                byte sByte = reader.ReadByte("sbyte");
                short uShort = reader.ReadShort("ushort");
                int uInt = reader.ReadInt("uint");
                long uLong = reader.ReadLong("ulong");

                obj0.PSbyte = *(sbyte*)&sByte;
                obj0.PUshort = *(ushort*)&uShort;
                obj0.PUint = *(uint*)&uInt;
                obj0.PUlong = *(ulong*)&uLong;

                obj0.PString = reader.ReadString("string");
                obj0.PGuid = reader.ReadObject<Guid>("guid");
                obj0.PnGuid = reader.ReadGuid("nguid");

                obj0.PDate = reader.ReadObject<DateTime>("date");
                obj0.PnDate = reader.ReadObject<DateTime?>("ndate");

                obj0.IgniteGuid = reader.ReadObject<IgniteGuid>("iguid");
            }
        }

        public class PrimitiveFieldsRawSerializer : IBinarySerializer
        {
            public unsafe void WriteBinary(object obj, IBinaryWriter writer)
            {
                PrimitiveFieldType obj0 = (PrimitiveFieldType)obj;

                IBinaryRawWriter rawWriter = writer.GetRawWriter();

                rawWriter.WriteBoolean(obj0.PBool);
                rawWriter.WriteByte(obj0.PByte);
                rawWriter.WriteShort( obj0.PShort);
                rawWriter.WriteInt( obj0.PInt);
                rawWriter.WriteLong( obj0.PLong);
                rawWriter.WriteChar(obj0.PChar);
                rawWriter.WriteFloat(obj0.PFloat);
                rawWriter.WriteDouble( obj0.PDouble);

                sbyte sByte = obj0.PSbyte;
                ushort uShort = obj0.PUshort;
                uint uInt = obj0.PUint;
                ulong uLong = obj0.PUlong;

                rawWriter.WriteByte(*(byte*)&sByte);
                rawWriter.WriteShort(*(short*)&uShort);
                rawWriter.WriteInt(*(int*)&uInt);
                rawWriter.WriteLong(*(long*)&uLong);

                rawWriter.WriteString(obj0.PString);
                rawWriter.WriteGuid(obj0.PGuid);
                rawWriter.WriteGuid(obj0.PnGuid);

                rawWriter.WriteObject(obj0.PDate);
                rawWriter.WriteObject(obj0.PnDate);

                rawWriter.WriteObject(obj0.IgniteGuid);
            }

            public unsafe void ReadBinary(object obj, IBinaryReader reader)
            {
                PrimitiveFieldType obj0 = (PrimitiveFieldType)obj;

                IBinaryRawReader rawReader = reader.GetRawReader();

                obj0.PBool = rawReader.ReadBoolean();
                obj0.PByte = rawReader.ReadByte();
                obj0.PShort = rawReader.ReadShort();
                obj0.PInt = rawReader.ReadInt();
                obj0.PLong = rawReader.ReadLong();
                obj0.PChar = rawReader.ReadChar();
                obj0.PFloat = rawReader.ReadFloat();
                obj0.PDouble = rawReader.ReadDouble();

                byte sByte = rawReader.ReadByte();
                short uShort = rawReader.ReadShort();
                int uInt = rawReader.ReadInt();
                long uLong = rawReader.ReadLong();

                obj0.PSbyte = *(sbyte*)&sByte;
                obj0.PUshort = *(ushort*)&uShort;
                obj0.PUint = *(uint*)&uInt;
                obj0.PUlong = *(ulong*)&uLong;

                obj0.PString = rawReader.ReadString();
                obj0.PGuid = rawReader.ReadGuid().Value;
                obj0.PnGuid = rawReader.ReadGuid();

                obj0.PDate = rawReader.ReadObject<DateTime>();
                obj0.PnDate = rawReader.ReadObject<DateTime?>();

                obj0.IgniteGuid = rawReader.ReadObject<IgniteGuid>();
            }
        }

        public class HandleOuter : IBinarizable
        {
            public string Before;
            public HandleInner Inner;
            public string After;

            public string RawBefore;
            public HandleInner RawInner;
            public string RawAfter;

            /** <inheritdoc /> */
            virtual public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteString("before", Before);
                writer.WriteObject("inner", Inner);
                writer.WriteString("after", After);

                IBinaryRawWriter rawWriter = writer.GetRawWriter();

                rawWriter.WriteString(RawBefore);
                rawWriter.WriteObject(RawInner);
                rawWriter.WriteString(RawAfter);
            }

            /** <inheritdoc /> */
            virtual public void ReadBinary(IBinaryReader reader)
            {
                Before = reader.ReadString("before");
                Inner = reader.ReadObject<HandleInner>("inner");
                After = reader.ReadString("after");

                IBinaryRawReader rawReader = reader.GetRawReader();

                RawBefore = rawReader.ReadString();
                RawInner = rawReader.ReadObject<HandleInner>();
                RawAfter = rawReader.ReadString();
            }
        }

        public class HandleInner : IBinarizable
        {
            public string Before;
            public HandleOuter Outer;
            public string After;

            public string RawBefore;
            public HandleOuter RawOuter;
            public string RawAfter;

            /** <inheritdoc /> */
            virtual public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteString("before", Before);
                writer.WriteObject("outer", Outer);
                writer.WriteString("after", After);

                IBinaryRawWriter rawWriter = writer.GetRawWriter();

                rawWriter.WriteString(RawBefore);
                rawWriter.WriteObject(RawOuter);
                rawWriter.WriteString(RawAfter);
            }

            /** <inheritdoc /> */
            virtual public void ReadBinary(IBinaryReader reader)
            {
                Before = reader.ReadString("before");
                Outer = reader.ReadObject<HandleOuter>("outer");
                After = reader.ReadString("after");

                IBinaryRawReader rawReader = reader.GetRawReader();

                RawBefore = rawReader.ReadString();
                RawOuter = rawReader.ReadObject<HandleOuter>();
                RawAfter = rawReader.ReadString();
            }
        }


        public class HandleOuterExclusive : HandleOuter
        {
            /** <inheritdoc /> */
            override public void WriteBinary(IBinaryWriter writer)
            {
                BinaryWriter writer0 = (BinaryWriter)writer;

                writer.WriteString("before", Before);

                writer0.WriteObject("inner", Inner);
                
                writer.WriteString("after", After);

                IBinaryRawWriter rawWriter = writer.GetRawWriter();

                rawWriter.WriteString(RawBefore);

                writer0.WriteObjectDetached(RawInner);

                rawWriter.WriteString(RawAfter);
            }

            /** <inheritdoc /> */
            override public void ReadBinary(IBinaryReader reader)
            {
                var reader0 = (BinaryReader) reader;

                Before = reader0.ReadString("before");

                reader0.DetachNext();
                Inner = reader0.ReadObject<HandleInner>("inner");

                After = reader0.ReadString("after");

                var rawReader = (BinaryReader) reader.GetRawReader();

                RawBefore = rawReader.ReadString();

                reader0.DetachNext();
                RawInner = rawReader.ReadObject<HandleInner>();

                RawAfter = rawReader.ReadString();
            }
        }

        public class HandleCollection : IBinarizable
        {
            public ICollection Collection { get; set; }
            public IDictionary Dictionary { get; set; }
            public DictionaryEntry DictionaryEntry { get; set; }
            public ICollection CollectionRaw { get; set; }
            public object Object { get; set; }
            public object[] Array { get; set; }

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteCollection("1col", Collection);
                writer.WriteDictionary("2dict", Dictionary);
                writer.WriteObject("3dictEntry", DictionaryEntry);
                writer.WriteObject("4obj", Object);
                writer.WriteArray("5arr", Array);
                writer.GetRawWriter().WriteCollection(CollectionRaw);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Collection = reader.ReadCollection("1col");
                Dictionary = reader.ReadDictionary("2dict");
                DictionaryEntry = reader.ReadObject<DictionaryEntry>("3dictEntry");
                Object = reader.ReadObject<object>("4obj");
                Array = reader.ReadArray<object>("5arr");
                CollectionRaw = reader.GetRawReader().ReadCollection();
            }
        }

        public class PropertyType
        {
            public int Field1;

            public int Field2
            {
                get;
                set;
            }
        }

        public enum TestEnum : short
        {
            Val1, Val2, Val3 = 10
        }

        public class DecimalReflective
        {
            /** */
            public decimal? Val;

            /** */
            public decimal?[] ValArr;
        }

        public class DecimalMarshalAware : DecimalReflective, IBinarizable
        {
            /** */
            public decimal? RawVal;

            /** */
            public decimal?[] RawValArr;

            /** <inheritDoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteDecimal("val", Val);
                writer.WriteDecimalArray("valArr", ValArr);

                IBinaryRawWriter rawWriter = writer.GetRawWriter();

                rawWriter.WriteDecimal(RawVal);
                rawWriter.WriteDecimalArray(RawValArr);
            }

            /** <inheritDoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                Val = reader.ReadDecimal("val");
                ValArr = reader.ReadDecimalArray("valArr");

                IBinaryRawReader rawReader = reader.GetRawReader();

                RawVal = rawReader.ReadDecimal();
                RawValArr = rawReader.ReadDecimalArray();
            }
        }

        /// <summary>
        /// Date time type.
        /// </summary>
        public class DateTimeType : IBinarizable
        {
            public DateTime Utc;

            public DateTime? UtcNull;

            public DateTime?[] UtcArr;

            public DateTime UtcRaw;

            public DateTime? UtcNullRaw;

            public DateTime?[] UtcArrRaw;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="now">Current local time.</param>
            public DateTimeType(DateTime now)
            {
                Utc = now.ToUniversalTime();

                UtcNull = Utc;

                UtcArr = new DateTime?[] { Utc };

                UtcRaw = Utc;

                UtcNullRaw = UtcNull;

                UtcArrRaw = new[] { UtcArr[0] };
            }

            /** <inheritDoc /> */
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteTimestamp("utc", Utc);
                writer.WriteTimestamp("utcNull", UtcNull);
                writer.WriteTimestampArray("utcArr", UtcArr);

                IBinaryRawWriter rawWriter = writer.GetRawWriter();

                rawWriter.WriteTimestamp(UtcRaw);
                rawWriter.WriteTimestamp(UtcNullRaw);
                rawWriter.WriteTimestampArray(UtcArrRaw);
            }

            /** <inheritDoc /> */
            public void ReadBinary(IBinaryReader reader)
            {
                Utc = reader.ReadTimestamp("utc").Value;
                UtcNull = reader.ReadTimestamp("utc").Value;
                UtcArr = reader.ReadTimestampArray("utcArr");

                IBinaryRawReader rawReader = reader.GetRawReader();

                UtcRaw = rawReader.ReadTimestamp().Value;
                UtcNullRaw = rawReader.ReadTimestamp().Value;
                UtcArrRaw = rawReader.ReadTimestampArray();
            }
        }

        [Serializable]
        private class SerializableObject
        {
            public int Foo { get; set; }

            private bool Equals(SerializableObject other)
            {
                return Foo == other.Foo;
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != GetType()) return false;

                return Equals((SerializableObject) obj);
            }

            public override int GetHashCode()
            {
                return Foo;
            }
        }

        private struct ReflectiveStruct : IEquatable<ReflectiveStruct>
        {
            private readonly int _x;
            private readonly double _y;

            public ReflectiveStruct(int x, double y)
            {
                _x = x;
                _y = y;
            }

            public int X
            {
                get { return _x; }
            }

            public double Y
            {
                get { return _y; }
            }

            public bool Equals(ReflectiveStruct other)
            {
                return _x == other._x && _y.Equals(other._y);
            }

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj))
                    return false;

                return obj is ReflectiveStruct && Equals((ReflectiveStruct) obj);
            }

            public override int GetHashCode()
            {
                unchecked
                {
                    return (_x*397) ^ _y.GetHashCode();
                }
            }

            public static bool operator ==(ReflectiveStruct left, ReflectiveStruct right)
            {
                return left.Equals(right);
            }

            public static bool operator !=(ReflectiveStruct left, ReflectiveStruct right)
            {
                return !left.Equals(right);
            }
        }

        private class MultidimArrays
        {
            public int[][] JaggedInt { get; set; }
            public uint[][][] JaggedUInt { get; set; }

            public int[,] MultidimInt { get; set; }
            public uint[,,] MultidimUInt { get; set; }
        }

        private class MultidimArraysBinarizable : IBinarizable
        {
            public int[][] JaggedInt { get; set; }
            public uint[][][] JaggedUInt { get; set; }

            public int[,] MultidimInt { get; set; }
            public uint[,,] MultidimUInt { get; set; }
            
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteObject("JaggedInt", JaggedInt);
                writer.WriteObject("JaggedUInt", JaggedUInt);

                writer.WriteObject("MultidimInt", MultidimInt);
                writer.WriteObject("MultidimUInt", MultidimUInt);
            }

            public void ReadBinary(IBinaryReader reader)
            {
                JaggedInt = reader.ReadObject<int[][]>("JaggedInt");
                JaggedUInt = reader.ReadObject<uint[][][]>("JaggedUInt");

                MultidimInt = reader.ReadObject<int[,]>("MultidimInt");
                MultidimUInt = reader.ReadObject<uint[,,]>("MultidimUInt");
            }
        }

        private unsafe class Pointers
        {
            public IntPtr IntPtr { get; set; }
            public IntPtr[] IntPtrs { get; set; }
            public UIntPtr UIntPtr { get; set; }
            public UIntPtr[] UIntPtrs { get; set; }
            public byte* ByteP { get; set; }
            public int* IntP { get; set; }
            public void* VoidP { get; set; }
        }

        private class BinaryObjectWrapper
        {
            public IBinaryObject Val;
        }
    }
}
