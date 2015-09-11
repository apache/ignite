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
namespace Apache.Ignite.Core.Tests.Portable 
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Linq;
    using System.Text;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Portable;
    using NUnit.Framework;

    /// <summary>
    /// 
    /// </summary>
    [TestFixture]
    public class PortableSelfTest { 
        /** */
        private PortableMarshaller _marsh;

        /// <summary>
        /// 
        /// </summary>
        [TestFixtureSetUp]
        public void BeforeTest()
        {
            _marsh = new PortableMarshaller(null);
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
            Assert.AreEqual(_marsh.Unmarshal<bool?>(_marsh.Marshal(null)), null);
        }

        /**
         * <summary>Check write of primitive boolean array.</summary>
         */
        [Test]
        public void TestWritePrimitiveBoolArray()
        {
            bool[] vals = { true, false };

            Assert.AreEqual(_marsh.Unmarshal<bool[]>(_marsh.Marshal(vals)), vals);
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
            Assert.AreEqual(_marsh.Unmarshal<sbyte?>(_marsh.Marshal(null)), null);
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
            Assert.AreEqual(_marsh.Unmarshal<byte?>(_marsh.Marshal(null)), null);
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
            Assert.AreEqual(_marsh.Unmarshal<short?>(_marsh.Marshal(null)), null);
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
            Assert.AreEqual(_marsh.Unmarshal<ushort?>(_marsh.Marshal(null)), null);
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
            Assert.AreEqual(_marsh.Unmarshal<char?>(_marsh.Marshal(null)), null);
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
            Assert.AreEqual(_marsh.Unmarshal<int?>(_marsh.Marshal(null)), null);
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
            Assert.AreEqual(_marsh.Unmarshal<uint?>(_marsh.Marshal(null)), null);
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
            Assert.AreEqual(_marsh.Unmarshal<long?>(_marsh.Marshal(null)), null);
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
            Assert.AreEqual(_marsh.Unmarshal<ulong?>(_marsh.Marshal(null)), null);
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
            Assert.AreEqual(_marsh.Unmarshal<float?>(_marsh.Marshal(null)), null);
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
            Assert.AreEqual(_marsh.Unmarshal<double?>(_marsh.Marshal(null)), null);
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
            Assert.AreEqual(_marsh.Unmarshal<decimal?>(_marsh.Marshal(null)), null);
        }

        /**
         * <summary>Check write of decimal array.</summary>
         */
        [Test]
        public void TestWritePrimitiveDecimalArray()
        {
            decimal[] vals = { decimal.One, decimal.Parse("11,12") };
            decimal[] newVals = _marsh.Unmarshal<decimal[]>(_marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of string.</summary>
         */
        [Test]
        public void TestWriteString()
        {
            Assert.AreEqual(_marsh.Unmarshal<string>(_marsh.Marshal("str")), "str");
            Assert.AreEqual(_marsh.Unmarshal<string>(_marsh.Marshal(null)), null);
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

        /**
        * <summary>Check write of enum.</summary>
        */
        [Test]
        public void TestWriteEnum()
        {
            TestEnum val = TestEnum.Val1;

            Assert.AreEqual(_marsh.Unmarshal<TestEnum>(_marsh.Marshal(val)), val);
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

        /**
         * <summary>Check write of date.</summary>
         */
        [Test]
        public void TestWriteDate() {
            DateTime time = DateTime.Now.ToUniversalTime();

            Assert.AreEqual(_marsh.Unmarshal<DateTime>(_marsh.Marshal(time)), time);
        }

        /// <summary>
        /// Test object with dates.
        /// </summary>
        [Test]
        public void TestDateObject()
        {
            ICollection<PortableTypeConfiguration> typeCfgs =
                new List<PortableTypeConfiguration>();

            typeCfgs.Add(new PortableTypeConfiguration(typeof(DateTimeType)));

            PortableConfiguration cfg = new PortableConfiguration {TypeConfigurations = typeCfgs};

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            DateTime now = DateTime.Now;

            DateTimeType obj = new DateTimeType(now);

            DateTimeType otherObj = marsh.Unmarshal<DateTimeType>(marsh.Marshal(obj));

            Assert.AreEqual(obj.Loc, otherObj.Loc);
            Assert.AreEqual(obj.Utc, otherObj.Utc);
            Assert.AreEqual(obj.LocNull, otherObj.LocNull);
            Assert.AreEqual(obj.UtcNull, otherObj.UtcNull);            
            Assert.AreEqual(obj.LocArr, otherObj.LocArr);
            Assert.AreEqual(obj.UtcArr, otherObj.UtcArr);

            Assert.AreEqual(obj.LocRaw, otherObj.LocRaw);
            Assert.AreEqual(obj.UtcRaw, otherObj.UtcRaw);
            Assert.AreEqual(obj.LocNullRaw, otherObj.LocNullRaw);
            Assert.AreEqual(obj.UtcNullRaw, otherObj.UtcNullRaw);
            Assert.AreEqual(obj.LocArrRaw, otherObj.LocArrRaw);
            Assert.AreEqual(obj.UtcArrRaw, otherObj.UtcArrRaw);
        }

        /**
         * <summary>Check generic collections.</summary>
         */
        [Test]
        public void TestGenericCollections()
        {
            ICollection<string> list = new List<string>();

            list.Add("1");

            byte[] data = _marsh.Marshal(list);

            ICollection<object> newObjList = _marsh.Unmarshal<List<object>>(data);

            Assert.NotNull(newObjList);

            ICollection<string> newList = new List<string>();

            foreach (object obj in newObjList)
                newList.Add((string)obj);

            CollectionAssert.AreEquivalent(list, newList);
        }

        /**
         * <summary>Check property read.</summary>
         */
        [Test]
        public void TestProperty()
        {
            ICollection<PortableTypeConfiguration> typeCfgs = 
                new List<PortableTypeConfiguration>();

            typeCfgs.Add(new PortableTypeConfiguration(typeof(PropertyType)));

            PortableConfiguration cfg = new PortableConfiguration {TypeConfigurations = typeCfgs};

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            PropertyType obj = new PropertyType
            {
                Field1 = 1,
                Field2 = 2
            };

            byte[] data = marsh.Marshal(obj);

            PropertyType newObj = marsh.Unmarshal<PropertyType>(data);

            Assert.AreEqual(obj.Field1, newObj.Field1);
            Assert.AreEqual(obj.Field2, newObj.Field2);

            IPortableObject portNewObj = marsh.Unmarshal<IPortableObject>(data, PortableMode.ForcePortable);

            Assert.AreEqual(obj.Field1, portNewObj.Field<int>("field1"));
            Assert.AreEqual(obj.Field2, portNewObj.Field<int>("Field2"));
        }

        /**
         * <summary>Check write of primitive fields through reflection.</summary>
         */
        [Test]
        public void TestPrimitiveFieldsReflective()
        {
            ICollection<PortableTypeConfiguration> typeCfgs = 
                new List<PortableTypeConfiguration>();

            typeCfgs.Add(new PortableTypeConfiguration(typeof(PrimitiveFieldType)));

            PortableConfiguration cfg = new PortableConfiguration {TypeConfigurations = typeCfgs};

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            PrimitiveFieldType obj = new PrimitiveFieldType();

            CheckPrimitiveFields(marsh, obj);
        }

        /**
         * <summary>Check write of primitive fields through portable interface.</summary>
         */
        [Test]
        public void TestPrimitiveFieldsPortable()
        {
            ICollection<PortableTypeConfiguration> typeCfgs = 
                new List<PortableTypeConfiguration>();

            typeCfgs.Add(new PortableTypeConfiguration(typeof(PrimitiveFieldPortableType)));

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            PrimitiveFieldPortableType obj = new PrimitiveFieldPortableType();

            CheckPrimitiveFields(marsh, obj);
        }

        /**
         * <summary>Check write of primitive fields through portable interface.</summary>
         */
        [Test]
        public void TestPrimitiveFieldsRawPortable()
        {
            ICollection<PortableTypeConfiguration> typeCfgs = 
                new List<PortableTypeConfiguration>();

            typeCfgs.Add(new PortableTypeConfiguration(typeof(PrimitiveFieldRawPortableType)));

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            PrimitiveFieldRawPortableType obj = new PrimitiveFieldRawPortableType();

            CheckPrimitiveFields(marsh, obj);
        }

        /**
         * <summary>Check write of primitive fields through portable interface.</summary>
         */
        [Test]
        public void TestPrimitiveFieldsSerializer()
        {
            var typeCfgs = new List<PortableTypeConfiguration>
            {
                new PortableTypeConfiguration(typeof (PrimitiveFieldType))
                {
                    Serializer = new PrimitiveFieldsSerializer()
                }
            };

            PortableConfiguration cfg = new PortableConfiguration {TypeConfigurations = typeCfgs};

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            PrimitiveFieldType obj = new PrimitiveFieldType();

            CheckPrimitiveFields(marsh, obj);
        }

        /**
         * <summary>Check decimals.</summary>
         */
        [Test]
        public void TestDecimalFields()
        {
            PortableConfiguration cfg = new PortableConfiguration
            {
                TypeConfigurations = new List<PortableTypeConfiguration>
                {
                    new PortableTypeConfiguration(typeof (DecimalReflective)),
                    new PortableTypeConfiguration(typeof (DecimalMarshalAware))
                }
            };

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            // 1. Test reflective stuff.
            DecimalReflective obj1 = new DecimalReflective
            {
                Val = decimal.Zero,
                ValArr = new[] {decimal.One, decimal.MinusOne}
            };

            IPortableObject portObj = marsh.Unmarshal<IPortableObject>(marsh.Marshal(obj1), PortableMode.ForcePortable);

            Assert.AreEqual(obj1.Val, portObj.Field<decimal>("val"));
            Assert.AreEqual(obj1.ValArr, portObj.Field<decimal[]>("valArr"));

            Assert.AreEqual(obj1.Val, portObj.Deserialize<DecimalReflective>().Val);
            Assert.AreEqual(obj1.ValArr, portObj.Deserialize<DecimalReflective>().ValArr);

            // 2. Test marshal aware stuff.
            DecimalMarshalAware obj2 = new DecimalMarshalAware();

            obj2.Val = decimal.Zero;
            obj2.ValArr = new[] { decimal.One, decimal.MinusOne };
            obj2.RawVal = decimal.MaxValue;
            obj2.RawValArr = new[] { decimal.MinusOne, decimal.One} ;

            portObj = marsh.Unmarshal<IPortableObject>(marsh.Marshal(obj2), PortableMode.ForcePortable);

            Assert.AreEqual(obj2.Val, portObj.Field<decimal>("val"));
            Assert.AreEqual(obj2.ValArr, portObj.Field<decimal[]>("valArr"));

            Assert.AreEqual(obj2.Val, portObj.Deserialize<DecimalMarshalAware>().Val);
            Assert.AreEqual(obj2.ValArr, portObj.Deserialize<DecimalMarshalAware>().ValArr);
            Assert.AreEqual(obj2.RawVal, portObj.Deserialize<DecimalMarshalAware>().RawVal);
            Assert.AreEqual(obj2.RawValArr, portObj.Deserialize<DecimalMarshalAware>().RawValArr);
        }

        /**
         * <summary>Check write of primitive fields through raw serializer.</summary>
         */
        [Test]
        public void TestPrimitiveFieldsRawSerializer()
        {
            ICollection<PortableTypeConfiguration> typeCfgs = 
                new List<PortableTypeConfiguration>();

            PortableTypeConfiguration typeCfg =
                new PortableTypeConfiguration(typeof(PrimitiveFieldType));

            typeCfg.Serializer = new PrimitiveFieldsRawSerializer();

            typeCfgs.Add(typeCfg);

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            PrimitiveFieldType obj = new PrimitiveFieldType();

            CheckPrimitiveFields(marsh, obj);
        }

        private void CheckPrimitiveFields(PortableMarshaller marsh, PrimitiveFieldType obj)
        {
            obj.PBool = true;
            obj.PByte = 2;
            obj.PSbyte = 3;
            obj.PShort = 4;
            obj.PUshort = 5;
            obj.PInt = 6;
            obj.PUint = 7;
            obj.PLong = 8;
            obj.PUlong = 9;
            obj.PChar = 'a';
            obj.PFloat = 10;
            obj.PDouble = 11;
            obj.PString = "abc";
            obj.PGuid = Guid.NewGuid();
            obj.PnGuid = Guid.NewGuid();
            
            //CheckPrimitiveFieldsSerialization(marsh, obj);

            //obj.PString = "";

            //CheckPrimitiveFieldsSerialization(marsh, obj);

            //obj.PString = null;

            //CheckPrimitiveFieldsSerialization(marsh, obj);

            //obj.PString = null;
            //obj.PNGuid = null;

            CheckPrimitiveFieldsSerialization(marsh, obj);
        }

        private void CheckPrimitiveFieldsSerialization(PortableMarshaller marsh, PrimitiveFieldType obj)
        {
            byte[] bytes = marsh.Marshal(obj);

            IPortableObject portObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.ForcePortable);

            Assert.AreEqual(obj.GetHashCode(), portObj.GetHashCode());

            PrimitiveFieldType newObj = portObj.Deserialize<PrimitiveFieldType>();

            Assert.AreEqual(obj, newObj);
        }

        /**
         * <summary>Check write of object with enums.</summary>
         */
        [Test]
        public void TestEnumsReflective()
        {
            PortableMarshaller marsh =
                new PortableMarshaller(new PortableConfiguration
                {
                    TypeConfigurations =
                        new List<PortableTypeConfiguration> {new PortableTypeConfiguration(typeof (EnumType))}
                });

            EnumType obj = new EnumType
            {
                PEnum = TestEnum.Val1,
                PEnumArray = new[] {TestEnum.Val2, TestEnum.Val3}
            };

            byte[] bytes = marsh.Marshal(obj);

            IPortableObject portObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.ForcePortable);

            Assert.AreEqual(obj.GetHashCode(), portObj.GetHashCode());

            EnumType newObj = portObj.Deserialize<EnumType>();

            Assert.AreEqual(obj.PEnum, newObj.PEnum);
            Assert.AreEqual(obj.PEnumArray, newObj.PEnumArray);
        }

        /**
         * <summary>Check write of object with collections.</summary>
         */
        [Test]
        public void TestCollectionsReflective()
        {
            ICollection<PortableTypeConfiguration> typeCfgs =
                new List<PortableTypeConfiguration>();

            typeCfgs.Add(new PortableTypeConfiguration(typeof(CollectionsType)));
            typeCfgs.Add(new PortableTypeConfiguration(typeof(InnerObjectType)));

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            CollectionsType obj = new CollectionsType();

            ArrayList list = new ArrayList();

            list.Add(true);
            list.Add((byte)1);
            list.Add((short)2);
            list.Add('a');
            list.Add(3);
            list.Add((long)4);
            list.Add((float)5);
            list.Add((double)6);

            list.Add("string");
            list.Add(Guid.NewGuid());

            InnerObjectType innerObj = new InnerObjectType();

            innerObj.PInt1 = 1;
            innerObj.PInt2 = 2;
            
            list.Add(innerObj);

            obj.Col1 = list;

            byte[] bytes = marsh.Marshal(obj);

            IPortableObject portObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.ForcePortable);

            Assert.AreEqual(obj.GetHashCode(), portObj.GetHashCode());

            CollectionsType newObj = portObj.Deserialize<CollectionsType>();

            Assert.AreEqual(obj, newObj);

            obj.Col1 = null;

            Assert.AreEqual(obj, marsh.Unmarshal<CollectionsType>(marsh.Marshal(obj)));

            obj.Col1 = list;
            obj.Col2 = list;

            Assert.AreEqual(obj, marsh.Unmarshal<CollectionsType>(marsh.Marshal(obj)));

            obj.Col2 = new TestList();

            Assert.AreEqual(obj, marsh.Unmarshal<CollectionsType>(marsh.Marshal(obj)));
        }

        /**
         * <summary>Check write of object fields through reflective serializer.</summary>
         */
        [Test]
        public void TestObjectReflective()
        {
            ICollection<PortableTypeConfiguration> typeCfgs = 
                new List<PortableTypeConfiguration>();

            typeCfgs.Add(new PortableTypeConfiguration(typeof(OuterObjectType)));
            typeCfgs.Add(new PortableTypeConfiguration(typeof(InnerObjectType)));

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            CheckObject(marsh, new OuterObjectType(), new InnerObjectType());
        }

        /**
         * <summary>Test handles.</summary>
         */
        [Test]
        public void TestHandles()
        {
            ICollection<PortableTypeConfiguration> typeCfgs =
                new List<PortableTypeConfiguration>();

            typeCfgs.Add(new PortableTypeConfiguration(typeof(HandleInner)));
            typeCfgs.Add(new PortableTypeConfiguration(typeof(HandleOuter)));

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            PortableMarshaller marsh = new PortableMarshaller(cfg);

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

            IPortableObject outerObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.ForcePortable);

            HandleOuter newOuter = outerObj.Deserialize<HandleOuter>();
            HandleInner newInner = newOuter.Inner;

            CheckHandlesConsistency(outer, inner, newOuter, newInner);

            // Get inner object by field.
            IPortableObject innerObj = outerObj.Field<IPortableObject>("inner");

            newInner = innerObj.Deserialize<HandleInner>();
            newOuter = newInner.Outer;

            CheckHandlesConsistency(outer, inner, newOuter, newInner);

            // Get outer object from inner object by handle.
            outerObj = innerObj.Field<IPortableObject>("outer");

            newOuter = outerObj.Deserialize<HandleOuter>();
            newInner = newOuter.Inner;

            CheckHandlesConsistency(outer, inner, newOuter, newInner);
        }

        /**
         * <summary>Test handles with exclusive writes.</summary>
         */
        [Test]
        public void TestHandlesExclusive([Values(true, false)] bool detached, [Values(true, false)] bool asPortable)
        {
            var marsh = new PortableMarshaller(new PortableConfiguration
            {
                TypeConfigurations = new List<PortableTypeConfiguration>
                {
                    new PortableTypeConfiguration(typeof (HandleInner)),
                    new PortableTypeConfiguration(typeof (HandleOuterExclusive))
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

            var bytes = asPortable
                ? marsh.Marshal(new PortablesImpl(marsh).ToPortable<IPortableObject>(outer))
                : marsh.Marshal(outer);

            IPortableObject outerObj;

            if (detached)
            {
                var reader = new PortableReaderImpl(marsh, new Dictionary<long, IPortableTypeDescriptor>(),
                    new PortableHeapStream(bytes), PortableMode.ForcePortable, null);

                reader.DetachNext();

                outerObj = reader.Deserialize<IPortableObject>();
            }
            else
                outerObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.ForcePortable);

            HandleOuter newOuter = outerObj.Deserialize<HandleOuter>();

            Assert.IsFalse(newOuter == newOuter.Inner.Outer);
            Assert.IsFalse(newOuter == newOuter.Inner.RawOuter);
            Assert.IsFalse(newOuter == newOuter.RawInner.RawOuter);
            Assert.IsFalse(newOuter == newOuter.RawInner.RawOuter);

            Assert.IsFalse(newOuter.Inner == newOuter.RawInner);

            Assert.IsTrue(newOuter.Inner.Outer == newOuter.Inner.RawOuter);
            Assert.IsTrue(newOuter.RawInner.Outer == newOuter.RawInner.RawOuter);

            Assert.IsTrue(newOuter.Inner == newOuter.Inner.Outer.Inner);
            Assert.IsTrue(newOuter.Inner == newOuter.Inner.Outer.RawInner);
            Assert.IsTrue(newOuter.RawInner == newOuter.RawInner.Outer.Inner);
            Assert.IsTrue(newOuter.RawInner == newOuter.RawInner.Outer.RawInner);
        }

        ///
        /// <summary>Test KeepSerialized property</summary>
        ///
        [Test]
        public void TestKeepSerializedDefault()
        {
            CheckKeepSerialized(new PortableConfiguration(), true);
        }

        ///
        /// <summary>Test KeepSerialized property</summary>
        ///
        [Test]
        public void TestKeepSerializedDefaultFalse()
        {
            PortableConfiguration cfg = new PortableConfiguration();

            cfg.DefaultKeepDeserialized = false;

            CheckKeepSerialized(cfg, false);
        }

        ///
        /// <summary>Test KeepSerialized property</summary>
        ///
        [Test]
        public void TestKeepSerializedTypeCfgFalse()
        {
            PortableTypeConfiguration typeCfg = new PortableTypeConfiguration(typeof(PropertyType));

            typeCfg.KeepDeserialized = false;

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = new List<PortableTypeConfiguration> { typeCfg };

            CheckKeepSerialized(cfg, false);
        }

        ///
        /// <summary>Test KeepSerialized property</summary>
        ///
        [Test]
        public void TestKeepSerializedTypeCfgTrue()
        {
            PortableTypeConfiguration typeCfg = new PortableTypeConfiguration(typeof(PropertyType));
            typeCfg.KeepDeserialized = true;

            PortableConfiguration cfg = new PortableConfiguration();
            cfg.DefaultKeepDeserialized = false;

            cfg.TypeConfigurations = new List<PortableTypeConfiguration> { typeCfg };

            CheckKeepSerialized(cfg, true);
        }

        /// <summary>
        /// Test correct serialization/deserialization of arrays of special types.
        /// </summary>
        [Test]
        public void TestSpecialArrays()
        {
            ICollection<PortableTypeConfiguration> typeCfgs =
                new List<PortableTypeConfiguration>();

            typeCfgs.Add(new PortableTypeConfiguration(typeof(SpecialArray)));
            typeCfgs.Add(new PortableTypeConfiguration(typeof(SpecialArrayMarshalAware)));

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            Guid[] guidArr = { Guid.NewGuid() };
            Guid?[] nGuidArr = { Guid.NewGuid() };
            DateTime[] dateArr = { DateTime.Now.ToUniversalTime() };
            DateTime?[] nDateArr = { DateTime.Now.ToUniversalTime() };

            // Use special object.
            SpecialArray obj1 = new SpecialArray();

            obj1.GuidArr = guidArr;
            obj1.NGuidArr = nGuidArr;
            obj1.DateArr = dateArr;
            obj1.NDateArr = nDateArr;

            byte[] bytes = marsh.Marshal(obj1);

            IPortableObject portObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.ForcePortable);

            Assert.AreEqual(guidArr, portObj.Field<Guid[]>("guidArr"));
            Assert.AreEqual(nGuidArr, portObj.Field<Guid?[]>("nGuidArr"));
            Assert.AreEqual(dateArr, portObj.Field<DateTime[]>("dateArr"));
            Assert.AreEqual(nDateArr, portObj.Field<DateTime?[]>("nDateArr"));

            obj1 = portObj.Deserialize<SpecialArray>();

            Assert.AreEqual(guidArr, obj1.GuidArr);
            Assert.AreEqual(nGuidArr, obj1.NGuidArr);
            Assert.AreEqual(dateArr, obj1.DateArr);
            Assert.AreEqual(nDateArr, obj1.NDateArr);

            // Use special with IGridPortableMarshalAware.
            SpecialArrayMarshalAware obj2 = new SpecialArrayMarshalAware();

            obj2.GuidArr = guidArr;
            obj2.NGuidArr = nGuidArr;
            obj2.DateArr = dateArr;
            obj2.NDateArr = nDateArr;

            bytes = marsh.Marshal(obj2);

            portObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.ForcePortable);

            Assert.AreEqual(guidArr, portObj.Field<Guid[]>("a"));
            Assert.AreEqual(nGuidArr, portObj.Field<Guid?[]>("b"));
            Assert.AreEqual(dateArr, portObj.Field<DateTime[]>("c"));
            Assert.AreEqual(nDateArr, portObj.Field<DateTime?[]>("d"));

            obj2 = portObj.Deserialize<SpecialArrayMarshalAware>();

            Assert.AreEqual(guidArr, obj2.GuidArr);
            Assert.AreEqual(nGuidArr, obj2.NGuidArr);
            Assert.AreEqual(dateArr, obj2.DateArr);
            Assert.AreEqual(nDateArr, obj2.NDateArr);
        }

        private static void CheckKeepSerialized(PortableConfiguration cfg, bool expKeep)
        {
            if (cfg.TypeConfigurations == null)
            {
                cfg.TypeConfigurations = new List<PortableTypeConfiguration>
                {
                    new PortableTypeConfiguration(typeof(PropertyType))
                };
            }

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            byte[] data = marsh.Marshal(new PropertyType());

            IPortableObject portNewObj = marsh.Unmarshal<IPortableObject>(data, PortableMode.ForcePortable);

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

        private static void CheckObject(PortableMarshaller marsh, OuterObjectType outObj, InnerObjectType inObj)
        {
            inObj.PInt1 = 1;
            inObj.PInt2 = 2;

            outObj.InObj = inObj;

            byte[] bytes = marsh.Marshal(outObj);

            IPortableObject portOutObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.ForcePortable);

            Assert.AreEqual(outObj.GetHashCode(), portOutObj.GetHashCode());

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

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                if (obj != null && obj is CollectionsType)
                {
                    CollectionsType that = (CollectionsType)obj;

                    return CompareCollections(Col1, that.Col1) && CompareCollections(Col2, that.Col2);
                }
                return false;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                int res = Col1 != null ? Col1.GetHashCode() : 0;

                res = 31 * res + (Col2 != null ? Col2.GetHashCode() : 0);

                return res;
            }

            /** <inheritdoc /> */
            public override string ToString()
            {
                return "CollectoinsType[col1=" + CollectionAsString(Col1) + 
                    ", col2=" + CollectionAsString(Col2) + ']'; 
            }
        }

        private static string CollectionAsString(ICollection col)
        {
            if (col == null)
                return null;
            StringBuilder sb = new StringBuilder("[");

            bool first = true;

            foreach (object elem in col)
            {
                if (first)
                    first = false;
                else
                    sb.Append(", ");

                sb.Append(elem);
            }

            sb.Append("]");

            return sb.ToString();
        }

        public class TestList : ArrayList
        {

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

        public class SpecialArrayMarshalAware : SpecialArray, IPortableMarshalAware
        {
            public void WritePortable(IPortableWriter writer)
            {
                writer.WriteObjectArray("a", GuidArr);
                writer.WriteObjectArray("b", NGuidArr);
                writer.WriteObjectArray("c", DateArr);
                writer.WriteObjectArray("d", NDateArr);
            }

            public void ReadPortable(IPortableReader reader)
            {
                GuidArr = reader.ReadObjectArray<Guid>("a");
                NGuidArr = reader.ReadObjectArray<Guid?>("b");
                DateArr = reader.ReadObjectArray<DateTime>("c");
                NDateArr = reader.ReadObjectArray<DateTime?>("d");
            }
        }

        public class EnumType
        {
            public TestEnum PEnum { get; set; }

            public TestEnum[] PEnumArray { get; set; }
        }

        public class PrimitiveFieldType 
        {
            private Guid _pGuid;

            public bool PBool { get; set; }

            public sbyte PSbyte { get; set; }

            public byte PByte { get; set; }

            public short PShort { get; set; }

            public ushort PUshort { get; set; }

            public char PChar { get; set; }

            public int PInt { get; set; }

            public uint PUint { get; set; }

            public long PLong { get; set; }

            public ulong PUlong { get; set; }

            public float PFloat { get; set; }

            public double PDouble { get; set; }

            public string PString { get; set; }

            public Guid PGuid
            {
                get { return _pGuid; }
                set { _pGuid = value; }
            }

            public Guid? PnGuid { get; set; }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                if (obj != null && obj is PrimitiveFieldType)
                {
                    PrimitiveFieldType that = (PrimitiveFieldType)obj;

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
                        (PString == null && that.PString == null || PString != null && PString.Equals(that.PString)) &&
                        _pGuid.Equals(that._pGuid) &&
                        (PnGuid == null && that.PnGuid == null || PnGuid != null && PnGuid.Equals(that.PnGuid));
                }
                return false;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return PInt;
            }
        }

        public class PrimitiveFieldPortableType : PrimitiveFieldType, IPortableMarshalAware
        {
            public unsafe void WritePortable(IPortableWriter writer)
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
            }

            public unsafe void ReadPortable(IPortableReader reader)
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
                PGuid = reader.ReadGuid("guid").Value;
                PnGuid = reader.ReadGuid("nguid");
            }
        }

        public class PrimitiveFieldRawPortableType : PrimitiveFieldType, IPortableMarshalAware
        {
            public unsafe void WritePortable(IPortableWriter writer)
            {
                IPortableRawWriter rawWriter = writer.RawWriter();

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
            }

            public unsafe void ReadPortable(IPortableReader reader)
            {
                IPortableRawReader rawReader = reader.RawReader();

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
            }
        }

        public class PrimitiveFieldsSerializer : IPortableSerializer
        {
            public unsafe void WritePortable(object obj, IPortableWriter writer)
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
            }

            public unsafe void ReadPortable(object obj, IPortableReader reader)
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
                obj0.PGuid = reader.ReadGuid("guid").Value;
                obj0.PnGuid = reader.ReadGuid("nguid");
            }
        }

        public class PrimitiveFieldsRawSerializer : IPortableSerializer
        {
            public unsafe void WritePortable(object obj, IPortableWriter writer)
            {
                PrimitiveFieldType obj0 = (PrimitiveFieldType)obj;

                IPortableRawWriter rawWriter = writer.RawWriter();

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
            }

            public unsafe void ReadPortable(object obj, IPortableReader reader)
            {
                PrimitiveFieldType obj0 = (PrimitiveFieldType)obj;

                IPortableRawReader rawReader = reader.RawReader();

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
            }
        }

        public static string PrintBytes(byte[] bytes)
        {
            StringBuilder sb = new StringBuilder();

            foreach (byte b in bytes)
                sb.Append(b + " ");

            return sb.ToString();
        }

        public class HandleOuter : IPortableMarshalAware
        {
            public string Before;
            public HandleInner Inner;
            public string After;

            public string RawBefore;
            public HandleInner RawInner;
            public string RawAfter;

            /** <inheritdoc /> */
            virtual public void WritePortable(IPortableWriter writer)
            {
                writer.WriteString("before", Before);
                writer.WriteObject("inner", Inner);
                writer.WriteString("after", After);

                IPortableRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteString(RawBefore);
                rawWriter.WriteObject(RawInner);
                rawWriter.WriteString(RawAfter);
            }

            /** <inheritdoc /> */
            virtual public void ReadPortable(IPortableReader reader)
            {
                Before = reader.ReadString("before");
                Inner = reader.ReadObject<HandleInner>("inner");
                After = reader.ReadString("after");

                IPortableRawReader rawReader = reader.RawReader();

                RawBefore = rawReader.ReadString();
                RawInner = rawReader.ReadObject<HandleInner>();
                RawAfter = rawReader.ReadString();
            }
        }

        public class HandleInner : IPortableMarshalAware
        {
            public string Before;
            public HandleOuter Outer;
            public string After;

            public string RawBefore;
            public HandleOuter RawOuter;
            public string RawAfter;

            /** <inheritdoc /> */
            virtual public void WritePortable(IPortableWriter writer)
            {
                writer.WriteString("before", Before);
                writer.WriteObject("outer", Outer);
                writer.WriteString("after", After);

                IPortableRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteString(RawBefore);
                rawWriter.WriteObject(RawOuter);
                rawWriter.WriteString(RawAfter);
            }

            /** <inheritdoc /> */
            virtual public void ReadPortable(IPortableReader reader)
            {
                Before = reader.ReadString("before");
                Outer = reader.ReadObject<HandleOuter>("outer");
                After = reader.ReadString("after");

                IPortableRawReader rawReader = reader.RawReader();

                RawBefore = rawReader.ReadString();
                RawOuter = rawReader.ReadObject<HandleOuter>();
                RawAfter = rawReader.ReadString();
            }
        }


        public class HandleOuterExclusive : HandleOuter
        {
            /** <inheritdoc /> */
            override public void WritePortable(IPortableWriter writer)
            {
                PortableWriterImpl writer0 = (PortableWriterImpl)writer;

                writer.WriteString("before", Before);

                writer0.DetachNext();
                writer.WriteObject("inner", Inner);

                writer.WriteString("after", After);

                IPortableRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteString(RawBefore);

                writer0.DetachNext();
                rawWriter.WriteObject(RawInner);

                rawWriter.WriteString(RawAfter);
            }

            /** <inheritdoc /> */
            override public void ReadPortable(IPortableReader reader)
            {
                var reader0 = (PortableReaderImpl) reader;

                Before = reader0.ReadString("before");

                reader0.DetachNext();
                Inner = reader0.ReadObject<HandleInner>("inner");

                After = reader0.ReadString("after");

                var rawReader = (PortableReaderImpl) reader.RawReader();

                RawBefore = rawReader.ReadString();

                reader0.DetachNext();
                RawInner = rawReader.ReadObject<HandleInner>();

                RawAfter = rawReader.ReadString();
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

        public enum TestEnum
        {
            Val1, Val2, Val3 = 10
        }

        public class DecimalReflective
        {
            /** */
            public decimal Val;

            /** */
            public decimal[] ValArr;
        }

        public class DecimalMarshalAware : DecimalReflective, IPortableMarshalAware
        {
            /** */
            public decimal RawVal;

            /** */
            public decimal[] RawValArr;

            /** <inheritDoc /> */
            public void WritePortable(IPortableWriter writer)
            {
                writer.WriteDecimal("val", Val);
                writer.WriteDecimalArray("valArr", ValArr);

                IPortableRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteDecimal(RawVal);
                rawWriter.WriteDecimalArray(RawValArr);
            }

            /** <inheritDoc /> */
            public void ReadPortable(IPortableReader reader)
            {
                Val = reader.ReadDecimal("val");
                ValArr = reader.ReadDecimalArray("valArr");

                IPortableRawReader rawReader = reader.RawReader();

                RawVal = rawReader.ReadDecimal();
                RawValArr = rawReader.ReadDecimalArray();
            }
        }

        /// <summary>
        /// Date time type.
        /// </summary>
        public class DateTimeType : IPortableMarshalAware
        {
            public DateTime Loc;
            public DateTime Utc;

            public DateTime? LocNull;
            public DateTime? UtcNull;

            public DateTime?[] LocArr;
            public DateTime?[] UtcArr;

            public DateTime LocRaw;
            public DateTime UtcRaw;

            public DateTime? LocNullRaw;
            public DateTime? UtcNullRaw;

            public DateTime?[] LocArrRaw;
            public DateTime?[] UtcArrRaw;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="now">Current local time.</param>
            public DateTimeType(DateTime now)
            {
                Loc = now;
                Utc = now.ToUniversalTime();

                LocNull = Loc;
                UtcNull = Utc;

                LocArr = new DateTime?[] { Loc };
                UtcArr = new DateTime?[] { Utc };

                LocRaw = Loc;
                UtcRaw = Utc;

                LocNullRaw = LocNull;
                UtcNullRaw = UtcNull;

                LocArrRaw = new[] { LocArr[0] };
                UtcArrRaw = new[] { UtcArr[0] };
            }

            /** <inheritDoc /> */
            public void WritePortable(IPortableWriter writer)
            {
                writer.WriteDate("loc", Loc);
                writer.WriteDate("utc", Utc);
                writer.WriteDate("locNull", LocNull);
                writer.WriteDate("utcNull", UtcNull);
                writer.WriteDateArray("locArr", LocArr);
                writer.WriteDateArray("utcArr", UtcArr);

                IPortableRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteDate(LocRaw);
                rawWriter.WriteDate(UtcRaw);
                rawWriter.WriteDate(LocNullRaw);
                rawWriter.WriteDate(UtcNullRaw);
                rawWriter.WriteDateArray(LocArrRaw);
                rawWriter.WriteDateArray(UtcArrRaw);
            }

            /** <inheritDoc /> */
            public void ReadPortable(IPortableReader reader)
            {
                Loc = reader.ReadDate("loc", true).Value;
                Utc = reader.ReadDate("utc", false).Value;
                LocNull = reader.ReadDate("loc", true).Value;
                UtcNull = reader.ReadDate("utc", false).Value;
                LocArr = reader.ReadDateArray("locArr", true);
                UtcArr = reader.ReadDateArray("utcArr", false);

                IPortableRawReader rawReader = reader.RawReader();

                LocRaw = rawReader.ReadDate(true).Value;
                UtcRaw = rawReader.ReadDate(false).Value;
                LocNullRaw = rawReader.ReadDate(true).Value;
                UtcNullRaw = rawReader.ReadDate(false).Value;
                LocArrRaw = rawReader.ReadDateArray(true);
                UtcArrRaw = rawReader.ReadDateArray(false);
            }
        }
    }
}
