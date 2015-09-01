/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable 
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Text;    

    using GridGain.Portable;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Portable.IO;
    using NUnit.Framework;


    /// <summary>
    /// 
    /// </summary>
    [TestFixture]
    public class GridPortableSelfTest { 
        /** */
        private PortableMarshaller marsh;

        /// <summary>
        /// 
        /// </summary>
        [TestFixtureSetUp]
        public void BeforeTest()
        {
            GridTestUtils.KillProcesses();

            marsh = new PortableMarshaller(null);
        }
        
        /**
         * <summary>Check write of primitive boolean.</summary>
         */
        [Test]
        public void TestWritePrimitiveBool()
        {
            Assert.AreEqual(marsh.Unmarshal<bool>(marsh.Marshal(false)), false);
            Assert.AreEqual(marsh.Unmarshal<bool>(marsh.Marshal(true)), true);

            Assert.AreEqual(marsh.Unmarshal<bool?>(marsh.Marshal((bool?)false)), false);
            Assert.AreEqual(marsh.Unmarshal<bool?>(marsh.Marshal((bool?)null)), null);
        }

        /**
         * <summary>Check write of primitive boolean array.</summary>
         */
        [Test]
        public void TestWritePrimitiveBoolArray()
        {
            bool[] vals = new bool[] { true, false };

            Assert.AreEqual(marsh.Unmarshal<bool[]>(marsh.Marshal(vals)), vals);
        }

        /**
         * <summary>Check write of primitive sbyte.</summary>
         */
        [Test]
        public void TestWritePrimitiveSbyte()
        {
            Assert.AreEqual(marsh.Unmarshal<sbyte>(marsh.Marshal((sbyte)1)), (sbyte)1);
            Assert.AreEqual(marsh.Unmarshal<sbyte>(marsh.Marshal(SByte.MinValue)), SByte.MinValue);
            Assert.AreEqual(marsh.Unmarshal<sbyte>(marsh.Marshal(SByte.MaxValue)), SByte.MaxValue);

            Assert.AreEqual(marsh.Unmarshal<sbyte?>(marsh.Marshal((sbyte?)1)), (sbyte?)1);
            Assert.AreEqual(marsh.Unmarshal<sbyte?>(marsh.Marshal((sbyte?)null)), null);
        }

        /**
         * <summary>Check write of primitive sbyte array.</summary>
         */
        [Test]
        public void TestWritePrimitiveSbyteArray()
        {
            sbyte[] vals = new sbyte[] { SByte.MinValue, 0, 1, SByte.MaxValue };
            sbyte[] newVals = marsh.Unmarshal<sbyte[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive byte.</summary>
         */
        [Test]
        public void TestWritePrimitiveByte()
        {
            Assert.AreEqual(marsh.Unmarshal<byte>(marsh.Marshal((byte)1)), (byte)1);
            Assert.AreEqual(marsh.Unmarshal<byte>(marsh.Marshal(Byte.MinValue)), Byte.MinValue);
            Assert.AreEqual(marsh.Unmarshal<byte>(marsh.Marshal(Byte.MaxValue)), Byte.MaxValue);

            Assert.AreEqual(marsh.Unmarshal<byte?>(marsh.Marshal((byte?)1)), (byte?)1);
            Assert.AreEqual(marsh.Unmarshal<byte?>(marsh.Marshal((byte?)null)), null);
        }

        /**
         * <summary>Check write of primitive byte array.</summary>
         */
        [Test]
        public void TestWritePrimitiveByteArray()
        {
            byte[] vals = new byte[] { Byte.MinValue, 0, 1, Byte.MaxValue };
            byte[] newVals = marsh.Unmarshal<byte[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive short.</summary>
         */
        [Test]
        public void TestWritePrimitiveShort()
        {
            Assert.AreEqual(marsh.Unmarshal<short>(marsh.Marshal((short)1)), (short)1);
            Assert.AreEqual(marsh.Unmarshal<short>(marsh.Marshal(Int16.MinValue)), Int16.MinValue);
            Assert.AreEqual(marsh.Unmarshal<short>(marsh.Marshal(Int16.MaxValue)), Int16.MaxValue);

            Assert.AreEqual(marsh.Unmarshal<short?>(marsh.Marshal((short?)1)), (short?)1);
            Assert.AreEqual(marsh.Unmarshal<short?>(marsh.Marshal((short?)null)), null);
        }

        /**
         * <summary>Check write of primitive short array.</summary>
         */
        [Test]
        public void TestWritePrimitiveShortArray()
        {
            short[] vals = new short[] { Int16.MinValue, 0, 1, Int16.MaxValue };
            short[] newVals = marsh.Unmarshal<short[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive ushort.</summary>
         */
        [Test]
        public void TestWritePrimitiveUshort()
        {
            Assert.AreEqual(marsh.Unmarshal<ushort>(marsh.Marshal((ushort)1)), (ushort)1);
            Assert.AreEqual(marsh.Unmarshal<ushort>(marsh.Marshal(UInt16.MinValue)), UInt16.MinValue);
            Assert.AreEqual(marsh.Unmarshal<ushort>(marsh.Marshal(UInt16.MaxValue)), UInt16.MaxValue);

            Assert.AreEqual(marsh.Unmarshal<ushort?>(marsh.Marshal((ushort?)1)), (ushort?)1);
            Assert.AreEqual(marsh.Unmarshal<ushort?>(marsh.Marshal((ushort?)null)), null);
        }

        /**
         * <summary>Check write of primitive short array.</summary>
         */
        [Test]
        public void TestWritePrimitiveUshortArray()
        {
            ushort[] vals = new ushort[] { UInt16.MinValue, 0, 1, UInt16.MaxValue };
            ushort[] newVals = marsh.Unmarshal<ushort[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive char.</summary>
         */
        [Test]
        public void TestWritePrimitiveChar()
        {
            Assert.AreEqual(marsh.Unmarshal<char>(marsh.Marshal((char)1)), (char)1);
            Assert.AreEqual(marsh.Unmarshal<char>(marsh.Marshal(Char.MinValue)), Char.MinValue);
            Assert.AreEqual(marsh.Unmarshal<char>(marsh.Marshal(Char.MaxValue)), Char.MaxValue);

            Assert.AreEqual(marsh.Unmarshal<char?>(marsh.Marshal((char?)1)), (char?)1);
            Assert.AreEqual(marsh.Unmarshal<char?>(marsh.Marshal((char?)null)), null);
        }

        /**
         * <summary>Check write of primitive uint array.</summary>
         */
        [Test]
        public void TestWritePrimitiveCharArray()
        {
            char[] vals = new char[] { Char.MinValue, (char)0, (char)1, Char.MaxValue };
            char[] newVals = marsh.Unmarshal<char[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive int.</summary>
         */
        [Test]
        public void TestWritePrimitiveInt()
        {
            Assert.AreEqual(marsh.Unmarshal<int>(marsh.Marshal((int)1)), (int)1);
            Assert.AreEqual(marsh.Unmarshal<int>(marsh.Marshal(Int32.MinValue)), Int32.MinValue);
            Assert.AreEqual(marsh.Unmarshal<int>(marsh.Marshal(Int32.MaxValue)), Int32.MaxValue);

            Assert.AreEqual(marsh.Unmarshal<int?>(marsh.Marshal((int?)1)), (int?)1);
            Assert.AreEqual(marsh.Unmarshal<int?>(marsh.Marshal((int?)null)), null);
        }

        /**
         * <summary>Check write of primitive uint array.</summary>
         */
        [Test]
        public void TestWritePrimitiveIntArray()
        {
            int[] vals = new int[] { Int32.MinValue, 0, 1, Int32.MaxValue };
            int[] newVals = marsh.Unmarshal<int[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive uint.</summary>
         */
        [Test]
        public void TestWritePrimitiveUint()
        {
            Assert.AreEqual(marsh.Unmarshal<uint>(marsh.Marshal((uint)1)), (uint)1);
            Assert.AreEqual(marsh.Unmarshal<uint>(marsh.Marshal(UInt32.MinValue)), UInt32.MinValue);
            Assert.AreEqual(marsh.Unmarshal<uint>(marsh.Marshal(UInt32.MaxValue)), UInt32.MaxValue);

            Assert.AreEqual(marsh.Unmarshal<uint?>(marsh.Marshal((uint?)1)), (int?)1);
            Assert.AreEqual(marsh.Unmarshal<uint?>(marsh.Marshal((uint?)null)), null);
        }

        /**
         * <summary>Check write of primitive uint array.</summary>
         */
        [Test]
        public void TestWritePrimitiveUintArray()
        {
            uint[] vals = new uint[] { UInt32.MinValue, 0, 1, UInt32.MaxValue };
            uint[] newVals = marsh.Unmarshal<uint[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive long.</summary>
         */
        [Test]
        public void TestWritePrimitiveLong()
        {
            Assert.AreEqual(marsh.Unmarshal<long>(marsh.Marshal((long)1)), (long)1);
            Assert.AreEqual(marsh.Unmarshal<long>(marsh.Marshal(Int64.MinValue)), Int64.MinValue);
            Assert.AreEqual(marsh.Unmarshal<long>(marsh.Marshal(Int64.MaxValue)), Int64.MaxValue);

            Assert.AreEqual(marsh.Unmarshal<long?>(marsh.Marshal((long?)1)), (long?)1);
            Assert.AreEqual(marsh.Unmarshal<long?>(marsh.Marshal((long?)null)), null);
        }

        /**
         * <summary>Check write of primitive long array.</summary>
         */
        [Test]
        public void TestWritePrimitiveLongArray()
        {
            long[] vals = new long[] { Int64.MinValue, 0, 1, Int64.MaxValue };
            long[] newVals = marsh.Unmarshal<long[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive ulong.</summary>
         */
        [Test]
        public void TestWritePrimitiveUlong()
        {
            Assert.AreEqual(marsh.Unmarshal<ulong>(marsh.Marshal((ulong)1)), (ulong)1);
            Assert.AreEqual(marsh.Unmarshal<ulong>(marsh.Marshal(UInt64.MinValue)), UInt64.MinValue);
            Assert.AreEqual(marsh.Unmarshal<ulong>(marsh.Marshal(UInt64.MaxValue)), UInt64.MaxValue);

            Assert.AreEqual(marsh.Unmarshal<ulong?>(marsh.Marshal((ulong?)1)), (ulong?)1);
            Assert.AreEqual(marsh.Unmarshal<ulong?>(marsh.Marshal((ulong?)null)), null);
        }

        /**
         * <summary>Check write of primitive ulong array.</summary>
         */
        [Test]
        public void TestWritePrimitiveUlongArray()
        {
            ulong[] vals = new ulong[] { UInt64.MinValue, 0, 1, UInt64.MaxValue };
            ulong[] newVals = marsh.Unmarshal<ulong[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive float.</summary>
         */
        [Test]
        public void TestWritePrimitiveFloat()
        {
            Assert.AreEqual(marsh.Unmarshal<float>(marsh.Marshal((float)1)), (float)1);
            Assert.AreEqual(marsh.Unmarshal<float>(marsh.Marshal(float.MinValue)), float.MinValue);
            Assert.AreEqual(marsh.Unmarshal<float>(marsh.Marshal(float.MaxValue)), float.MaxValue);

            Assert.AreEqual(marsh.Unmarshal<float?>(marsh.Marshal((float?)1)), (float?)1);
            Assert.AreEqual(marsh.Unmarshal<float?>(marsh.Marshal((float?)null)), null);
        }

        /**
         * <summary>Check write of primitive float array.</summary>
         */
        [Test]
        public void TestWritePrimitiveFloatArray()
        {
            float[] vals = new float[] { float.MinValue, 0, 1, float.MaxValue };
            float[] newVals = marsh.Unmarshal<float[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive double.</summary>
         */
        [Test]
        public void TestWritePrimitiveDouble()
        {
            Assert.AreEqual(marsh.Unmarshal<double>(marsh.Marshal((double)1)), (double)1);
            Assert.AreEqual(marsh.Unmarshal<double>(marsh.Marshal(double.MinValue)), double.MinValue);
            Assert.AreEqual(marsh.Unmarshal<double>(marsh.Marshal(double.MaxValue)), double.MaxValue);

            Assert.AreEqual(marsh.Unmarshal<double?>(marsh.Marshal((double?)1)), (double?)1);
            Assert.AreEqual(marsh.Unmarshal<double?>(marsh.Marshal((double?)null)), null);
        }

        /**
         * <summary>Check write of primitive double array.</summary>
         */
        [Test]
        public void TestWritePrimitiveDoubleArray()
        {
            double[] vals = new double[] { double.MinValue, 0, 1, double.MaxValue };
            double[] newVals = marsh.Unmarshal<double[]>(marsh.Marshal(vals));

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
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Zero)), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = new decimal(1, 0, 0, false, 0))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = new decimal(1, 0, 0, true, 0))), val);

            // Test 32, 64 and 96 bits + mixed.
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = new decimal(0, 1, 0, false, 0))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = new decimal(0, 1, 0, true, 0))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = new decimal(0, 0, 1, false, 0))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = new decimal(0, 0, 1, true, 0))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = new decimal(1, 1, 1, false, 0))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = new decimal(1, 1, 1, true, 0))), val);

            // Test extremes.
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("65536"))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("-65536"))), val);

            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("4294967296"))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("-4294967296"))), val);

            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("281474976710656"))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("-281474976710656"))), val);

            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("18446744073709551616"))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("-18446744073709551616"))), val);

            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("1208925819614629174706176"))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("-1208925819614629174706176"))), val);

            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.MaxValue)), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.MinValue)), val);

            // Test scale.
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("11,12"))), val);
            Assert.AreEqual(marsh.Unmarshal<decimal>(marsh.Marshal(val = decimal.Parse("-11,12"))), val);

            // Test null.
            Assert.AreEqual(marsh.Unmarshal<decimal?>(marsh.Marshal(null)), null);
        }

        /**
         * <summary>Check write of decimal array.</summary>
         */
        [Test]
        public void TestWritePrimitiveDecimalArray()
        {
            decimal[] vals = new decimal[] { decimal.One, decimal.Parse("11,12") };
            decimal[] newVals = marsh.Unmarshal<decimal[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of string.</summary>
         */
        [Test]
        public void TestWriteString()
        {
            Assert.AreEqual(marsh.Unmarshal<string>(marsh.Marshal("str")), "str");
            Assert.AreEqual(marsh.Unmarshal<string>(marsh.Marshal(null)), null);
        }

        /**
         * <summary>Check write of string array.</summary>
         */
        [Test]
        public void TestWriteStringArray()
        {
            string[] vals = new string[] { "str1", null, "", "str2", null};
            string[] newVals = marsh.Unmarshal<string[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of Guid.</summary>
         */
        [Test]
        public void TestWriteGuid()
        {
            Guid guid = Guid.NewGuid();
            Guid? nGuid = (Guid?)guid;

            Assert.AreEqual(marsh.Unmarshal<Guid>(marsh.Marshal(guid)), guid);
            Assert.AreEqual(marsh.Unmarshal<Guid?>(marsh.Marshal(nGuid)), nGuid);

            nGuid = null;

            Assert.AreEqual(marsh.Unmarshal<Guid?>(marsh.Marshal(nGuid)), null);
        }

        /**
         * <summary>Check write of string array.</summary>
         */
        [Test]
        public void TestWriteGuidArray()
        {
            Guid?[] vals = new Guid?[] { Guid.NewGuid(), null, Guid.Empty, Guid.NewGuid(), null };
            Guid?[] newVals = marsh.Unmarshal<Guid?[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
        * <summary>Check write of enum.</summary>
        */
        [Test]
        public void TestWriteEnum()
        {
            TestEnum val = TestEnum.VAL1;

            Assert.AreEqual(marsh.Unmarshal<TestEnum>(marsh.Marshal(val)), val);
        }

        /**
        * <summary>Check write of enum.</summary>
        */
        [Test]
        public void TestWriteEnumArray()
        {
            TestEnum[] vals = new TestEnum[] { TestEnum.VAL2, TestEnum.VAL3 };
            TestEnum[] newVals = marsh.Unmarshal<TestEnum[]>(marsh.Marshal(vals));

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of date.</summary>
         */
        [Test]
        public void TestWriteDate() {
            DateTime time = System.DateTime.Now.ToUniversalTime();

            Assert.AreEqual(marsh.Unmarshal<DateTime>(marsh.Marshal(time)), time);
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

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            DateTime now = DateTime.Now;

            DateTimeType obj = new DateTimeType(now);

            DateTimeType otherObj = marsh.Unmarshal<DateTimeType>(marsh.Marshal(obj));

            Assert.AreEqual(obj.loc, otherObj.loc);
            Assert.AreEqual(obj.utc, otherObj.utc);
            Assert.AreEqual(obj.locNull, otherObj.locNull);
            Assert.AreEqual(obj.utcNull, otherObj.utcNull);            
            Assert.AreEqual(obj.locArr, otherObj.locArr);
            Assert.AreEqual(obj.utcArr, otherObj.utcArr);

            Assert.AreEqual(obj.locRaw, otherObj.locRaw);
            Assert.AreEqual(obj.utcRaw, otherObj.utcRaw);
            Assert.AreEqual(obj.locNullRaw, otherObj.locNullRaw);
            Assert.AreEqual(obj.utcNullRaw, otherObj.utcNullRaw);
            Assert.AreEqual(obj.locArrRaw, otherObj.locArrRaw);
            Assert.AreEqual(obj.utcArrRaw, otherObj.utcArrRaw);
        }

        /**
         * <summary>Check generic collections.</summary>
         */
        [Test]
        public void TestGenericCollections()
        {
            ICollection<string> list = new List<string>();

            list.Add("1");

            byte[] data = marsh.Marshal(list);

            ICollection<object> newObjList = marsh.Unmarshal<List<object>>(data);

            Assert.NotNull(newObjList);

            ICollection<string> newList = new List<string>();

            foreach (object obj in newObjList)
                newList.Add((string)obj);

            CompareCollections<string>(list, newList);
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

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            PropertyType obj = new PropertyType();

            obj.field1 = 1;
            obj.Field2 = 2;

            byte[] data = marsh.Marshal(obj);

            PropertyType newObj = marsh.Unmarshal<PropertyType>(data);

            Assert.AreEqual(obj.field1, newObj.field1);
            Assert.AreEqual(obj.Field2, newObj.Field2);

            IPortableObject portNewObj = marsh.Unmarshal<IPortableObject>(data, PortableMode.FORCE_PORTABLE);

            Assert.AreEqual(obj.field1, portNewObj.Field<int>("field1"));
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

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

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
            ICollection<PortableTypeConfiguration> typeCfgs = 
                new List<PortableTypeConfiguration>();

            PortableTypeConfiguration typeCfg =
                new PortableTypeConfiguration(typeof(PrimitiveFieldType));

            typeCfg.Serializer = new PrimitiveFieldsSerializer();
            
            typeCfgs.Add(typeCfg);

            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

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
            PortableConfiguration cfg = new PortableConfiguration();

            cfg.TypeConfigurations = new List<PortableTypeConfiguration> 
            { 
                new PortableTypeConfiguration(typeof(DecimalReflective)),
                new PortableTypeConfiguration(typeof(DecimalMarshalAware))
            }; ;

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            // 1. Test reflective stuff.
            DecimalReflective obj1 = new DecimalReflective();

            obj1.val = decimal.Zero;
            obj1.valArr = new decimal[] { decimal.One, decimal.MinusOne };

            IPortableObject portObj = marsh.Unmarshal<IPortableObject>(marsh.Marshal(obj1), PortableMode.FORCE_PORTABLE);

            Assert.AreEqual(obj1.val, portObj.Field<decimal>("val"));
            Assert.AreEqual(obj1.valArr, portObj.Field<decimal[]>("valArr"));

            Assert.AreEqual(obj1.val, portObj.Deserialize<DecimalReflective>().val);
            Assert.AreEqual(obj1.valArr, portObj.Deserialize<DecimalReflective>().valArr);

            // 2. Test marshal aware stuff.
            DecimalMarshalAware obj2 = new DecimalMarshalAware();

            obj2.val = decimal.Zero;
            obj2.valArr = new decimal[] { decimal.One, decimal.MinusOne };
            obj2.rawVal = decimal.MaxValue;
            obj2.rawValArr = new decimal[] { decimal.MinusOne, decimal.One} ;

            portObj = marsh.Unmarshal<IPortableObject>(marsh.Marshal(obj2), PortableMode.FORCE_PORTABLE);

            Assert.AreEqual(obj2.val, portObj.Field<decimal>("val"));
            Assert.AreEqual(obj2.valArr, portObj.Field<decimal[]>("valArr"));

            Assert.AreEqual(obj2.val, portObj.Deserialize<DecimalMarshalAware>().val);
            Assert.AreEqual(obj2.valArr, portObj.Deserialize<DecimalMarshalAware>().valArr);
            Assert.AreEqual(obj2.rawVal, portObj.Deserialize<DecimalMarshalAware>().rawVal);
            Assert.AreEqual(obj2.rawValArr, portObj.Deserialize<DecimalMarshalAware>().rawValArr);
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
            obj.PNGuid = Guid.NewGuid();
            
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

            IPortableObject portObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.FORCE_PORTABLE);

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
                PEnum = TestEnum.VAL1,
                PEnumArray = new[] {TestEnum.VAL2, TestEnum.VAL3}
            };

            byte[] bytes = marsh.Marshal(obj);

            IPortableObject portObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.FORCE_PORTABLE);

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
            list.Add((int)3);
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

            IPortableObject portObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.FORCE_PORTABLE);

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

            outer.before = "outBefore";
            outer.after = "outAfter";
            outer.rawBefore = "outRawBefore";
            outer.rawAfter = "outRawAfter";

            HandleInner inner = new HandleInner();

            inner.before = "inBefore";
            inner.after = "inAfter";
            inner.rawBefore = "inRawBefore";
            inner.rawAfter = "inRawAfter";

            outer.inner = inner;
            outer.rawInner = inner;

            inner.outer = outer;
            inner.rawOuter = outer;

            byte[] bytes = marsh.Marshal(outer);

            IPortableObject outerObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.FORCE_PORTABLE);

            HandleOuter newOuter = outerObj.Deserialize<HandleOuter>();
            HandleInner newInner = newOuter.inner;

            CheckHandlesConsistency(outer, inner, newOuter, newInner);

            // Get inner object by field.
            IPortableObject innerObj = outerObj.Field<IPortableObject>("inner");

            newInner = innerObj.Deserialize<HandleInner>();
            newOuter = newInner.outer;

            CheckHandlesConsistency(outer, inner, newOuter, newInner);

            // Get outer object from inner object by handle.
            outerObj = innerObj.Field<IPortableObject>("outer");

            newOuter = outerObj.Deserialize<HandleOuter>();
            newInner = newOuter.inner;

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
                before = "inBefore",
                after = "inAfter",
                rawBefore = "inRawBefore",
                rawAfter = "inRawAfter"
            };

            var outer = new HandleOuterExclusive
            {
                before = "outBefore",
                after = "outAfter",
                rawBefore = "outRawBefore",
                rawAfter = "outRawAfter",
                inner = inner,
                rawInner = inner
            };

            inner.outer = outer;
            inner.rawOuter = outer;

            var bytes = asPortable
                ? marsh.Marshal(new PortablesImpl(marsh).ToPortable<IPortableObject>(outer))
                : marsh.Marshal(outer);

            IPortableObject outerObj;

            if (detached)
            {
                var reader = new PortableReaderImpl(marsh, new Dictionary<long, IPortableTypeDescriptor>(),
                    new PortableHeapStream(bytes), PortableMode.FORCE_PORTABLE, null);

                reader.DetachNext();

                outerObj = reader.Deserialize<IPortableObject>();
            }
            else
                outerObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.FORCE_PORTABLE);

            HandleOuter newOuter = outerObj.Deserialize<HandleOuter>();

            Assert.IsFalse(newOuter == newOuter.inner.outer);
            Assert.IsFalse(newOuter == newOuter.inner.rawOuter);
            Assert.IsFalse(newOuter == newOuter.rawInner.rawOuter);
            Assert.IsFalse(newOuter == newOuter.rawInner.rawOuter);

            Assert.IsFalse(newOuter.inner == newOuter.rawInner);

            Assert.IsTrue(newOuter.inner.outer == newOuter.inner.rawOuter);
            Assert.IsTrue(newOuter.rawInner.outer == newOuter.rawInner.rawOuter);

            Assert.IsTrue(newOuter.inner == newOuter.inner.outer.inner);
            Assert.IsTrue(newOuter.inner == newOuter.inner.outer.rawInner);
            Assert.IsTrue(newOuter.rawInner == newOuter.rawInner.outer.inner);
            Assert.IsTrue(newOuter.rawInner == newOuter.rawInner.outer.rawInner);
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

            cfg.TypeConfigurations = new List<PortableTypeConfiguration>() { typeCfg };

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

            cfg.TypeConfigurations = new List<PortableTypeConfiguration>() { typeCfg };

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

            Guid[] guidArr = new Guid[] { Guid.NewGuid() };
            Guid?[] nGuidArr = new Guid?[] { Guid.NewGuid() };
            DateTime[] dateArr = new DateTime[] { DateTime.Now.ToUniversalTime() };
            DateTime?[] nDateArr = new DateTime?[] { DateTime.Now.ToUniversalTime() };

            // Use special object.
            SpecialArray obj1 = new SpecialArray();

            obj1.guidArr = guidArr;
            obj1.nGuidArr = nGuidArr;
            obj1.dateArr = dateArr;
            obj1.nDateArr = nDateArr;

            byte[] bytes = marsh.Marshal(obj1);

            IPortableObject portObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.FORCE_PORTABLE);

            Assert.AreEqual(guidArr, portObj.Field<Guid[]>("guidArr"));
            Assert.AreEqual(nGuidArr, portObj.Field<Guid?[]>("nGuidArr"));
            Assert.AreEqual(dateArr, portObj.Field<DateTime[]>("dateArr"));
            Assert.AreEqual(nDateArr, portObj.Field<DateTime?[]>("nDateArr"));

            obj1 = portObj.Deserialize<SpecialArray>();

            Assert.AreEqual(guidArr, obj1.guidArr);
            Assert.AreEqual(nGuidArr, obj1.nGuidArr);
            Assert.AreEqual(dateArr, obj1.dateArr);
            Assert.AreEqual(nDateArr, obj1.nDateArr);

            // Use special with IGridPortableMarshalAware.
            SpecialArrayMarshalAware obj2 = new SpecialArrayMarshalAware();

            obj2.guidArr = guidArr;
            obj2.nGuidArr = nGuidArr;
            obj2.dateArr = dateArr;
            obj2.nDateArr = nDateArr;

            bytes = marsh.Marshal(obj2);

            portObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.FORCE_PORTABLE);

            Assert.AreEqual(guidArr, portObj.Field<Guid[]>("a"));
            Assert.AreEqual(nGuidArr, portObj.Field<Guid?[]>("b"));
            Assert.AreEqual(dateArr, portObj.Field<DateTime[]>("c"));
            Assert.AreEqual(nDateArr, portObj.Field<DateTime?[]>("d"));

            obj2 = portObj.Deserialize<SpecialArrayMarshalAware>();

            Assert.AreEqual(guidArr, obj2.guidArr);
            Assert.AreEqual(nGuidArr, obj2.nGuidArr);
            Assert.AreEqual(dateArr, obj2.dateArr);
            Assert.AreEqual(nDateArr, obj2.nDateArr);
        }

        private static void CheckKeepSerialized(PortableConfiguration cfg, bool expKeep)
        {
            if (cfg.TypeConfigurations == null)
            {
                cfg.TypeConfigurations = new List<PortableTypeConfiguration>() 
                {
                    new PortableTypeConfiguration(typeof(PropertyType))
                };
            }

            PortableMarshaller marsh = new PortableMarshaller(cfg);

            byte[] data = marsh.Marshal(new PropertyType());

            IPortableObject portNewObj = marsh.Unmarshal<IPortableObject>(data, PortableMode.FORCE_PORTABLE);

            PropertyType deserialized1 = portNewObj.Deserialize<PropertyType>();
            PropertyType deserialized2 = portNewObj.Deserialize<PropertyType>();

            Assert.NotNull(deserialized1);

            Assert.AreEqual(expKeep, deserialized1 == deserialized2);
        }

        private void CheckHandlesConsistency(HandleOuter outer, HandleInner inner, HandleOuter newOuter, 
            HandleInner newInner)
        {
            Assert.True(newOuter != null);
            Assert.AreEqual(outer.before, newOuter.before);
            Assert.True(newOuter.inner == newInner);
            Assert.AreEqual(outer.after, newOuter.after);
            Assert.AreEqual(outer.rawBefore, newOuter.rawBefore);
            Assert.True(newOuter.rawInner == newInner);
            Assert.AreEqual(outer.rawAfter, newOuter.rawAfter);

            Assert.True(newInner != null);
            Assert.AreEqual(inner.before, newInner.before);
            Assert.True(newInner.outer == newOuter);
            Assert.AreEqual(inner.after, newInner.after);
            Assert.AreEqual(inner.rawBefore, newInner.rawBefore);
            Assert.True(newInner.rawOuter == newOuter);
            Assert.AreEqual(inner.rawAfter, newInner.rawAfter);            
        }

        private static void CheckObject(PortableMarshaller marsh, OuterObjectType outObj, InnerObjectType inObj)
        {
            inObj.PInt1 = 1;
            inObj.PInt2 = 2;

            outObj.InObj = inObj;

            byte[] bytes = marsh.Marshal(outObj);

            IPortableObject portOutObj = marsh.Unmarshal<IPortableObject>(bytes, PortableMode.FORCE_PORTABLE);

            Assert.AreEqual(outObj.GetHashCode(), portOutObj.GetHashCode());

            OuterObjectType newOutObj = portOutObj.Deserialize<OuterObjectType>();

            Assert.AreEqual(outObj, newOutObj);
        }

        public class OuterObjectType
        {
            private InnerObjectType inObj;

            public InnerObjectType InObj
            {
                get { return inObj; }
                set { inObj = value; }
            }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                if (obj != null && obj is OuterObjectType)
                {
                    OuterObjectType that = (OuterObjectType)obj;

                    return inObj == null ? that.inObj == null : inObj.Equals(that.inObj);
                }
                else
                    return false;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return inObj != null ? inObj.GetHashCode() : 0;
            }
        }

        public class InnerObjectType
        {
            private int pInt1;
            private int pInt2;

            public int PInt1
            {
                get { return pInt1; }
                set { pInt1 = value; }
            }

            public int PInt2
            {
                get { return pInt2; }
                set { pInt2 = value; }
            }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                if (obj != null && obj is InnerObjectType)
                {
                    InnerObjectType that = (InnerObjectType)obj;

                    return pInt1 == that.pInt1 && pInt2 == that.pInt2;
                }
                else
                    return false;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return 31 * pInt1 + pInt2;
            }

            /** <inheritdoc /> */
            public override string ToString()
            {
                return "InnerObjectType[pInt1=" + pInt1 + ", pInt2=" + pInt2 + ']';
            }
        }

        public class CollectionsType
        {
            private ICollection col1;

            private ArrayList col2;
            
            public ICollection Col1
            {
                get { return col1; }
                set { col1 = value; }
            }

            public ArrayList Col2
            {
                get { return col2; }
                set { col2 = value; }
            }
            
            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                if (obj != null && obj is CollectionsType)
                {
                    CollectionsType that = (CollectionsType)obj;

                    return CompareCollections(col1, that.col1) && CompareCollections(col2, that.col2);
                }
                else
                    return false;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                int res = col1 != null ? col1.GetHashCode() : 0;

                res = 31 * res + (col2 != null ? col2.GetHashCode() : 0);

                return res;
            }

            /** <inheritdoc /> */
            public override string ToString()
            {
                return "CollectoinsType[col1=" + CollectionAsString(col1) + 
                    ", col2=" + CollectionAsString(col2) + ']'; 
            }
        }

        private static string CollectionAsString(ICollection col)
        {
            if (col == null)
                return null;
            else
            {
                StringBuilder sb = new StringBuilder("[");

                bool first = true;

                foreach (Object elem in col)
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
        }

        public class TestList : ArrayList
        {

        }

        private static bool CompareCollections(ICollection col1, ICollection col2)
        {
            if (col1 == null && col2 == null)
                return true;
            else
            {
                if (col1 == null || col2 == null)
                    return false;
                else
                {
                    if (col1.Count != col2.Count)
                        return false;
                    else
                    {
                        IEnumerator enum1 = col1.GetEnumerator();

                        while (enum1.MoveNext())
                        {
                            object elem = enum1.Current;

                            bool contains = false;

                            foreach (object thatElem in col2)
                            {
                                if (elem == null && thatElem == null || elem != null && elem.Equals(thatElem))
                                {
                                    contains = true;

                                    break;
                                }
                            }

                            if (!contains)
                                return false;
                        }

                        return true;
                    }
                }
            }
        }

        private static bool CompareCollections<T>(ICollection<T> col1, ICollection<T> col2)
        {
            if (col1 == null && col2 == null)
                return true;
            else
            {
                if (col1 == null || col2 == null)
                    return false;
                else
                {
                    if (col1.Count != col2.Count)
                        return false;
                    else
                    {
                        IEnumerator enum1 = col1.GetEnumerator();

                        while (enum1.MoveNext())
                        {
                            object elem = enum1.Current;

                            bool contains = false;

                            foreach (object thatElem in col2)
                            {
                                if (elem == null && thatElem == null || elem != null && elem.Equals(thatElem))
                                {
                                    contains = true;

                                    break;
                                }
                            }

                            if (!contains)
                                return false;
                        }

                        return true;
                    }
                }
            }
        }

        public class PrimitiveArrayFieldType
        {
            private bool[] pBool;
            private sbyte[] pSbyte;
            private byte[] pByte;
            private short[] pShort;
            private ushort[] pUshort;
            private char[] pChar;
            private int[] pInt;
            private uint[] pUint;
            private long[] pLong;
            private ulong[] pUlong;
            private float[] pFloat;
            private double[] pDouble;
            private string[] pString;
            private Guid?[] pGuid;
            
            public bool[] PBool
            {
                get { return pBool; }
                set { pBool = value; }
            }

            public sbyte[] PSbyte
            {
                get { return pSbyte; }
                set { pSbyte = value; }
            }

            public byte[] PByte
            {
                get { return pByte; }
                set { pByte = value; }
            }

            public short[] PShort
            {
                get { return pShort; }
                set { pShort = value; }
            }

            public ushort[] PUshort
            {
                get { return pUshort; }
                set { pUshort = value; }
            }

            public char[] PChar
            {
                get { return pChar; }
                set { pChar = value; }
            }

            public int[] PInt
            {
                get { return pInt; }
                set { pInt = value; }
            }

            public uint[] PUint
            {
                get { return pUint; }
                set { pUint = value; }
            }

            public long[] PLong
            {
                get { return pLong; }
                set { pLong = value; }
            }

            public ulong[] PUlong
            {
                get { return pUlong; }
                set { pUlong = value; }
            }

            public float[] PFloat
            {
                get { return pFloat; }
                set { pFloat = value; }
            }

            public double[] PDouble
            {
                get { return pDouble; }
                set { pDouble = value; }
            }

            public string[] PString
            {
                get { return pString; }
                set { pString = value; }
            }

            public Guid?[] PGuid
            {
                get { return pGuid; }
                set { pGuid = value; }
            }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                if (obj != null && obj is PrimitiveArrayFieldType)
                {
                    PrimitiveArrayFieldType that = (PrimitiveArrayFieldType)obj;

                    return pBool == that.pBool &&
                        pByte == that.pByte &&
                        pSbyte == that.pSbyte &&
                        pShort == that.pShort &&
                        pUshort == that.pUshort &&
                        pInt == that.pInt &&
                        pUint == that.pUint &&
                        pLong == that.pLong &&
                        pUlong == that.pUlong &&
                        pChar == that.pChar &&
                        pFloat == that.pFloat &&
                        pDouble == that.pDouble &&
                        pString == that.pString &&
                        pGuid == that.pGuid;
                }
                else
                    return false;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return pInt != null && pInt.Length > 0 ? pInt[0].GetHashCode() : 0;
            }
        }

        public class SpecialArray
        {
            public Guid[] guidArr;
            public Guid?[] nGuidArr;
            public DateTime[] dateArr;
            public DateTime?[] nDateArr;
        }

        public class SpecialArrayMarshalAware : SpecialArray, IPortableMarshalAware
        {
            public void WritePortable(IPortableWriter writer)
            {
                writer.WriteObjectArray<Guid>("a", guidArr);
                writer.WriteObjectArray<Guid?>("b", nGuidArr);
                writer.WriteObjectArray<DateTime>("c", dateArr);
                writer.WriteObjectArray<DateTime?>("d", nDateArr);
            }

            public void ReadPortable(IPortableReader reader)
            {
                guidArr = reader.ReadObjectArray<Guid>("a");
                nGuidArr = reader.ReadObjectArray<Guid?>("b");
                dateArr = reader.ReadObjectArray<DateTime>("c");
                nDateArr = reader.ReadObjectArray<DateTime?>("d");
            }
        }

        public class EnumType
        {
            private TestEnum pEnum;
            private TestEnum[] pEnumArr;

            public TestEnum PEnum
            {
                get { return pEnum; }
                set { pEnum = value; }
            }

            public TestEnum[] PEnumArray
            {
                get { return pEnumArr; }
                set { pEnumArr = value; }
            }
        }

        public class PrimitiveFieldType 
        {
            private bool pBool;
            private sbyte pSbyte;
            private byte pByte;
            private short pShort;
            private ushort pUshort;
            private char pChar;
            private int pInt;
            private uint pUint;
            private long pLong;
            private ulong pUlong;
            private float pFloat;
            private double pDouble;
            private string pString;
            private Guid pGuid;
            private Guid? pNguid;
            
            public bool PBool
            {
                get { return pBool; }
                set { pBool = value; }
            }

            public sbyte PSbyte
            {
                get { return pSbyte; }
                set { pSbyte = value; }
            }

            public byte PByte
            {
                get { return pByte; }
                set { pByte = value; }
            }

            public short PShort
            {
                get { return pShort; }
                set { pShort = value; }
            }

            public ushort PUshort
            {
                get { return pUshort; }
                set { pUshort = value; }
            }

            public char PChar
            {
                get { return pChar; }
                set { pChar = value; }
            }

            public int PInt
            {
                get { return pInt; }
                set { pInt = value; }
            }

            public uint PUint
            {
                get { return pUint; }
                set { pUint = value; }
            }

            public long PLong
            {
                get { return pLong; }
                set { pLong = value; }
            }

            public ulong PUlong
            {
                get { return pUlong; }
                set { pUlong = value; }
            }

            public float PFloat
            {
                get { return pFloat; }
                set { pFloat = value; }
            }

            public double PDouble
            {
                get { return pDouble; }
                set { pDouble = value; }
            }

            public string PString
            {
                get { return pString; }
                set { pString = value; }
            }

            public Guid PGuid
            {
                get { return pGuid; }
                set { pGuid = value; }
            }

            public Guid? PNGuid
            {
                get { return pNguid; }
                set { pNguid = value; }
            }

            /** <inheritdoc /> */
            public override bool Equals(object obj)
            {
                if (this == obj)
                    return true;

                if (obj != null && obj is PrimitiveFieldType)
                {
                    PrimitiveFieldType that = (PrimitiveFieldType)obj;

                    return pBool == that.pBool &&
                        pByte == that.pByte &&
                        pSbyte == that.pSbyte &&
                        pShort == that.pShort &&
                        pUshort == that.pUshort &&
                        pInt == that.pInt &&
                        pUint == that.pUint &&
                        pLong == that.pLong &&
                        pUlong == that.pUlong &&
                        pChar == that.pChar &&
                        pFloat == that.pFloat &&
                        pDouble == that.pDouble &&
                        (pString == null && that.pString == null || pString != null && pString.Equals(that.pString)) &&
                        pGuid.Equals(that.pGuid) &&
                        (pNguid == null && that.pNguid == null || pNguid != null && pNguid.Equals(that.pNguid));
                }
                else
                    return false;
            }

            /** <inheritdoc /> */
            public override int GetHashCode()
            {
                return pInt;
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
                writer.WriteGuid("nguid", PNGuid);
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
                PNGuid = reader.ReadGuid("nguid");
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
                rawWriter.WriteGuid(PNGuid);
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
                PNGuid = rawReader.ReadGuid();
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
                writer.WriteGuid("nguid", obj0.PNGuid);
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
                obj0.PNGuid = reader.ReadGuid("nguid");
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
                rawWriter.WriteGuid(obj0.PNGuid);
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
                obj0.PNGuid = rawReader.ReadGuid();
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
            public string before;
            public HandleInner inner;
            public string after;

            public string rawBefore;
            public HandleInner rawInner;
            public string rawAfter;

            /** <inheritdoc /> */
            virtual public void WritePortable(IPortableWriter writer)
            {
                writer.WriteString("before", before);
                writer.WriteObject<HandleInner>("inner", inner);
                writer.WriteString("after", after);

                IPortableRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteString(rawBefore);
                rawWriter.WriteObject<HandleInner>(rawInner);
                rawWriter.WriteString(rawAfter);
            }

            /** <inheritdoc /> */
            virtual public void ReadPortable(IPortableReader reader)
            {
                before = reader.ReadString("before");
                inner = reader.ReadObject<HandleInner>("inner");
                after = reader.ReadString("after");

                IPortableRawReader rawReader = reader.RawReader();

                rawBefore = rawReader.ReadString();
                rawInner = rawReader.ReadObject<HandleInner>();
                rawAfter = rawReader.ReadString();
            }
        }

        public class HandleInner : IPortableMarshalAware
        {
            public string before;
            public HandleOuter outer;
            public string after;

            public string rawBefore;
            public HandleOuter rawOuter;
            public string rawAfter;

            /** <inheritdoc /> */
            virtual public void WritePortable(IPortableWriter writer)
            {
                writer.WriteString("before", before);
                writer.WriteObject<HandleOuter>("outer", outer);
                writer.WriteString("after", after);

                IPortableRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteString(rawBefore);
                rawWriter.WriteObject<HandleOuter>(rawOuter);
                rawWriter.WriteString(rawAfter);
            }

            /** <inheritdoc /> */
            virtual public void ReadPortable(IPortableReader reader)
            {
                before = reader.ReadString("before");
                outer = reader.ReadObject<HandleOuter>("outer");
                after = reader.ReadString("after");

                IPortableRawReader rawReader = reader.RawReader();

                rawBefore = rawReader.ReadString();
                rawOuter = rawReader.ReadObject<HandleOuter>();
                rawAfter = rawReader.ReadString();
            }
        }


        public class HandleOuterExclusive : HandleOuter
        {
            /** <inheritdoc /> */
            override public void WritePortable(IPortableWriter writer)
            {
                PortableWriterImpl writer0 = (PortableWriterImpl)writer;

                writer.WriteString("before", before);

                writer0.DetachNext();
                writer.WriteObject("inner", inner);

                writer.WriteString("after", after);

                IPortableRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteString(rawBefore);

                writer0.DetachNext();
                rawWriter.WriteObject(rawInner);

                rawWriter.WriteString(rawAfter);
            }

            /** <inheritdoc /> */
            override public void ReadPortable(IPortableReader reader)
            {
                var reader0 = (PortableReaderImpl) reader;

                before = reader0.ReadString("before");

                reader0.DetachNext();
                inner = reader0.ReadObject<HandleInner>("inner");

                after = reader0.ReadString("after");

                var rawReader = (PortableReaderImpl) reader.RawReader();

                rawBefore = rawReader.ReadString();

                reader0.DetachNext();
                rawInner = rawReader.ReadObject<HandleInner>();

                rawAfter = rawReader.ReadString();
            }
        }

        public class PropertyType
        {
            public int field1;

            public int Field2
            {
                get;
                set;
            }
        }

        public enum TestEnum
        {
            VAL1, VAL2, VAL3 = 10
        }

        public class DecimalReflective
        {
            /** */
            public decimal val;

            /** */
            public decimal[] valArr;
        }

        public class DecimalMarshalAware : DecimalReflective, IPortableMarshalAware
        {
            /** */
            public decimal rawVal;

            /** */
            public decimal[] rawValArr;

            /** <inheritDoc /> */
            public void WritePortable(IPortableWriter writer)
            {
                writer.WriteDecimal("val", val);
                writer.WriteDecimalArray("valArr", valArr);

                IPortableRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteDecimal(rawVal);
                rawWriter.WriteDecimalArray(rawValArr);
            }

            /** <inheritDoc /> */
            public void ReadPortable(IPortableReader reader)
            {
                val = reader.ReadDecimal("val");
                valArr = reader.ReadDecimalArray("valArr");

                IPortableRawReader rawReader = reader.RawReader();

                rawVal = rawReader.ReadDecimal();
                rawValArr = rawReader.ReadDecimalArray();
            }
        }

        /// <summary>
        /// Date time type.
        /// </summary>
        public class DateTimeType : IPortableMarshalAware
        {
            public DateTime loc;
            public DateTime utc;

            public DateTime? locNull;
            public DateTime? utcNull;

            public DateTime?[] locArr;
            public DateTime?[] utcArr;

            public DateTime locRaw;
            public DateTime utcRaw;

            public DateTime? locNullRaw;
            public DateTime? utcNullRaw;

            public DateTime?[] locArrRaw;
            public DateTime?[] utcArrRaw;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="now">Current local time.</param>
            public DateTimeType(DateTime now)
            {
                loc = now;
                utc = now.ToUniversalTime();

                locNull = loc;
                utcNull = utc;

                locArr = new DateTime?[] { loc };
                utcArr = new DateTime?[] { utc };

                locRaw = loc;
                utcRaw = utc;

                locNullRaw = locNull;
                utcNullRaw = utcNull;

                locArrRaw = new DateTime?[] { locArr[0] };
                utcArrRaw = new DateTime?[] { utcArr[0] };
            }

            /** <inheritDoc /> */
            public void WritePortable(IPortableWriter writer)
            {
                writer.WriteDate("loc", loc);
                writer.WriteDate("utc", utc);
                writer.WriteDate("locNull", locNull);
                writer.WriteDate("utcNull", utcNull);
                writer.WriteDateArray("locArr", locArr);
                writer.WriteDateArray("utcArr", utcArr);

                IPortableRawWriter rawWriter = writer.RawWriter();

                rawWriter.WriteDate(locRaw);
                rawWriter.WriteDate(utcRaw);
                rawWriter.WriteDate(locNullRaw);
                rawWriter.WriteDate(utcNullRaw);
                rawWriter.WriteDateArray(locArrRaw);
                rawWriter.WriteDateArray(utcArrRaw);
            }

            /** <inheritDoc /> */
            public void ReadPortable(IPortableReader reader)
            {
                loc = reader.ReadDate("loc", true).Value;
                utc = reader.ReadDate("utc", false).Value;
                locNull = reader.ReadDate("loc", true).Value;
                utcNull = reader.ReadDate("utc", false).Value;
                locArr = reader.ReadDateArray("locArr", true);
                utcArr = reader.ReadDateArray("utcArr", false);

                IPortableRawReader rawReader = reader.RawReader();

                locRaw = rawReader.ReadDate(true).Value;
                utcRaw = rawReader.ReadDate(false).Value;
                locNullRaw = rawReader.ReadDate(true).Value;
                utcNullRaw = rawReader.ReadDate(false).Value;
                locArrRaw = rawReader.ReadDateArray(true);
                utcArrRaw = rawReader.ReadDateArray(false);
            }
        }
    }
}
