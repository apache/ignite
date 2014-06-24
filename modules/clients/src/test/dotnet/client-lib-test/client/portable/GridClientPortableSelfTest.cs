// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable {
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Text;
    using NUnit.Framework;

    using GridGain.Client.Impl.Portable;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    [TestFixture]
    public class GridClientPortableSelfTest : GridClientAbstractTest {

        private GridClientPortableMarshaller marsh;
        
        [TestFixtureSetUp]
        override public void InitClient()
        {
            marsh = new GridClientPortableMarshaller(null);
        }

        [TestFixtureTearDown]
        override public void StopClient()
        {
           // No-op.
        }

        /**
         * <summary>Check write of primitive boolean.</summary>
         */
        public void TestWritePrimitiveBool()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(false)).Deserialize<bool>(), false);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(true)).Deserialize<bool>(), true);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((bool?)false)).Deserialize<bool?>(), false);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((bool?)null)), null);
        }

        /**
         * <summary>Check write of primitive boolean array.</summary>
         */
        public void TestWritePrimitiveBoolArray()
        {
            bool[] vals = new bool[] { true, false };

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<bool[]>(), vals);
        }

        /**
         * <summary>Check write of primitive sbyte.</summary>
         */
        public void TestWritePrimitiveSbyte()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((sbyte)1)).Deserialize<sbyte>(), (sbyte)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(SByte.MinValue)).Deserialize<sbyte>(), SByte.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(SByte.MaxValue)).Deserialize<sbyte>(), SByte.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((sbyte?)1)).Deserialize<sbyte?>(), (sbyte?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((sbyte?)null)), null);
        }

        /**
         * <summary>Check write of primitive sbyte array.</summary>
         */
        public void TestWritePrimitiveSbyteArray()
        {
            sbyte[] vals = new sbyte[] { SByte.MinValue, 0, 1, SByte.MaxValue };
            sbyte[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<sbyte[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive byte.</summary>
         */
        public void TestWritePrimitiveByte()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((byte)1)).Deserialize<byte>(), (byte)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Byte.MinValue)).Deserialize<byte>(), Byte.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Byte.MaxValue)).Deserialize<byte>(), Byte.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((byte?)1)).Deserialize<byte?>(), (byte?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((byte?)null)), null);
        }

        /**
         * <summary>Check write of primitive byte array.</summary>
         */
        public void TestWritePrimitiveByteArray()
        {
            byte[] vals = new byte[] { Byte.MinValue, 0, 1, Byte.MaxValue };
            byte[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<byte[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive short.</summary>
         */
        public void TestWritePrimitiveShort()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((short)1)).Deserialize<short>(), (short)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int16.MinValue)).Deserialize<short>(), Int16.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int16.MaxValue)).Deserialize<short>(), Int16.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((short?)1)).Deserialize<short?>(), (short?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((short?)null)), null);
        }

        /**
         * <summary>Check write of primitive short array.</summary>
         */
        public void TestWritePrimitiveShortArray()
        {
            short[] vals = new short[] { Int16.MinValue, 0, 1, Int16.MaxValue };
            short[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<short[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive ushort.</summary>
         */
        public void TestWritePrimitiveUshort()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ushort)1)).Deserialize<ushort>(), (ushort)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt16.MinValue)).Deserialize<ushort>(), UInt16.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt16.MaxValue)).Deserialize<ushort>(), UInt16.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ushort?)1)).Deserialize<ushort?>(), (ushort?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ushort?)null)), null);
        }

        /**
         * <summary>Check write of primitive short array.</summary>
         */
        public void TestWritePrimitiveUshortArray()
        {
            ushort[] vals = new ushort[] { UInt16.MinValue, 0, 1, UInt16.MaxValue };
            ushort[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<ushort[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive char.</summary>
         */
        public void TestWritePrimitiveChar()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((char)1)).Deserialize<char>(), (char)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Char.MinValue)).Deserialize<char>(), Char.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Char.MaxValue)).Deserialize<char>(), Char.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((char?)1)).Deserialize<char?>(), (char?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((char?)null)), null);
        }

        /**
         * <summary>Check write of primitive uint array.</summary>
         */
        public void TestWritePrimitiveCharArray()
        {
            char[] vals = new char[] { Char.MinValue, (char)0, (char)1, Char.MaxValue };
            char[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<char[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive int.</summary>
         */ 
        public void TestWritePrimitiveInt()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((int)1)).Deserialize<int>(), (int)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int32.MinValue)).Deserialize<int>(), Int32.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int32.MaxValue)).Deserialize<int>(), Int32.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((int?)1)).Deserialize<int?>(), (int?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((int?)null)), null);
        }

        /**
         * <summary>Check write of primitive uint array.</summary>
         */
        public void TestWritePrimitiveIntArray()
        {
            int[] vals = new int[] { Int32.MinValue, 0, 1, Int32.MaxValue };
            int[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<int[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive uint.</summary>
         */ 
        public void TestWritePrimitiveUint()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((uint)1)).Deserialize<uint>(), (uint)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt32.MinValue)).Deserialize<uint>(), UInt32.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt32.MaxValue)).Deserialize<uint>(), UInt32.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((uint?)1)).Deserialize<uint?>(), (int?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((uint?)null)), null);
        }

        /**
         * <summary>Check write of primitive uint array.</summary>
         */
        public void TestWritePrimitiveUintArray()
        {
            uint[] vals = new uint[] { UInt32.MinValue, 0, 1, UInt32.MaxValue };
            uint[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<uint[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive long.</summary>
         */
        public void TestWritePrimitiveLong()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((long)1)).Deserialize<long>(), (long)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int64.MinValue)).Deserialize<long>(), Int64.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int64.MaxValue)).Deserialize<long>(), Int64.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((long?)1)).Deserialize<long?>(), (long?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((long?)null)), null);
        }

        /**
         * <summary>Check write of primitive long array.</summary>
         */
        public void TestWritePrimitiveLongArray()
        {
            long[] vals = new long[] { Int64.MinValue, 0, 1, Int64.MaxValue };
            long[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<long[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive ulong.</summary>
         */
        public void TestWritePrimitiveUlong()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ulong)1)).Deserialize<ulong>(), (ulong)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt64.MinValue)).Deserialize<ulong>(), UInt64.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt64.MaxValue)).Deserialize<ulong>(), UInt64.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ulong?)1)).Deserialize<ulong?>(), (ulong?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ulong?)null)), null);
        }

        /**
         * <summary>Check write of primitive ulong array.</summary>
         */
        public void TestWritePrimitiveUlongArray()
        {
            ulong[] vals = new ulong[] { UInt64.MinValue, 0, 1, UInt64.MaxValue };
            ulong[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<ulong[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive float.</summary>
         */
        public void TestWritePrimitiveFloat()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((float)1)).Deserialize<float>(), (float)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(float.MinValue)).Deserialize<float>(), float.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(float.MaxValue)).Deserialize<float>(), float.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((float?)1)).Deserialize<float?>(), (float?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((float?)null)), null);
        }

        /**
         * <summary>Check write of primitive float array.</summary>
         */
        public void TestWritePrimitiveFloatArray()
        {
            float[] vals = new float[] { float.MinValue, 0, 1, float.MaxValue };
            float[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<float[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive double.</summary>
         */
        public void TestWritePrimitiveDouble()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((double)1)).Deserialize<double>(), (double)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(double.MinValue)).Deserialize<double>(), double.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(double.MaxValue)).Deserialize<double>(), double.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((double?)1)).Deserialize<double?>(), (double?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((double?)null)), null);
        }

        /**
         * <summary>Check write of primitive double array.</summary>
         */
        public void TestWritePrimitiveDoubleArray()
        {
            double[] vals = new double[] { double.MinValue, 0, 1, double.MaxValue };
            double[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<double[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of string.</summary>
         */
        public void TestWriteString()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal("str")).Deserialize<string>(), "str");
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(null)), null);
        }

        /**
         * <summary>Check write of string array.</summary>
         */
        public void TestWriteStringArray()
        {
            string[] vals = new string[] { "str1", null, "", "str2", null};
            string[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<string[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of Guid.</summary>
         */
        public void TestWriteGuid()
        {
            Guid guid = Guid.NewGuid();
            Guid? nGuid = (Guid?)guid;

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(guid)).Deserialize<Guid>(), guid);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(nGuid)).Deserialize<Guid?>(), nGuid);

            nGuid = null;

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(nGuid)), null);
        }

        /**
         * <summary>Check write of string array.</summary>
         */
        public void TestWriteGuidArray()
        {
            Guid?[] vals = new Guid?[] { Guid.NewGuid(), null, Guid.Empty, Guid.NewGuid(), null };
            Guid?[] newVals = marsh.Unmarshal(marsh.Marshal(vals)).Deserialize<Guid?[]>();

            Assert.AreEqual(vals, newVals);
        }

        /**
         * <summary>Check write of primitive fields through reflection.</summary>
         */
        public void TestPrimitiveFieldsReflective()
        {
            ICollection<GridClientPortableTypeConfiguration> typeCfgs = 
                new List<GridClientPortableTypeConfiguration>();

            typeCfgs.Add(new GridClientPortableTypeConfiguration(typeof(PrimitiveFieldType)));

            GridClientPortableConfiguration cfg = new GridClientPortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            GridClientPortableMarshaller marsh = new GridClientPortableMarshaller(cfg);

            PrimitiveFieldType obj = new PrimitiveFieldType();

            CheckPrimitiveFields(marsh, obj);
        }

        /**
         * <summary>Check write of primitive fields through portable interface.</summary>
         */
        public void TestPrimitiveFieldsPortable()
        {
            ICollection<GridClientPortableTypeConfiguration> typeCfgs = 
                new List<GridClientPortableTypeConfiguration>();

            typeCfgs.Add(new GridClientPortableTypeConfiguration(typeof(PrimitiveFieldPortableType)));

            GridClientPortableConfiguration cfg = new GridClientPortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            GridClientPortableMarshaller marsh = new GridClientPortableMarshaller(cfg);

            PrimitiveFieldPortableType obj = new PrimitiveFieldPortableType();

            CheckPrimitiveFields(marsh, obj);
        }

        /**
         * <summary>Check write of primitive fields through portable interface.</summary>
         */
        public void TestPrimitiveFieldsRawPortable()
        {
            ICollection<GridClientPortableTypeConfiguration> typeCfgs = 
                new List<GridClientPortableTypeConfiguration>();

            typeCfgs.Add(new GridClientPortableTypeConfiguration(typeof(PrimitiveFieldRawPortableType)));

            GridClientPortableConfiguration cfg = new GridClientPortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            GridClientPortableMarshaller marsh = new GridClientPortableMarshaller(cfg);

            PrimitiveFieldRawPortableType obj = new PrimitiveFieldRawPortableType();

            CheckPrimitiveFields(marsh, obj);
        }

        /**
         * <summary>Check write of primitive fields through portable interface.</summary>
         */
        public void TestPrimitiveFieldsSerializer()
        {
            ICollection<GridClientPortableTypeConfiguration> typeCfgs = 
                new List<GridClientPortableTypeConfiguration>();

            GridClientPortableTypeConfiguration typeCfg =
                new GridClientPortableTypeConfiguration(typeof(PrimitiveFieldType));

            typeCfg.Serializer = new PrimitiveFieldsSerializer();
            
            typeCfgs.Add(typeCfg);

            GridClientPortableConfiguration cfg = new GridClientPortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            GridClientPortableMarshaller marsh = new GridClientPortableMarshaller(cfg);

            PrimitiveFieldType obj = new PrimitiveFieldType();

            CheckPrimitiveFields(marsh, obj);
        }

        /**
         * <summary>Check write of primitive fields through raw serializer.</summary>
         */
        public void TestPrimitiveFieldsRawSerializer()
        {
            ICollection<GridClientPortableTypeConfiguration> typeCfgs = 
                new List<GridClientPortableTypeConfiguration>();

            GridClientPortableTypeConfiguration typeCfg =
                new GridClientPortableTypeConfiguration(typeof(PrimitiveFieldType));

            typeCfg.Serializer = new PrimitiveFieldsRawSerializer();

            typeCfgs.Add(typeCfg);

            GridClientPortableConfiguration cfg = new GridClientPortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            GridClientPortableMarshaller marsh = new GridClientPortableMarshaller(cfg);

            PrimitiveFieldType obj = new PrimitiveFieldType();

            CheckPrimitiveFields(marsh, obj);
        }

        private void CheckPrimitiveFields(GridClientPortableMarshaller marsh, PrimitiveFieldType obj)
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
            
            CheckPrimitiveFieldsSerialization(marsh, obj);

            obj.PString = "";

            CheckPrimitiveFieldsSerialization(marsh, obj);

            obj.PString = null;

            CheckPrimitiveFieldsSerialization(marsh, obj);

            obj.PNGuid = null;

            CheckPrimitiveFieldsSerialization(marsh, obj);
        }

        private void CheckPrimitiveFieldsSerialization(GridClientPortableMarshaller marsh, PrimitiveFieldType obj)
        {
            byte[] bytes = marsh.Marshal(obj);

            IGridClientPortableObject portObj = marsh.Unmarshal(bytes);

            Assert.AreEqual(obj.GetHashCode(), portObj.HashCode());

            PrimitiveFieldType newObj = portObj.Deserialize<PrimitiveFieldType>();

            Assert.AreEqual(obj, newObj);
        }

        /**
         * <summary>Check write of object with collections.</summary>
         */
        public void TestCollectionsReflective()
        {
            ICollection<GridClientPortableTypeConfiguration> typeCfgs =
                new List<GridClientPortableTypeConfiguration>();

            typeCfgs.Add(new GridClientPortableTypeConfiguration(typeof(CollectionsType)));
            typeCfgs.Add(new GridClientPortableTypeConfiguration(typeof(InnerObjectType)));

            GridClientPortableConfiguration cfg = new GridClientPortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            GridClientPortableMarshaller marsh = new GridClientPortableMarshaller(cfg);

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

            IGridClientPortableObject portObj = marsh.Unmarshal(bytes);

            Assert.AreEqual(obj.GetHashCode(), portObj.HashCode());

            CollectionsType newObj = portObj.Deserialize<CollectionsType>();

            Assert.AreEqual(obj, newObj);

            obj.Col1 = null;

            Assert.AreEqual(obj, marsh.Unmarshal(marsh.Marshal(obj)).Deserialize<CollectionsType>());

            obj.Col1 = list;
            obj.Col2 = list;

            Assert.AreEqual(obj, marsh.Unmarshal(marsh.Marshal(obj)).Deserialize<CollectionsType>());

            obj.Col2 = new TestList();

            Assert.AreEqual(obj, marsh.Unmarshal(marsh.Marshal(obj)).Deserialize<CollectionsType>());
        }

        /**
         * <summary>Check write of object fields through reflective serializer.</summary>
         */
        public void TestObjectReflective()
        {
            ICollection<GridClientPortableTypeConfiguration> typeCfgs = 
                new List<GridClientPortableTypeConfiguration>();

            typeCfgs.Add(new GridClientPortableTypeConfiguration(typeof(OuterObjectType)));
            typeCfgs.Add(new GridClientPortableTypeConfiguration(typeof(InnerObjectType)));

            GridClientPortableConfiguration cfg = new GridClientPortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            GridClientPortableMarshaller marsh = new GridClientPortableMarshaller(cfg);

            CheckObject(marsh, new OuterObjectType(), new InnerObjectType());
        }

        private void CheckObject(GridClientPortableMarshaller marsh, OuterObjectType outObj, InnerObjectType inObj)
        {
            inObj.PInt1 = 1;
            inObj.PInt2 = 2;

            outObj.InObj = inObj;

            byte[] bytes = marsh.Marshal(outObj);

            IGridClientPortableObject portOutObj = marsh.Unmarshal(bytes);

            Assert.AreEqual(outObj.GetHashCode(), portOutObj.HashCode());

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
                        pString == null && that.pString == null || pString != null && pString.Equals(that.pString) &&
                        pGuid.Equals(that.pGuid) &&
                        pNguid == null && that.pNguid == null || pNguid != null && pNguid.Equals(that.pNguid);
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
        
        public class PrimitiveFieldPortableType : PrimitiveFieldType, IGridClientPortable
        {
            public unsafe void WritePortable(IGridClientPortableWriter writer)
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
            }

            public unsafe void ReadPortable(IGridClientPortableReader reader)
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
            }
        }

        public class PrimitiveFieldRawPortableType : PrimitiveFieldType, IGridClientPortable
        {
            public unsafe void WritePortable(IGridClientPortableWriter writer)
            {
                IGridClientPortableRawWriter rawWriter = writer.RawWriter();

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
            }

            public unsafe void ReadPortable(IGridClientPortableReader reader)
            {
                IGridClientPortableRawReader rawReader = reader.RawReader();

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
            }
        }

        public class PrimitiveFieldsSerializer : IGridClientPortableSerializer
        {
            public unsafe void WritePortable(object obj, IGridClientPortableWriter writer)
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
            }

            public unsafe void ReadPortable(object obj, IGridClientPortableReader reader)
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
            }
        }

        public class PrimitiveFieldsRawSerializer : IGridClientPortableSerializer
        {
            public unsafe void WritePortable(object obj, IGridClientPortableWriter writer)
            {
                PrimitiveFieldType obj0 = (PrimitiveFieldType)obj;

                IGridClientPortableRawWriter rawWriter = writer.RawWriter();

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
            }

            public unsafe void ReadPortable(object obj, IGridClientPortableReader reader)
            {
                PrimitiveFieldType obj0 = (PrimitiveFieldType)obj;

                IGridClientPortableRawReader rawReader = reader.RawReader();

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
            }
        }

        public static string PrintBytes(byte[] bytes)
        {
            StringBuilder sb = new StringBuilder();

            foreach (byte b in bytes)
                sb.Append(b + " ");

            return sb.ToString();
        }
    }
}
