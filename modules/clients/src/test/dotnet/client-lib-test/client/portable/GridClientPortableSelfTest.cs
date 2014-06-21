// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Portable {
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using System.IO;
    using System.Web.Script.Serialization;
    using System.Text;
    using NUnit.Framework;

    using GridGain.Client.Impl.Portable;
    using GridGain.Client.Portable;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    [TestFixture]
    public class GridClientPortableSelfTest : GridClientAbstractTest {

        private GridClientPortableMarshaller marsh;

        private GridClientPortableSerializationContext ctx = new GridClientPortableSerializationContext();

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
            CheckStrict(marsh.Marshal(false, ctx), 
                PU.HDR_FULL, 
                new BooleanValueAction(false), 
                new IntValueAction(PU.TYPE_BOOL),
                new IntValueAction(false.GetHashCode()),
                new IntValueAction(18 + 1),
                new IntValueAction(0),
                PU.BYTE_ZERO);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(false, ctx)).Deserialize<bool>(), false);

            CheckStrict(marsh.Marshal(true, ctx),
                PU.HDR_FULL,
                new BooleanValueAction(false),
                new IntValueAction(PU.TYPE_BOOL),
                new IntValueAction(true.GetHashCode()),
                new IntValueAction(18 + 1),
                new IntValueAction(0),
                PU.BYTE_ONE);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(true, ctx)).Deserialize<bool>(), true);

            CheckStrict(marsh.Marshal((bool?)false, ctx),
                PU.HDR_FULL,
                new BooleanValueAction(false),
                new IntValueAction(PU.TYPE_BOOL),
                new IntValueAction(false.GetHashCode()),
                new IntValueAction(18 + 1),
                new IntValueAction(0),
                PU.BYTE_ZERO);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((bool?)false, ctx)).Deserialize<bool?>(), false);

            CheckStrict(marsh.Marshal((bool?)null, ctx),
                PU.HDR_NULL);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((bool?)null, ctx)), null);
        }

        /**
         * <summary>Check write of primitive sbyte.</summary>
         */
        public void TestWritePrimitiveSbyte()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((sbyte)1, ctx)).Deserialize<sbyte>(), (sbyte)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(SByte.MinValue, ctx)).Deserialize<sbyte>(), SByte.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(SByte.MaxValue, ctx)).Deserialize<sbyte>(), SByte.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((sbyte?)1, ctx)).Deserialize<sbyte?>(), (sbyte?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((sbyte?)null, ctx)), null);

            //CheckStrict(marsh.Marshal((sbyte)1, ctx), PU.TYPE_BYTE, new byte[] { 0x01 });
            //CheckStrict(marsh.Marshal(SByte.MinValue, ctx), PU.TYPE_BYTE, new byte[] { 0x80 });
            //CheckStrict(marsh.Marshal(SByte.MaxValue, ctx), PU.TYPE_BYTE, new byte[] { 0x7f });

            //CheckStrict(marsh.Marshal((sbyte?)1, ctx), PU.TYPE_BYTE, new byte[] { 0x01 });
            //CheckStrict(marsh.Marshal((sbyte?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive byte.</summary>
         */
        public void TestWritePrimitiveByte()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((byte)1, ctx)).Deserialize<byte>(), (byte)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Byte.MinValue, ctx)).Deserialize<byte>(), Byte.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Byte.MaxValue, ctx)).Deserialize<byte>(), Byte.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((byte?)1, ctx)).Deserialize<byte?>(), (byte?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((byte?)null, ctx)), null);

            //CheckStrict(marsh.Marshal((byte)1, ctx), PU.TYPE_BYTE, new byte[] { 0x01});
            //CheckStrict(marsh.Marshal(Byte.MinValue, ctx), PU.TYPE_BYTE, new byte[] { 0x00 });
            //CheckStrict(marsh.Marshal(Byte.MaxValue, ctx), PU.TYPE_BYTE, new byte[] { 0xFF });

            //CheckStrict(marsh.Marshal((byte?)1, ctx), PU.TYPE_BYTE, new byte[] { 0x01 });
            //CheckStrict(marsh.Marshal((byte?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive short.</summary>
         */
        public void TestWritePrimitiveShort()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((short)1, ctx)).Deserialize<short>(), (short)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int16.MinValue, ctx)).Deserialize<short>(), Int16.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int16.MaxValue, ctx)).Deserialize<short>(), Int16.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((short?)1, ctx)).Deserialize<short?>(), (short?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((short?)null, ctx)), null);

            //CheckStrict(marsh.Marshal((short)1, ctx), PU.TYPE_SHORT, new byte[] { 0x01, 0x00 });
            //CheckStrict(marsh.Marshal(Int16.MinValue, ctx), PU.TYPE_SHORT, new byte[] { 0x00, 0x80 });
            //CheckStrict(marsh.Marshal(Int16.MaxValue, ctx), PU.TYPE_SHORT, new byte[] { 0xFF, 0x7f });

            //CheckStrict(marsh.Marshal((short?)1, ctx), PU.TYPE_SHORT, new byte[] { 0x01, 0x00 });
            //CheckStrict(marsh.Marshal((short?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive ushort.</summary>
         */
        public void TestWritePrimitiveUshort()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ushort)1, ctx)).Deserialize<ushort>(), (ushort)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt16.MinValue, ctx)).Deserialize<ushort>(), UInt16.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt16.MaxValue, ctx)).Deserialize<ushort>(), UInt16.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ushort?)1, ctx)).Deserialize<ushort?>(), (ushort?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ushort?)null, ctx)), null);

            //CheckStrict(marsh.Marshal((ushort)1, ctx), PU.TYPE_SHORT, new byte[] { 0x01, 0x00 });
            //CheckStrict(marsh.Marshal(UInt16.MinValue, ctx), PU.TYPE_SHORT, new byte[] { 0x00, 0x00 });
            //CheckStrict(marsh.Marshal(UInt16.MaxValue, ctx), PU.TYPE_SHORT, new byte[] { 0xFF, 0xFF });

            //CheckStrict(marsh.Marshal((ushort?)1, ctx), PU.TYPE_SHORT, new byte[] { 0x01, 0x00 });
            //CheckStrict(marsh.Marshal((ushort?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive char.</summary>
         */
        public void TestWritePrimitiveChar()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((char)1, ctx)).Deserialize<char>(), (char)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Char.MinValue, ctx)).Deserialize<char>(), Char.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Char.MaxValue, ctx)).Deserialize<char>(), Char.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((char?)1, ctx)).Deserialize<char?>(), (char?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((char?)null, ctx)), null);

            //CheckStrict(marsh.Marshal((char)1, ctx), PU.TYPE_CHAR, new byte[] { 0x01, 0x00 });
            //CheckStrict(marsh.Marshal(Char.MinValue, ctx), PU.TYPE_CHAR, new byte[] { 0x00, 0x00 });
            //CheckStrict(marsh.Marshal(Char.MaxValue, ctx), PU.TYPE_CHAR, new byte[] { 0xFF, 0xFF });

            //CheckStrict(marsh.Marshal((char?)1, ctx), PU.TYPE_CHAR, new byte[] { 0x01, 0x00 });
            //CheckStrict(marsh.Marshal((char?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive int.</summary>
         */ 
        public void TestWritePrimitiveInt()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((int)1, ctx)).Deserialize<int>(), (int)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int32.MinValue, ctx)).Deserialize<int>(), Int32.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int32.MaxValue, ctx)).Deserialize<int>(), Int32.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((int?)1, ctx)).Deserialize<int?>(), (int?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((int?)null, ctx)), null);

            //CheckStrict(marsh.Marshal(1, ctx), PU.TYPE_INT, new byte[] { 0x01, 0x00, 0x00, 0x00 });
            //CheckStrict(marsh.Marshal(Int32.MinValue, ctx), PU.TYPE_INT, new byte[] { 0x00, 0x00, 0x00, 0x80 });
            //CheckStrict(marsh.Marshal(Int32.MaxValue, ctx), PU.TYPE_INT, new byte[] { 0xFF, 0xFF, 0xFF, 0x7f });

            //CheckStrict(marsh.Marshal((int?)1, ctx), PU.TYPE_INT, new byte[] { 0x01, 0x00, 0x00, 0x00 });
            //CheckStrict(marsh.Marshal((int?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive uint.</summary>
         */ 
        public void TestWritePrimitiveUint()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((uint)1, ctx)).Deserialize<uint>(), (uint)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt32.MinValue, ctx)).Deserialize<uint>(), UInt32.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt32.MaxValue, ctx)).Deserialize<uint>(), UInt32.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((uint?)1, ctx)).Deserialize<uint?>(), (int?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((uint?)null, ctx)), null);

            //CheckStrict(marsh.Marshal((uint)1, ctx), PU.TYPE_INT, new byte[] { 0x01, 0x00, 0x00, 0x00 });
            //CheckStrict(marsh.Marshal(UInt32.MinValue, ctx), PU.TYPE_INT, new byte[] { 0x00, 0x00, 0x00, 0x00 });
            //CheckStrict(marsh.Marshal(UInt32.MaxValue, ctx), PU.TYPE_INT, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF });

            //CheckStrict(marsh.Marshal((uint?)1, ctx), PU.TYPE_INT, new byte[] { 0x01, 0x00, 0x00, 0x00 });
            //CheckStrict(marsh.Marshal((uint?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive long.</summary>
         */
        public void TestWritePrimitiveLong()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((long)1, ctx)).Deserialize<long>(), (long)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int64.MinValue, ctx)).Deserialize<long>(), Int64.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(Int64.MaxValue, ctx)).Deserialize<long>(), Int64.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((long?)1, ctx)).Deserialize<long?>(), (long?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((long?)null, ctx)), null);

            //CheckStrict(marsh.Marshal((long)1, ctx), PU.TYPE_LONG, 
            //    new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            //CheckStrict(marsh.Marshal(Int64.MinValue, ctx), PU.TYPE_LONG, 
            //    new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80 });
            //CheckStrict(marsh.Marshal(Int64.MaxValue, ctx), PU.TYPE_LONG, 
            //    new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7f });

            //CheckStrict(marsh.Marshal((long?)1, ctx), PU.TYPE_LONG,
            //    new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            //CheckStrict(marsh.Marshal((long?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive ulong.</summary>
         */
        public void TestWritePrimitiveUlong()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ulong)1, ctx)).Deserialize<ulong>(), (ulong)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt64.MinValue, ctx)).Deserialize<ulong>(), UInt64.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(UInt64.MaxValue, ctx)).Deserialize<ulong>(), UInt64.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ulong?)1, ctx)).Deserialize<ulong?>(), (ulong?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((ulong?)null, ctx)), null);

            //CheckStrict(marsh.Marshal((ulong)1, ctx), PU.TYPE_LONG, 
            //    new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            //CheckStrict(marsh.Marshal(UInt64.MinValue, ctx), PU.TYPE_LONG, 
            //    new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            //CheckStrict(marsh.Marshal(UInt64.MaxValue, ctx), PU.TYPE_LONG, 
            //    new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });

            //CheckStrict(marsh.Marshal((ulong?)1, ctx), PU.TYPE_LONG, 
            //    new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            //CheckStrict(marsh.Marshal((ulong?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive float.</summary>
         */
        public void TestWritePrimitiveFloat()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((float)1, ctx)).Deserialize<float>(), (float)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(float.MinValue, ctx)).Deserialize<float>(), float.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(float.MaxValue, ctx)).Deserialize<float>(), float.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((float?)1, ctx)).Deserialize<float?>(), (float?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((float?)null, ctx)), null);

            //CheckStrict(marsh.Marshal((float)1, ctx), PU.TYPE_FLOAT, new byte[] { 0, 0, 128, 63 });
            //CheckStrict(marsh.Marshal(float.MinValue, ctx), PU.TYPE_FLOAT, new byte[] { 255, 255, 127, 255 });
            //CheckStrict(marsh.Marshal(float.MaxValue, ctx), PU.TYPE_FLOAT, new byte[] { 255, 255, 127, 127 });

            //CheckStrict(marsh.Marshal((float?)1, ctx), PU.TYPE_FLOAT, new byte[] { 0, 0, 128, 63 });
            //CheckStrict(marsh.Marshal((float?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive double.</summary>
         */
        public void TestWritePrimitiveDouble()
        {
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((double)1, ctx)).Deserialize<double>(), (double)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(double.MinValue, ctx)).Deserialize<double>(), double.MinValue);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal(double.MaxValue, ctx)).Deserialize<double>(), double.MaxValue);

            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((double?)1, ctx)).Deserialize<double?>(), (double?)1);
            Assert.AreEqual(marsh.Unmarshal(marsh.Marshal((double?)null, ctx)), null);

            //CheckStrict(marsh.Marshal((double)1, ctx), PU.TYPE_DOUBLE,
            //    new byte[] { 0, 0, 0, 0, 0, 0, 240, 63 });
            //CheckStrict(marsh.Marshal(double.MinValue, ctx), PU.TYPE_DOUBLE,
            //    new byte[] { 255, 255, 255, 255, 255, 255, 239, 255 });
            //CheckStrict(marsh.Marshal(double.MaxValue, ctx), PU.TYPE_DOUBLE,
            //    new byte[] { 255, 255, 255, 255, 255, 255, 239, 127 });

            //CheckStrict(marsh.Marshal((double?)1, ctx), PU.TYPE_DOUBLE,
            //    new byte[] { 0, 0, 0, 0, 0, 0, 240, 63 });
            //CheckStrict(marsh.Marshal((double?)null, ctx), PU.HDR_NULL);
        }

        public void TestPrimitiveFields()
        {
            GridClientPortableTypeConfiguration typeCfg = 
                new GridClientPortableTypeConfiguration(typeof(PrimitiveFieldReflectiveType));

            ICollection<GridClientPortableTypeConfiguration> typeCfgs = new List<GridClientPortableTypeConfiguration>();

            typeCfgs.Add(typeCfg);

            GridClientPortableConfiguration cfg = new GridClientPortableConfiguration();

            cfg.TypeConfigurations = typeCfgs;

            GridClientPortableMarshaller marsh = new GridClientPortableMarshaller(cfg);

            PrimitiveFieldReflectiveType obj = new PrimitiveFieldReflectiveType();

            byte[] bytes = marsh.Marshal(obj, ctx);

            IGridClientPortableObject portObj = marsh.Unmarshal(bytes);

            PrimitiveFieldReflectiveType newObj = portObj.Deserialize<PrimitiveFieldReflectiveType>();

            return;
        }

        public static string PrintBytes(byte[] bytes)
        {
            StringBuilder sb = new StringBuilder();

            foreach (byte b in bytes)
                sb.Append(b + " ");

            return sb.ToString();
        }

        public class PrimitiveFieldReflectiveType 
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

            //private bool? rBool;
            //private sbyte? rSbyte;
            //private byte? rByte;
            //private short? rShort;
            //private ushort? rUshort;
            //private char? rChar;
            //private int? rInt;
            //private uint? rUint;
            //private long? rLong;
            //private ulong? rUlong;
            //private float? rFloat;
            //private double? rDouble;

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
                get { return PByte; }
                set { PByte = value; }
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

            //public bool? RBool
            //{
            //    get { return rBool; }
            //    set { rBool = value; }
            //}

            //public sbyte? RSbyte
            //{
            //    get { return rSbyte; }
            //    set { rSbyte = value; }
            //}

            //public byte? RByte
            //{
            //    get { return rByte; }
            //    set { rByte = value; }
            //}

            //public short? RShort
            //{
            //    get { return rShort; }
            //    set { rShort = value; }
            //}

            //public ushort? RUshort
            //{
            //    get { return rUshort; }
            //    set { rUshort = value; }
            //}

            //public char? RChar
            //{
            //    get { return rChar; }
            //    set { rChar = value; }
            //}

            //public int? RInt
            //{
            //    get { return rInt; }
            //    set { rInt = value; }
            //}

            //public uint? RUint
            //{
            //    get { return rUint; }
            //    set { rUint = value; }
            //}

            //public long? RLong
            //{
            //    get { return rLong; }
            //    set { rLong = value; }
            //}

            //public ulong? RUlong
            //{
            //    get { return rUlong; }
            //    set { rUlong = value; }
            //}

            //public float? RFloat
            //{
            //    get { return rFloat; }
            //    set { rFloat = value; }
            //}

            //public double? RDouble
            //{
            //    get { return rDouble; }
            //    set { rDouble = value; }
            //}
        }

        

        private void CheckStrict(byte[] data, params object[] objs)
        {
            int pos = 0;

            foreach (object obj in objs)
            {
                if (obj is byte)
                {
                    byte obj0 = (byte)obj;

                    Assert.AreEqual(data[pos], obj0, "Invalid byte [pos=" + pos + ']');

                    pos++;
                }
                else if (obj is byte[])
                {
                    byte[] obj0 = (byte[])obj;

                    for (int i = 0; i < obj0.Length; i++)
                    {
                        Assert.AreEqual(data[pos], obj0[i], "Invalid byte [pos=" + pos + ']');

                        pos++;
                    }
                }
                else if (obj is BooleanValueAction)
                {
                    byte[] arr = new byte[1];

                    Array.Copy(data, pos, arr, 0, 1);

                    Assert.AreEqual(PU.ReadBoolean(new MemoryStream(arr)), ((BooleanValueAction)obj).Value);

                    pos += 1;
                }
                else if (obj is IntValueAction)
                {
                    byte[] arr = new byte[4];

                    Array.Copy(data, pos, arr, 0, 4);

                    Assert.AreEqual(PU.ReadInt(new MemoryStream(arr)), ((IntValueAction)obj).Value);

                    pos += 4;
                }
                else if (obj is SkipAction)
                    pos += ((SkipAction)obj).Length;
            }
        }

        private class BooleanValueAction
        {
            public BooleanValueAction(bool val)
            {
                Value = val;
            }

            public bool Value
            {
                get;
                set;
            }
        }

        private class IntValueAction
        {
            public IntValueAction(int val)
            {
                Value = val;
            }

            public int Value
            {
                get;
                set;
            }
        }

        private class SkipAction
        {
            public SkipAction(int len)
            {
                Length = len;
            }

            public int Length
            {
                get;
                set;
            }
        }
    }
}
