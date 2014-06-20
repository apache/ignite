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
    using System.Web.Script.Serialization;
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
            CheckStrict(marsh.Marshal(false, ctx), PU.TYPE_BOOL, PU.BYTE_ZERO);
            CheckStrict(marsh.Marshal(true, ctx), PU.TYPE_BOOL, PU.BYTE_ONE);

            CheckStrict(marsh.Marshal((bool?)false, ctx), PU.TYPE_BOOL, PU.BYTE_ZERO);
            CheckStrict(marsh.Marshal((bool?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive sbyte.</summary>
         */
        public void TestWritePrimitiveSbyte()
        {
            CheckStrict(marsh.Marshal((sbyte)1, ctx), PU.TYPE_BYTE, new byte[] { 0x01 });
            CheckStrict(marsh.Marshal(SByte.MinValue, ctx), PU.TYPE_BYTE, new byte[] { 0x80 });
            CheckStrict(marsh.Marshal(SByte.MaxValue, ctx), PU.TYPE_BYTE, new byte[] { 0x7f });

            CheckStrict(marsh.Marshal((sbyte?)1, ctx), PU.TYPE_BYTE, new byte[] { 0x01 });
            CheckStrict(marsh.Marshal((sbyte?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive byte.</summary>
         */
        public void TestWritePrimitiveByte()
        {
            CheckStrict(marsh.Marshal((byte)1, ctx), PU.TYPE_BYTE, new byte[] { 0x01});
            CheckStrict(marsh.Marshal(Byte.MinValue, ctx), PU.TYPE_BYTE, new byte[] { 0x00 });
            CheckStrict(marsh.Marshal(Byte.MaxValue, ctx), PU.TYPE_BYTE, new byte[] { 0xFF });

            CheckStrict(marsh.Marshal((byte?)1, ctx), PU.TYPE_BYTE, new byte[] { 0x01 });
            CheckStrict(marsh.Marshal((byte?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive short.</summary>
         */
        public void TestWritePrimitiveShort()
        {
            CheckStrict(marsh.Marshal((short)1, ctx), PU.TYPE_SHORT, new byte[] { 0x01, 0x00 });
            CheckStrict(marsh.Marshal(Int16.MinValue, ctx), PU.TYPE_SHORT, new byte[] { 0x00, 0x80 });
            CheckStrict(marsh.Marshal(Int16.MaxValue, ctx), PU.TYPE_SHORT, new byte[] { 0xFF, 0x7f });

            CheckStrict(marsh.Marshal((short?)1, ctx), PU.TYPE_SHORT, new byte[] { 0x01, 0x00 });
            CheckStrict(marsh.Marshal((short?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive ushort.</summary>
         */
        public void TestWritePrimitiveUshort()
        {
            CheckStrict(marsh.Marshal((ushort)1, ctx), PU.TYPE_SHORT, new byte[] { 0x01, 0x00 });
            CheckStrict(marsh.Marshal(UInt16.MinValue, ctx), PU.TYPE_SHORT, new byte[] { 0x00, 0x00 });
            CheckStrict(marsh.Marshal(UInt16.MaxValue, ctx), PU.TYPE_SHORT, new byte[] { 0xFF, 0xFF });

            CheckStrict(marsh.Marshal((ushort?)1, ctx), PU.TYPE_SHORT, new byte[] { 0x01, 0x00 });
            CheckStrict(marsh.Marshal((ushort?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive char.</summary>
         */
        public void TestWritePrimitiveChar()
        {
            CheckStrict(marsh.Marshal((char)1, ctx), PU.TYPE_CHAR, new byte[] { 0x01, 0x00 });
            CheckStrict(marsh.Marshal(Char.MinValue, ctx), PU.TYPE_CHAR, new byte[] { 0x00, 0x00 });
            CheckStrict(marsh.Marshal(Char.MaxValue, ctx), PU.TYPE_CHAR, new byte[] { 0xFF, 0xFF });

            CheckStrict(marsh.Marshal((char?)1, ctx), PU.TYPE_CHAR, new byte[] { 0x01, 0x00 });
            CheckStrict(marsh.Marshal((char?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive int.</summary>
         */ 
        public void TestWritePrimitiveInt()
        {
            CheckStrict(marsh.Marshal(1, ctx), PU.TYPE_INT, new byte[] { 0x01, 0x00, 0x00, 0x00 });
            CheckStrict(marsh.Marshal(Int32.MinValue, ctx), PU.TYPE_INT, new byte[] { 0x00, 0x00, 0x00, 0x80 });
            CheckStrict(marsh.Marshal(Int32.MaxValue, ctx), PU.TYPE_INT, new byte[] { 0xFF, 0xFF, 0xFF, 0x7f });

            CheckStrict(marsh.Marshal((int?)1, ctx), PU.TYPE_INT, new byte[] { 0x01, 0x00, 0x00, 0x00 });
            CheckStrict(marsh.Marshal((int?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive uint.</summary>
         */ 
        public void TestWritePrimitiveUint()
        {
            CheckStrict(marsh.Marshal((uint)1, ctx), PU.TYPE_INT, new byte[] { 0x01, 0x00, 0x00, 0x00 });
            CheckStrict(marsh.Marshal(UInt32.MinValue, ctx), PU.TYPE_INT, new byte[] { 0x00, 0x00, 0x00, 0x00 });
            CheckStrict(marsh.Marshal(UInt32.MaxValue, ctx), PU.TYPE_INT, new byte[] { 0xFF, 0xFF, 0xFF, 0xFF });

            CheckStrict(marsh.Marshal((uint?)1, ctx), PU.TYPE_INT, new byte[] { 0x01, 0x00, 0x00, 0x00 });
            CheckStrict(marsh.Marshal((uint?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive long.</summary>
         */
        public void TestWritePrimitiveLong()
        {
            CheckStrict(marsh.Marshal((long)1, ctx), PU.TYPE_LONG, 
                new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            CheckStrict(marsh.Marshal(Int64.MinValue, ctx), PU.TYPE_LONG, 
                new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x80 });
            CheckStrict(marsh.Marshal(Int64.MaxValue, ctx), PU.TYPE_LONG, 
                new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7f });

            CheckStrict(marsh.Marshal((long?)1, ctx), PU.TYPE_LONG,
                new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            CheckStrict(marsh.Marshal((long?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive ulong.</summary>
         */
        public void TestWritePrimitiveUlong()
        {
            CheckStrict(marsh.Marshal((ulong)1, ctx), PU.TYPE_LONG, 
                new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            CheckStrict(marsh.Marshal(UInt64.MinValue, ctx), PU.TYPE_LONG, 
                new byte[] { 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            CheckStrict(marsh.Marshal(UInt64.MaxValue, ctx), PU.TYPE_LONG, 
                new byte[] { 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF });

            CheckStrict(marsh.Marshal((ulong?)1, ctx), PU.TYPE_LONG, 
                new byte[] { 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00 });
            CheckStrict(marsh.Marshal((ulong?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive float.</summary>
         */
        public void TestWritePrimitiveFloat()
        {
            CheckStrict(marsh.Marshal((float)1, ctx), PU.TYPE_FLOAT, new byte[] { 0, 0, 128, 63 });
            CheckStrict(marsh.Marshal(float.MinValue, ctx), PU.TYPE_FLOAT, new byte[] { 255, 255, 127, 255 });
            CheckStrict(marsh.Marshal(float.MaxValue, ctx), PU.TYPE_FLOAT, new byte[] { 255, 255, 127, 127 });

            CheckStrict(marsh.Marshal((float?)1, ctx), PU.TYPE_FLOAT, new byte[] { 0, 0, 128, 63 });
            CheckStrict(marsh.Marshal((float?)null, ctx), PU.HDR_NULL);
        }

        /**
         * <summary>Check write of primitive double.</summary>
         */
        public void TestWritePrimitiveDouble()
        {
            CheckStrict(marsh.Marshal((double)1, ctx), PU.TYPE_DOUBLE,
                new byte[] { 0, 0, 0, 0, 0, 0, 240, 63 });
            CheckStrict(marsh.Marshal(double.MinValue, ctx), PU.TYPE_DOUBLE,
                new byte[] { 255, 255, 255, 255, 255, 255, 239, 255 });
            CheckStrict(marsh.Marshal(double.MaxValue, ctx), PU.TYPE_DOUBLE,
                new byte[] { 255, 255, 255, 255, 255, 255, 239, 127 });

            CheckStrict(marsh.Marshal((double?)1, ctx), PU.TYPE_DOUBLE,
                new byte[] { 0, 0, 0, 0, 0, 0, 240, 63 });
            CheckStrict(marsh.Marshal((double?)null, ctx), PU.HDR_NULL);
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
                else if (obj is int)
                    pos += (int)obj;
            }
        }
    }
}
