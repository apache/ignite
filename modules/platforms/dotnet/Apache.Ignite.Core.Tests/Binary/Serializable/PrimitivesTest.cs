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

// ReSharper disable StringLiteralTypo
// ReSharper disable IdentifierTypo
namespace Apache.Ignite.Core.Tests.Binary.Serializable
{
    using System;
    using System.Linq;
    using System.Runtime.Serialization;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests [Serializable] mechanism handling primitive types.
    /// </summary>
    public class PrimitivesTest
    {
        /** */
        private IIgnite _ignite;
        
        /// <summary>
        /// Sets up the test fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            _ignite = Ignition.Start(TestUtils.GetTestConfiguration());
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
        /// Tests the DateTime which is ISerializable struct.
        /// </summary>
        [Test]
        public void TestDateTime()
        {
            var marsh = GetMarshaller();

            var val = DateTime.Now;

            Assert.AreEqual(val, marsh.Unmarshal<DateTime>(marsh.Marshal(val)));

            Assert.AreEqual(new[] {val}, marsh.Unmarshal<DateTime[]>(marsh.Marshal(new[] {val})));

            Assert.AreEqual(new DateTime?[] {val, null},
                marsh.Unmarshal<DateTime?[]>(marsh.Marshal(new DateTime?[] {val, null})));
        }

        /// <summary>
        /// Tests that primitive types can be serialized with ISerializable mechanism.
        /// </summary>
        [Test]
        public void TestPrimitives()
        {
            var marsh = GetMarshaller();

            var val1 = new Primitives
            {
                Byte = 1,
                Bytes = new byte[] {2, 3, byte.MinValue, byte.MaxValue},
                Sbyte = -64,
                Sbytes = new sbyte[] {sbyte.MinValue, sbyte.MaxValue, 1, 2, -4, -5},
                Bool = true,
                Bools = new[] {true, true, false},
                Char = 'x',
                Chars = new[] {'a', 'z', char.MinValue, char.MaxValue},
                Short = -25,
                Shorts = new short[] {5, -7, 9, short.MinValue, short.MaxValue},
                Ushort = 99,
                Ushorts = new ushort[] {10, 20, 12, ushort.MinValue, ushort.MaxValue},
                Int = -456,
                Ints = new[] {-100, 200, -300, int.MinValue, int.MaxValue},
                Uint = 456,
                Uints = new uint[] {100, 200, 300, uint.MinValue, uint.MaxValue},
                Long = long.MaxValue,
                Longs = new[] {long.MinValue, long.MaxValue, 33, -44},
                Ulong = ulong.MaxValue,
                Ulongs = new ulong[] {ulong.MinValue, ulong.MaxValue, 33},
                Float = 1.33f,
                Floats = new[]
                {
                    float.MinValue, float.MaxValue,
                    float.Epsilon, float.NegativeInfinity, float.PositiveInfinity, float.NaN,
                    1.23f, -2.5f
                },
                Double = -6.78,
                Doubles = new[]
                {
                    double.MinValue, double.MaxValue, double.Epsilon,
                    double.NegativeInfinity, double.PositiveInfinity,
                    3.76, -9.89
                },
                Decimal = 1.23456789m,
                Decimals = new[]
                {
                    decimal.MinValue, decimal.MaxValue, decimal.One, decimal.MinusOne, decimal.Zero,
                    1.35m, -2.46m
                },
                DateTime = DateTime.UtcNow,
                DateTimes = new[] {DateTime.Now, DateTime.MinValue, DateTime.MaxValue, DateTime.UtcNow},
                Guid = Guid.NewGuid(),
                Guids = new[] {Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid()},
                String = "hello world",
                Strings = new[] {"hello", "world"},
                IntPtr = new IntPtr(12345),
                IntPtrs = new[] {IntPtr.Zero, new IntPtr(1), new IntPtr(-1), new IntPtr(long.MaxValue)},
                UIntPtr = new UIntPtr(1234567),
                UIntPtrs = new[] {UIntPtr.Zero, new UIntPtr(1), new UIntPtr(long.MaxValue), new UIntPtr(ulong.MaxValue)}
            };

            var vals = new[] {new Primitives(), val1};

            foreach (var val in vals)
            {
                Assert.IsFalse(val.GetObjectDataCalled);
                Assert.IsFalse(val.SerializationCtorCalled);

                // Unmarshal in full and binary form.
                var bytes = marsh.Marshal(val);
                var res = marsh.Unmarshal<Primitives>(bytes);
                var bin = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

                // Verify flags.
                Assert.IsTrue(val.GetObjectDataCalled);
                Assert.IsFalse(val.SerializationCtorCalled);

                Assert.IsFalse(res.GetObjectDataCalled);
                Assert.IsTrue(res.SerializationCtorCalled);

                // Verify values.
                Assert.AreEqual(val.Byte, res.Byte);
                Assert.AreEqual(val.Byte, bin.GetField<byte>("byte"));

                Assert.AreEqual(val.Bytes, res.Bytes);
                Assert.AreEqual(val.Bytes, bin.GetField<byte[]>("bytes"));

                Assert.AreEqual(val.Sbyte, res.Sbyte);
                Assert.AreEqual(val.Sbyte, bin.GetField<sbyte>("sbyte"));

                Assert.AreEqual(val.Sbytes, res.Sbytes);
                Assert.AreEqual(val.Sbytes, bin.GetField<sbyte[]>("sbytes"));

                Assert.AreEqual(val.Bool, res.Bool);
                Assert.AreEqual(val.Bool, bin.GetField<bool>("bool"));

                Assert.AreEqual(val.Bools, res.Bools);
                Assert.AreEqual(val.Bools, bin.GetField<bool[]>("bools"));

                Assert.AreEqual(val.Char, res.Char);
                Assert.AreEqual(val.Char, bin.GetField<char>("char"));

                Assert.AreEqual(val.Chars, res.Chars);
                Assert.AreEqual(val.Chars, bin.GetField<char[]>("chars"));

                Assert.AreEqual(val.Short, res.Short);
                Assert.AreEqual(val.Short, bin.GetField<short>("short"));

                Assert.AreEqual(val.Shorts, res.Shorts);
                Assert.AreEqual(val.Shorts, bin.GetField<short[]>("shorts"));

                Assert.AreEqual(val.Ushort, res.Ushort);
                Assert.AreEqual(val.Ushort, bin.GetField<ushort>("ushort"));

                Assert.AreEqual(val.Ushorts, res.Ushorts);
                Assert.AreEqual(val.Ushorts, bin.GetField<ushort[]>("ushorts"));

                Assert.AreEqual(val.Int, res.Int);
                Assert.AreEqual(val.Int, bin.GetField<int>("int"));

                Assert.AreEqual(val.Ints, res.Ints);
                Assert.AreEqual(val.Ints, bin.GetField<int[]>("ints"));

                Assert.AreEqual(val.Uint, res.Uint);
                Assert.AreEqual(val.Uint, bin.GetField<uint>("uint"));

                Assert.AreEqual(val.Uints, res.Uints);
                Assert.AreEqual(val.Uints, bin.GetField<uint[]>("uints"));

                Assert.AreEqual(val.Long, res.Long);
                Assert.AreEqual(val.Long, bin.GetField<long>("long"));

                Assert.AreEqual(val.Longs, res.Longs);
                Assert.AreEqual(val.Longs, bin.GetField<long[]>("longs"));

                Assert.AreEqual(val.Ulong, res.Ulong);
                Assert.AreEqual(val.Ulong, bin.GetField<ulong>("ulong"));

                Assert.AreEqual(val.Ulongs, res.Ulongs);
                Assert.AreEqual(val.Ulongs, bin.GetField<ulong[]>("ulongs"));

                Assert.AreEqual(val.Float, res.Float);
                Assert.AreEqual(val.Float, bin.GetField<float>("float"));

                Assert.AreEqual(val.Floats, res.Floats);
                Assert.AreEqual(val.Floats, bin.GetField<float[]>("floats"));

                Assert.AreEqual(val.Double, res.Double);
                Assert.AreEqual(val.Double, bin.GetField<double>("double"));

                Assert.AreEqual(val.Doubles, res.Doubles);
                Assert.AreEqual(val.Doubles, bin.GetField<double[]>("doubles"));

                Assert.AreEqual(val.Decimal, res.Decimal);
                Assert.AreEqual(val.Decimal, bin.GetField<decimal>("decimal"));

                Assert.AreEqual(val.Decimals, res.Decimals);
                Assert.AreEqual(val.Decimals, bin.GetField<decimal[]>("decimals"));

                Assert.AreEqual(val.Guid, res.Guid);
                Assert.AreEqual(val.Guid, bin.GetField<Guid>("guid"));

                Assert.AreEqual(val.Guids, res.Guids);
                Assert.AreEqual(val.Guids, bin.GetField<Guid[]>("guids"));

                Assert.AreEqual(val.DateTime, res.DateTime);
                Assert.AreEqual(val.DateTime, bin.GetField<IBinaryObject>("datetime").Deserialize<DateTime>());

                Assert.AreEqual(val.DateTimes, res.DateTimes);
                var dts = bin.GetField<IBinaryObject[]>("datetimes");
                Assert.AreEqual(val.DateTimes, dts == null ? null : dts.Select(x => x.Deserialize<DateTime>()));

                Assert.AreEqual(val.String, res.String);
                Assert.AreEqual(val.String, bin.GetField<string>("string"));

                Assert.AreEqual(val.Strings, res.Strings);
                Assert.AreEqual(val.Strings, bin.GetField<string[]>("strings"));

                Assert.AreEqual(val.IntPtr, res.IntPtr);
                Assert.AreEqual(val.IntPtr, bin.GetField<IntPtr>("intptr"));

                Assert.AreEqual(val.IntPtrs, res.IntPtrs);
                Assert.AreEqual(val.IntPtrs, bin.GetField<IntPtr[]>("intptrs"));

                Assert.AreEqual(val.UIntPtr, res.UIntPtr);
                Assert.AreEqual(val.UIntPtr, bin.GetField<UIntPtr>("uintptr"));

                Assert.AreEqual(val.UIntPtrs, res.UIntPtrs);
                Assert.AreEqual(val.UIntPtrs, bin.GetField<UIntPtr[]>("uintptrs"));

                VerifyFieldTypes(bin);
            }
        }

        /// <summary>
        /// Tests that primitive types in nullable form can be serialized with ISerializable mechanism.
        /// </summary>
        [Test]
        public void TestPrimitivesNullable()
        {
            var marsh = GetMarshaller();

            var val1 = new PrimitivesNullable
            {
                Byte = 1,
                Bytes = new byte?[] {2, 3, byte.MinValue, byte.MaxValue, null},
                Sbyte = -64,
                Sbytes = new sbyte?[] {sbyte.MinValue, sbyte.MaxValue, 1, 2, -4, -5, null},
                Bool = true,
                Bools = new bool?[] {true, true, false, null},
                Char = 'x',
                Chars = new char?[] {'a', 'z', char.MinValue, char.MaxValue, null},
                Short = -25,
                Shorts = new short?[] {5, -7, 9, short.MinValue, short.MaxValue, null},
                Ushort = 99,
                Ushorts = new ushort?[] {10, 20, 12, ushort.MinValue, ushort.MaxValue, null},
                Int = -456,
                Ints = new int?[] {-100, 200, -300, int.MinValue, int.MaxValue, null},
                Uint = 456,
                Uints = new uint?[] {100, 200, 300, uint.MinValue, uint.MaxValue, null},
                Long = long.MaxValue,
                Longs = new long?[] {long.MinValue, long.MaxValue, 33, -44, null},
                Ulong = ulong.MaxValue,
                Ulongs = new ulong?[] {ulong.MinValue, ulong.MaxValue, 33, null},
                Float = 1.33f,
                Floats = new float?[]
                {
                    float.MinValue, float.MaxValue,
                    float.Epsilon, float.NegativeInfinity, float.PositiveInfinity, float.NaN,
                    1.23f, -2.5f, null
                },
                Double = -6.78,
                Doubles = new double?[]
                {
                    double.MinValue, double.MaxValue, double.Epsilon,
                    double.NegativeInfinity, double.PositiveInfinity,
                    3.76, -9.89, null
                },
                Decimal = 1.23456789m,
                Decimals = new decimal?[]
                {
                    decimal.MinValue, decimal.MaxValue, decimal.One, decimal.MinusOne, decimal.Zero,
                    1.35m, -2.46m, null
                },
                DateTime = DateTime.UtcNow,
                DateTimes = new DateTime?[]
                {
                    DateTime.Now, DateTime.MinValue, DateTime.MaxValue, DateTime.UtcNow, null
                },
                Guid = Guid.NewGuid(),
                Guids = new Guid?[] {Guid.NewGuid(), Guid.NewGuid(), Guid.NewGuid(), null}
            };

            var vals = new[] {new PrimitivesNullable(), val1};

            foreach (var val in vals)
            {
                Assert.IsFalse(val.GetObjectDataCalled);
                Assert.IsFalse(val.SerializationCtorCalled);

                // Unmarshal in full and binary form.
                var bytes = marsh.Marshal(val);
                var res = marsh.Unmarshal<PrimitivesNullable>(bytes);
                var bin = marsh.Unmarshal<IBinaryObject>(bytes, BinaryMode.ForceBinary);

                // Verify flags.
                Assert.IsTrue(val.GetObjectDataCalled);
                Assert.IsFalse(val.SerializationCtorCalled);

                Assert.IsFalse(res.GetObjectDataCalled);
                Assert.IsTrue(res.SerializationCtorCalled);

                // Verify values.
                Assert.AreEqual(val.Byte, res.Byte);
                Assert.AreEqual(val.Byte, bin.GetField<byte?>("byte"));

                Assert.AreEqual(val.Bytes, res.Bytes);
                Assert.AreEqual(val.Bytes, bin.GetField<byte?[]>("bytes"));

                Assert.AreEqual(val.Sbyte, res.Sbyte);
                Assert.AreEqual(val.Sbyte, bin.GetField<sbyte?>("sbyte"));

                Assert.AreEqual(val.Sbytes, res.Sbytes);
                Assert.AreEqual(val.Sbytes, bin.GetField<sbyte?[]>("sbytes"));

                Assert.AreEqual(val.Bool, res.Bool);
                Assert.AreEqual(val.Bool, bin.GetField<bool?>("bool"));

                Assert.AreEqual(val.Bools, res.Bools);
                Assert.AreEqual(val.Bools, bin.GetField<bool?[]>("bools"));

                Assert.AreEqual(val.Char, res.Char);
                Assert.AreEqual(val.Char, bin.GetField<char?>("char"));

                Assert.AreEqual(val.Chars, res.Chars);
                Assert.AreEqual(val.Chars, bin.GetField<char?[]>("chars"));

                Assert.AreEqual(val.Short, res.Short);
                Assert.AreEqual(val.Short, bin.GetField<short?>("short"));

                Assert.AreEqual(val.Shorts, res.Shorts);
                Assert.AreEqual(val.Shorts, bin.GetField<short?[]>("shorts"));

                Assert.AreEqual(val.Ushort, res.Ushort);
                Assert.AreEqual(val.Ushort, bin.GetField<ushort?>("ushort"));

                Assert.AreEqual(val.Ushorts, res.Ushorts);
                Assert.AreEqual(val.Ushorts, bin.GetField<ushort?[]>("ushorts"));

                Assert.AreEqual(val.Int, res.Int);
                Assert.AreEqual(val.Int, bin.GetField<int?>("int"));

                Assert.AreEqual(val.Ints, res.Ints);
                Assert.AreEqual(val.Ints, bin.GetField<int?[]>("ints"));

                Assert.AreEqual(val.Uint, res.Uint);
                Assert.AreEqual(val.Uint, bin.GetField<uint?>("uint"));

                Assert.AreEqual(val.Uints, res.Uints);
                Assert.AreEqual(val.Uints, bin.GetField<uint?[]>("uints"));

                Assert.AreEqual(val.Long, res.Long);
                Assert.AreEqual(val.Long, bin.GetField<long?>("long"));

                Assert.AreEqual(val.Longs, res.Longs);
                Assert.AreEqual(val.Longs, bin.GetField<long?[]>("longs"));

                Assert.AreEqual(val.Ulong, res.Ulong);
                Assert.AreEqual(val.Ulong, bin.GetField<ulong?>("ulong"));

                Assert.AreEqual(val.Ulongs, res.Ulongs);
                Assert.AreEqual(val.Ulongs, bin.GetField<ulong?[]>("ulongs"));

                Assert.AreEqual(val.Float, res.Float);
                Assert.AreEqual(val.Float, bin.GetField<float?>("float"));

                Assert.AreEqual(val.Floats, res.Floats);
                Assert.AreEqual(val.Floats, bin.GetField<float?[]>("floats"));

                Assert.AreEqual(val.Double, res.Double);
                Assert.AreEqual(val.Double, bin.GetField<double?>("double"));

                Assert.AreEqual(val.Doubles, res.Doubles);
                Assert.AreEqual(val.Doubles, bin.GetField<double?[]>("doubles"));

                Assert.AreEqual(val.Decimal, res.Decimal);
                Assert.AreEqual(val.Decimal, bin.GetField<decimal?>("decimal"));

                Assert.AreEqual(val.Decimals, res.Decimals);
                Assert.AreEqual(val.Decimals, bin.GetField<decimal?[]>("decimals"));

                Assert.AreEqual(val.Guid, res.Guid);
                Assert.AreEqual(val.Guid, bin.GetField<Guid?>("guid"));

                Assert.AreEqual(val.Guids, res.Guids);
                Assert.AreEqual(val.Guids, bin.GetField<Guid?[]>("guids"));

                Assert.AreEqual(val.DateTime, res.DateTime);
                var dt = bin.GetField<IBinaryObject>("datetime");
                Assert.AreEqual(val.DateTime, dt == null ? null : dt.Deserialize<DateTime?>());

                Assert.AreEqual(val.DateTimes, res.DateTimes);
                var dts = bin.GetField<IBinaryObject[]>("datetimes");
                Assert.AreEqual(val.DateTimes, dts == null
                    ? null
                    : dts.Select(x => x == null ? null : x.Deserialize<DateTime?>()));
            }
        }

        /// <summary>
        /// Verifies the field types.
        /// </summary>
        private static void VerifyFieldTypes(IBinaryObject bin)
        {
            var binType = bin.GetBinaryType();
            
            Assert.AreEqual("byte", binType.GetFieldTypeName("byte"));
            Assert.AreEqual("byte", binType.GetFieldTypeName("sbyte"));
            
            Assert.AreEqual("byte[]", binType.GetFieldTypeName("bytes"));
            Assert.AreEqual("byte[]", binType.GetFieldTypeName("sbytes"));
            
            Assert.AreEqual("boolean", binType.GetFieldTypeName("bool"));
            Assert.AreEqual("boolean[]", binType.GetFieldTypeName("bools"));
            
            Assert.AreEqual("char", binType.GetFieldTypeName("char"));
            Assert.AreEqual("char[]", binType.GetFieldTypeName("chars"));

            Assert.AreEqual("short", binType.GetFieldTypeName("short"));
            Assert.AreEqual("short[]", binType.GetFieldTypeName("shorts"));

            Assert.AreEqual("short", binType.GetFieldTypeName("ushort"));
            Assert.AreEqual("short[]", binType.GetFieldTypeName("ushorts"));

            Assert.AreEqual("int", binType.GetFieldTypeName("int"));
            Assert.AreEqual("int[]", binType.GetFieldTypeName("ints"));

            Assert.AreEqual("int", binType.GetFieldTypeName("uint"));
            Assert.AreEqual("int[]", binType.GetFieldTypeName("uints"));

            Assert.AreEqual("long", binType.GetFieldTypeName("long"));
            Assert.AreEqual("long[]", binType.GetFieldTypeName("longs"));

            Assert.AreEqual("long", binType.GetFieldTypeName("ulong"));
            Assert.AreEqual("long[]", binType.GetFieldTypeName("ulongs"));

            Assert.AreEqual("float", binType.GetFieldTypeName("float"));
            Assert.AreEqual("float[]", binType.GetFieldTypeName("floats"));

            Assert.AreEqual("double", binType.GetFieldTypeName("double"));
            Assert.AreEqual("double[]", binType.GetFieldTypeName("doubles"));

            Assert.AreEqual("decimal", binType.GetFieldTypeName("decimal"));
            Assert.AreEqual("Object", binType.GetFieldTypeName("decimals"));

            Assert.AreEqual("UUID", binType.GetFieldTypeName("guid"));
            Assert.AreEqual("Object", binType.GetFieldTypeName("guids"));

            Assert.AreEqual("Object", binType.GetFieldTypeName("datetime"));
            Assert.AreEqual("Object", binType.GetFieldTypeName("datetimes"));

            Assert.AreEqual("Object", binType.GetFieldTypeName("intptr"));
            Assert.AreEqual("Object", binType.GetFieldTypeName("intptrs"));

            Assert.AreEqual("Object", binType.GetFieldTypeName("uintptr"));
            Assert.AreEqual("Object", binType.GetFieldTypeName("uintptrs"));
        }

        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        private Marshaller GetMarshaller()
        {
            return ((Ignite) _ignite).Marshaller;
        }

        [Serializable]
        public class Primitives : ISerializable
        {
            public bool GetObjectDataCalled { get; private set; }
            public bool SerializationCtorCalled { get; private set; }

            public byte Byte { get; set; }
            public byte[] Bytes { get; set; }
            public sbyte Sbyte { get; set; }
            public sbyte[] Sbytes { get; set; }
            public bool Bool { get; set; }
            public bool[] Bools { get; set; }
            public char Char { get; set; }
            public char[] Chars { get; set; }
            public short Short { get; set; }
            public short[] Shorts { get; set; }
            public ushort Ushort { get; set; }
            public ushort[] Ushorts { get; set; }
            public int Int { get; set; }
            public int[] Ints { get; set; }
            public uint Uint { get; set; }
            public uint[] Uints { get; set; }
            public long Long { get; set; }
            public long[] Longs { get; set; }
            public ulong Ulong { get; set; }
            public ulong[] Ulongs { get; set; }
            public float Float { get; set; }
            public float[] Floats { get; set; }
            public double Double { get; set; }
            public double[] Doubles { get; set; }
            public decimal Decimal { get; set; }
            public decimal[] Decimals { get; set; }
            public Guid Guid { get; set; }
            public Guid[] Guids { get; set; }
            public DateTime DateTime { get; set; }
            public DateTime[] DateTimes { get; set; }
            public string String { get; set; }
            public string[] Strings { get; set; }
            public IntPtr IntPtr { get; set; }
            public IntPtr[] IntPtrs { get; set; }
            public UIntPtr UIntPtr { get; set; }
            public UIntPtr[] UIntPtrs { get; set; }

            public Primitives()
            {
                // No-op.
            }

            protected Primitives(SerializationInfo info, StreamingContext context)
            {
                SerializationCtorCalled = true;

                Byte = info.GetByte("byte");
                Bytes = (byte[]) info.GetValue("bytes", typeof(byte[]));

                Sbyte = info.GetSByte("sbyte");
                Sbytes = (sbyte[]) info.GetValue("sbytes", typeof(sbyte[]));

                Bool = info.GetBoolean("bool");
                Bools = (bool[]) info.GetValue("bools", typeof(bool[]));

                Char = info.GetChar("char");
                Chars = (char[]) info.GetValue("chars", typeof(char[]));

                Short = info.GetInt16("short");
                Shorts = (short[]) info.GetValue("shorts", typeof(short[]));

                Ushort = info.GetUInt16("ushort");
                Ushorts = (ushort[]) info.GetValue("ushorts", typeof(ushort[]));

                Int = info.GetInt32("int");
                Ints = (int[]) info.GetValue("ints", typeof(int[]));

                Uint = info.GetUInt32("uint");
                Uints = (uint[]) info.GetValue("uints", typeof(uint[]));

                Long = info.GetInt64("long");
                Longs = (long[]) info.GetValue("longs", typeof(long[]));

                Ulong = info.GetUInt64("ulong");
                Ulongs = (ulong[]) info.GetValue("ulongs", typeof(ulong[]));

                Float = info.GetSingle("float");
                Floats = (float[]) info.GetValue("floats", typeof(float[]));

                Double = info.GetDouble("double");
                Doubles = (double[]) info.GetValue("doubles", typeof(double[]));

                Decimal = info.GetDecimal("decimal");
                Decimals = (decimal[]) info.GetValue("decimals", typeof(decimal[]));

                Guid = (Guid) info.GetValue("guid", typeof(Guid));
                Guids = (Guid[]) info.GetValue("guids", typeof(Guid[]));

                DateTime = info.GetDateTime("datetime");
                DateTimes = (DateTime[]) info.GetValue("datetimes", typeof(DateTime[]));

                String = info.GetString("string");
                Strings = (string[]) info.GetValue("strings", typeof(string[]));

                IntPtr = (IntPtr) info.GetInt64("intptr");
                IntPtrs = (IntPtr[]) info.GetValue("intptrs", typeof(IntPtr[]));

                UIntPtr = (UIntPtr) info.GetInt64("uintptr");
                UIntPtrs = (UIntPtr[]) info.GetValue("uintptrs", typeof(UIntPtr[]));
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                GetObjectDataCalled = true;

                info.AddValue("byte", Byte);
                info.AddValue("bytes", Bytes, typeof(byte[]));
                info.AddValue("sbyte", Sbyte);
                info.AddValue("sbytes", Sbytes, typeof(sbyte[]));
                info.AddValue("bool", Bool);
                info.AddValue("bools", Bools, typeof(bool[]));
                info.AddValue("char", Char);
                info.AddValue("chars", Chars, typeof(char[]));
                info.AddValue("short", Short);
                info.AddValue("shorts", Shorts, typeof(short[]));
                info.AddValue("ushort", Ushort);
                info.AddValue("ushorts", Ushorts, typeof(ushort[]));
                info.AddValue("int", Int);
                info.AddValue("ints", Ints, typeof(int[]));
                info.AddValue("uint", Uint);
                info.AddValue("uints", Uints, typeof(uint[]));
                info.AddValue("long", Long);
                info.AddValue("longs", Longs, typeof(long[]));
                info.AddValue("ulong", Ulong);
                info.AddValue("ulongs", Ulongs, typeof(ulong[]));
                info.AddValue("float", Float);
                info.AddValue("floats", Floats, typeof(float[]));
                info.AddValue("double", Double);
                info.AddValue("doubles", Doubles, typeof(double[]));
                info.AddValue("decimal", Decimal);
                info.AddValue("decimals", Decimals, typeof(decimal[]));
                info.AddValue("guid", Guid);
                info.AddValue("guids", Guids, typeof(Guid[]));
                info.AddValue("datetime", DateTime);
                info.AddValue("datetimes", DateTimes, typeof(DateTime[]));
                info.AddValue("string", String, typeof(string));
                info.AddValue("strings", Strings, typeof(string[]));
                info.AddValue("intptr", IntPtr);
                info.AddValue("intptrs", IntPtrs, typeof(IntPtr[]));
                info.AddValue("uintptr", UIntPtr);
                info.AddValue("uintptrs", UIntPtrs, typeof(UIntPtr[]));
            }
        }

        [Serializable]
        private class PrimitivesNullable : ISerializable
        {
            public bool GetObjectDataCalled { get; private set; }
            public bool SerializationCtorCalled { get; private set; }

            public byte? Byte { get; set; }
            public byte?[] Bytes { get; set; }
            public sbyte? Sbyte { get; set; }
            public sbyte?[] Sbytes { get; set; }
            public bool? Bool { get; set; }
            public bool?[] Bools { get; set; }
            public char? Char { get; set; }
            public char?[] Chars { get; set; }
            public short? Short { get; set; }
            public short?[] Shorts { get; set; }
            public ushort? Ushort { get; set; }
            public ushort?[] Ushorts { get; set; }
            public int? Int { get; set; }
            public int?[] Ints { get; set; }
            public uint? Uint { get; set; }
            public uint?[] Uints { get; set; }
            public long? Long { get; set; }
            public long?[] Longs { get; set; }
            public ulong? Ulong { get; set; }
            public ulong?[] Ulongs { get; set; }
            public float? Float { get; set; }
            public float?[] Floats { get; set; }
            public double? Double { get; set; }
            public double?[] Doubles { get; set; }
            public decimal? Decimal { get; set; }
            public decimal?[] Decimals { get; set; }
            public Guid? Guid { get; set; }
            public Guid?[] Guids { get; set; }
            public DateTime? DateTime { get; set; }
            public DateTime?[] DateTimes { get; set; }

            public PrimitivesNullable()
            {
                // No-op.
            }

            protected PrimitivesNullable(SerializationInfo info, StreamingContext context)
            {
                SerializationCtorCalled = true;

                Byte = (byte?) info.GetValue("byte", typeof(byte?));
                Bytes = (byte?[]) info.GetValue("bytes", typeof(byte?[]));

                Sbyte = (sbyte?) info.GetValue("sbyte", typeof(sbyte?));
                Sbytes = (sbyte?[]) info.GetValue("sbytes", typeof(sbyte?[]));

                Bool = (bool?) info.GetValue("bool", typeof(bool?));
                Bools = (bool?[]) info.GetValue("bools", typeof(bool?[]));

                Char = (char?) info.GetValue("char", typeof(char?));
                Chars = (char?[]) info.GetValue("chars", typeof(char?[]));

                Short = (short?) info.GetValue("short", typeof(short?));
                Shorts = (short?[]) info.GetValue("shorts", typeof(short?[]));

                Ushort = (ushort?) info.GetValue("ushort", typeof(ushort?));
                Ushorts = (ushort?[]) info.GetValue("ushorts", typeof(ushort?[]));

                Int = (int?) info.GetValue("int", typeof(int?));
                Ints = (int?[]) info.GetValue("ints", typeof(int?[]));

                Uint = (uint?) info.GetValue("uint", typeof(uint?));
                Uints = (uint?[]) info.GetValue("uints", typeof(uint?[]));

                Long = (long?) info.GetValue("long", typeof(long?));
                Longs = (long?[]) info.GetValue("longs", typeof(long?[]));

                Ulong = (ulong?) info.GetValue("ulong", typeof(ulong?));
                Ulongs = (ulong?[]) info.GetValue("ulongs", typeof(ulong?[]));

                Float = (float?) info.GetValue("float", typeof(float?));
                Floats = (float?[]) info.GetValue("floats", typeof(float?[]));

                Double = (double?) info.GetValue("double", typeof(double?));
                Doubles = (double?[]) info.GetValue("doubles", typeof(double?[]));

                Decimal = (decimal?) info.GetValue("decimal", typeof(decimal?));
                Decimals = (decimal?[]) info.GetValue("decimals", typeof(decimal?[]));

                Guid = (Guid?) info.GetValue("guid", typeof(Guid?));
                Guids = (Guid?[]) info.GetValue("guids", typeof(Guid?[]));

                DateTime = (DateTime?) info.GetValue("datetime", typeof(DateTime?));
                DateTimes = (DateTime?[]) info.GetValue("datetimes", typeof(DateTime?[]));
            }

            public void GetObjectData(SerializationInfo info, StreamingContext context)
            {
                GetObjectDataCalled = true;

                info.AddValue("byte", Byte, typeof(object));
                info.AddValue("bytes", Bytes, typeof(object));
                info.AddValue("sbyte", Sbyte, typeof(object));
                info.AddValue("sbytes", Sbytes, typeof(object));
                info.AddValue("bool", Bool, typeof(object));
                info.AddValue("bools", Bools, typeof(object));
                info.AddValue("char", Char, typeof(object));
                info.AddValue("chars", Chars, typeof(object));
                info.AddValue("short", Short, typeof(object));
                info.AddValue("shorts", Shorts, typeof(object));
                info.AddValue("ushort", Ushort, typeof(object));
                info.AddValue("ushorts", Ushorts, typeof(object));
                info.AddValue("int", Int, typeof(object));
                info.AddValue("ints", Ints, typeof(object));
                info.AddValue("uint", Uint, typeof(object));
                info.AddValue("uints", Uints, typeof(object));
                info.AddValue("long", Long, typeof(object));
                info.AddValue("longs", Longs, typeof(object));
                info.AddValue("ulong", Ulong, typeof(object));
                info.AddValue("ulongs", Ulongs, typeof(object));
                info.AddValue("float", Float, typeof(object));
                info.AddValue("floats", Floats, typeof(object));
                info.AddValue("double", Double, typeof(object));
                info.AddValue("doubles", Doubles, typeof(object));
                info.AddValue("decimal", Decimal, typeof(object));
                info.AddValue("decimals", Decimals, typeof(object));
                info.AddValue("guid", Guid, typeof(object));
                info.AddValue("guids", Guids, typeof(object));
                info.AddValue("datetime", DateTime, typeof(object));
                info.AddValue("datetimes", DateTimes, typeof(object));
            }
        }
    }
}
