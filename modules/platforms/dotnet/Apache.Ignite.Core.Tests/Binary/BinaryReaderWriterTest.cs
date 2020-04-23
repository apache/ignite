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
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using NUnit.Framework;
    using BinaryReader = Apache.Ignite.Core.Impl.Binary.BinaryReader;
    using BinaryWriter = Apache.Ignite.Core.Impl.Binary.BinaryWriter;

    /// <summary>
    /// Tests the <see cref="BinaryReader"/> and <see cref="BinaryWriter"/> classes.
    /// </summary>
    public class BinaryReaderWriterTest
    {
        /// <summary>
        /// Tests all read/write methods.
        /// </summary>
        [Test]
        public void TestWriteRead()
        {
            var marsh = new Marshaller(new BinaryConfiguration(typeof(ReadWriteAll)));

            marsh.Unmarshal<ReadWriteAll>(marsh.Marshal(new ReadWriteAll()));
        }

        /// <summary>
        /// Tests the custom stream position.
        /// </summary>
        [Test]
        public void TestCustomPosition()
        {
            var stream = new BinaryHeapStream(16);

            stream.WriteLong(54);

            var marsh = new Marshaller(new BinaryConfiguration());

            var writer = new BinaryWriter(marsh, stream);

            writer.WriteChar('x');

            stream.Seek(0, SeekOrigin.Begin);

            Assert.AreEqual(54, stream.ReadLong());

            var reader = new BinaryReader(marsh, stream, BinaryMode.Deserialize, null);

            Assert.AreEqual('x', reader.ReadChar());
        }

        private class ReadWriteAll : IBinarizable
        {
            private static readonly DateTime Date = DateTime.UtcNow;

            private static readonly Guid Guid = Guid.NewGuid();

            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteByte("Byte", 1);
                writer.WriteByteArray("ByteArray", new byte[] {1});
                writer.WriteChar("Char", '1');
                writer.WriteCharArray("CharArray", new[] {'1'});
                writer.WriteShort("Short", 1);
                writer.WriteShortArray("ShortArray", new short[] {1});
                writer.WriteInt("Int", 1);
                writer.WriteIntArray("IntArray", new[] {1});
                writer.WriteLong("Long", 1);
                writer.WriteLongArray("LongArray", new long[] {1});
                writer.WriteBoolean("Boolean", true);
                writer.WriteBooleanArray("BooleanArray", new[] {true});
                writer.WriteFloat("Float", 1);
                writer.WriteFloatArray("FloatArray", new float[] {1});
                writer.WriteDouble("Double", 1);
                writer.WriteDoubleArray("DoubleArray", new double[] {1});
                writer.WriteDecimal("Decimal", 1);
                writer.WriteDecimal("DecimalN", null);
                writer.WriteDecimalArray("DecimalArray", new decimal?[] {1});
                writer.WriteTimestamp("Timestamp", Date);
                writer.WriteTimestampArray("TimestampArray", new DateTime?[] {Date});
                writer.WriteString("String", "1");
                writer.WriteStringArray("StringArray", new[] {"1"});
                writer.WriteGuid("Guid", Guid);
                writer.WriteGuid("GuidN", null);
                writer.WriteGuidArray("GuidArray", new Guid?[] {Guid});
                writer.WriteEnum("Enum", MyEnum.Bar);
                writer.WriteEnumArray("EnumArray", new[] {MyEnum.Bar});

                var raw = writer.GetRawWriter();

                raw.WriteByte(1);
                raw.WriteByteArray(new byte[] {1});
                raw.WriteChar('1');
                raw.WriteCharArray(new[] {'1'});
                raw.WriteShort(1);
                raw.WriteShortArray(new short[] {1});
                raw.WriteInt(1);
                raw.WriteIntArray(new[] {1});
                raw.WriteLong(1);
                raw.WriteLongArray(new long[] {1});
                raw.WriteBoolean(true);
                raw.WriteBooleanArray(new[] {true});
                raw.WriteFloat(1);
                raw.WriteFloatArray(new float[] {1});
                raw.WriteDouble(1);
                raw.WriteDoubleArray(new double[] {1});
                raw.WriteDecimal(1);
                raw.WriteDecimal(null);
                raw.WriteDecimalArray(new decimal?[] {1});
                raw.WriteTimestamp(Date);
                raw.WriteTimestampArray(new DateTime?[] {Date});
                raw.WriteString("1");
                raw.WriteStringArray(new[] {"1"});
                raw.WriteGuid(Guid);
                raw.WriteGuid(null);
                raw.WriteGuidArray(new Guid?[] {Guid});
                raw.WriteEnum(MyEnum.Bar);
                raw.WriteEnumArray(new[] {MyEnum.Bar});
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Assert.AreEqual(1, reader.ReadByte("Byte"));
                Assert.AreEqual(new byte[] {1}, reader.ReadByteArray("ByteArray"));
                Assert.AreEqual('1', reader.ReadChar("Char"));
                Assert.AreEqual(new[] {'1'}, reader.ReadCharArray("CharArray"));
                Assert.AreEqual(1, reader.ReadShort("Short"));
                Assert.AreEqual(new short[] {1}, reader.ReadShortArray("ShortArray"));
                Assert.AreEqual(1, reader.ReadInt("Int"));
                Assert.AreEqual(new[] {1}, reader.ReadIntArray("IntArray"));
                Assert.AreEqual(1, reader.ReadLong("Long"));
                Assert.AreEqual(new long[] {1}, reader.ReadLongArray("LongArray"));
                Assert.AreEqual(true, reader.ReadBoolean("Boolean"));
                Assert.AreEqual(new[] {true}, reader.ReadBooleanArray("BooleanArray"));
                Assert.AreEqual(1, reader.ReadFloat("Float"));
                Assert.AreEqual(new float[] {1}, reader.ReadFloatArray("FloatArray"));
                Assert.AreEqual(1, reader.ReadDouble("Double"));
                Assert.AreEqual(new double[] {1}, reader.ReadDoubleArray("DoubleArray"));
                Assert.AreEqual(1, reader.ReadDecimal("Decimal"));
                Assert.AreEqual(null, reader.ReadDecimal("DecimalN"));
                Assert.AreEqual(new decimal?[] {1}, reader.ReadDecimalArray("DecimalArray"));
                Assert.AreEqual(Date, reader.ReadTimestamp("Timestamp"));
                Assert.AreEqual(new DateTime?[] {Date}, reader.ReadTimestampArray("TimestampArray"));
                Assert.AreEqual("1", reader.ReadString("String"));
                Assert.AreEqual(new[] {"1"}, reader.ReadStringArray("StringArray"));
                Assert.AreEqual(Guid, reader.ReadGuid("Guid"));
                Assert.AreEqual(null, reader.ReadGuid("GuidN"));
                Assert.AreEqual(new Guid?[] {Guid}, reader.ReadGuidArray("GuidArray"));
                Assert.AreEqual(MyEnum.Bar, reader.ReadEnum<MyEnum>("Enum"));
                Assert.AreEqual(new[] {MyEnum.Bar}, reader.ReadEnumArray<MyEnum>("EnumArray"));

                var raw = reader.GetRawReader();

                Assert.AreEqual(1, raw.ReadByte());
                Assert.AreEqual(new byte[] { 1 }, raw.ReadByteArray());
                Assert.AreEqual('1', raw.ReadChar());
                Assert.AreEqual(new[] { '1' }, raw.ReadCharArray());
                Assert.AreEqual(1, raw.ReadShort());
                Assert.AreEqual(new short[] { 1 }, raw.ReadShortArray());
                Assert.AreEqual(1, raw.ReadInt());
                Assert.AreEqual(new[] { 1 }, raw.ReadIntArray());
                Assert.AreEqual(1, raw.ReadLong());
                Assert.AreEqual(new long[] { 1 }, raw.ReadLongArray());
                Assert.AreEqual(true, raw.ReadBoolean());
                Assert.AreEqual(new[] { true }, raw.ReadBooleanArray());
                Assert.AreEqual(1, raw.ReadFloat());
                Assert.AreEqual(new float[] { 1 }, raw.ReadFloatArray());
                Assert.AreEqual(1, raw.ReadDouble());
                Assert.AreEqual(new double[] { 1 }, raw.ReadDoubleArray());
                Assert.AreEqual(1, raw.ReadDecimal());
                Assert.AreEqual(null, raw.ReadDecimal());
                Assert.AreEqual(new decimal?[] { 1 }, raw.ReadDecimalArray());
                Assert.AreEqual(Date, raw.ReadTimestamp());
                Assert.AreEqual(new DateTime?[] { Date }, raw.ReadTimestampArray());
                Assert.AreEqual("1", raw.ReadString());
                Assert.AreEqual(new[] { "1" }, raw.ReadStringArray());
                Assert.AreEqual(Guid, raw.ReadGuid());
                Assert.AreEqual(null, raw.ReadGuid());
                Assert.AreEqual(new Guid?[] { Guid }, raw.ReadGuidArray());
                Assert.AreEqual(MyEnum.Bar, raw.ReadEnum<MyEnum>());
                Assert.AreEqual(new[] { MyEnum.Bar }, raw.ReadEnumArray<MyEnum>());
            }
        }

        private enum MyEnum
        {
            Bar
        }
    }
}
