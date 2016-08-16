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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using NUnit.Framework;

    /// <summary>
    /// Tests the <see cref="Impl.Binary.BinaryReader"/> and <see cref="Impl.Binary.BinaryWriter"/> classes.
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

        private class ReadWriteAll : IBinarizable
        {
            public void WriteBinary(IBinaryWriter writer)
            {
                writer.WriteByte("Byte", 1);
                writer.WriteByteArray("ByteArray", new byte[] { 1});
                writer.WriteChar("Char", '1');
                writer.WriteCharArray("CharArray", new[] { '1' });
                writer.WriteShort("Short", 1);
                writer.WriteShortArray("ShortArray", new short[] {1});
                writer.WriteInt("Int", 1);
                writer.WriteIntArray("IntArray", new[] {1});
                writer.WriteLong("Long", 1);
                writer.WriteLongArray("LongArray", new long[] { 1 });
                writer.WriteBoolean("Boolean", true);
                writer.WriteBooleanArray("BooleanArray", new [] {true});
                writer.WriteFloat("Float", 1);
                writer.WriteFloatArray("FloatArray", new float[] {1});
                writer.WriteDouble("Double", 1);
                writer.WriteDoubleArray("DoubleArray", new double[] {1});
                writer.WriteDecimal("Decimal", 1);
                writer.WriteDecimalArray("DecimalArray", new decimal?[] {1});
                writer.WriteTimestamp("Timestamp", DateTime.UtcNow);
                writer.WriteTimestampArray("TimestampArray", new DateTime?[] {DateTime.UtcNow});
                writer.WriteString("String", "1");
                writer.WriteStringArray("StringArray", new[] {"1"});
                writer.WriteGuid("Guid", Guid.Empty);
                writer.WriteGuidArray("GuidArray", new Guid?[] {Guid.Empty});
                writer.WriteEnum("Enum", 1);
                writer.WriteEnumArray("EnumArray", new[] {1});
            }

            public void ReadBinary(IBinaryReader reader)
            {
                Assert.AreEqual(1, reader.ReadInt("int"));
            }
        }
    }
}
