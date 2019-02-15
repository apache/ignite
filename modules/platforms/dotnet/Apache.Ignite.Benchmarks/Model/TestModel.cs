/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Benchmarks.Model
{
    using System;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Model class with all kinds of fields to test serialization.
    /// </summary>
    internal class TestModel : IBinarizable
    {
        public byte Byte { get; set; }
        public byte[] ByteArray { get; set; }
        public char Char { get; set; }
        public char[] CharArray { get; set; }
        public short Short { get; set; }
        public short[] ShortArray { get; set; }
        public int Int { get; set; }
        public int[] IntArray { get; set; }
        public long Long { get; set; }
        public long[] LongArray { get; set; }
        public bool Boolean { get; set; }
        public bool[] BooleanArray { get; set; }
        public float Float { get; set; }
        public float[] FloatArray { get; set; }
        public double Double { get; set; }
        public double[] DoubleArray { get; set; }
        public decimal? Decimal { get; set; }
        public decimal?[] DecimalArray { get; set; }
        public DateTime? Date { get; set; }
        public DateTime?[] DateArray { get; set; }
        public string String { get; set; }
        public string[] StringArray { get; set; }
        public Guid? Guid { get; set; }
        public Guid?[] GuidArray { get; set; }

        /** <inheritDoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            writer.WriteByte("Byte", Byte);
            writer.WriteByteArray("ByteArray", ByteArray);
            writer.WriteChar("Char", Char);
            writer.WriteCharArray("CharArray", CharArray);
            writer.WriteShort("Short", Short);
            writer.WriteShortArray("ShortArray", ShortArray);
            writer.WriteInt("Int", Int);
            writer.WriteIntArray("IntArray", IntArray);
            writer.WriteLong("Long", Long);
            writer.WriteLongArray("LongArray", LongArray);
            writer.WriteBoolean("Boolean", Boolean);
            writer.WriteBooleanArray("BooleanArray", BooleanArray);
            writer.WriteFloat("Float", Float);
            writer.WriteFloatArray("FloatArray", FloatArray);
            writer.WriteDouble("Double", Double);
            writer.WriteDoubleArray("DoubleArray", DoubleArray);
            writer.WriteDecimal("Decimal", Decimal);
            writer.WriteDecimalArray("DecimalArray", DecimalArray);
            writer.WriteTimestamp("Date", Date);
            writer.WriteTimestampArray("DateArray", DateArray);
            writer.WriteString("String", String);
            writer.WriteStringArray("StringArray", StringArray);
            writer.WriteGuid("Guid", Guid);
            writer.WriteGuidArray("GuidArray", GuidArray);
        }

        /** <inheritDoc /> */
        public void ReadBinary(IBinaryReader reader)
        {
            Byte = reader.ReadByte("Byte");
            ByteArray = reader.ReadByteArray("ByteArray");
            Char = reader.ReadChar("Char");
            CharArray = reader.ReadCharArray("CharArray");
            Short = reader.ReadShort("Short");
            ShortArray = reader.ReadShortArray("ShortArray");
            Int = reader.ReadInt("Int");
            IntArray = reader.ReadIntArray("IntArray");
            Long = reader.ReadLong("Long");
            LongArray = reader.ReadLongArray("LongArray");
            Boolean = reader.ReadBoolean("Boolean");
            BooleanArray = reader.ReadBooleanArray("BooleanArray");
            Float = reader.ReadFloat("Float");
            FloatArray = reader.ReadFloatArray("FloatArray");
            Double = reader.ReadDouble("Double");
            DoubleArray = reader.ReadDoubleArray("DoubleArray");
            Decimal = reader.ReadDecimal("Decimal");
            DecimalArray = reader.ReadDecimalArray("DecimalArray");
            Date = reader.ReadTimestamp("Date");
            DateArray = reader.ReadTimestampArray("DateArray");
            String = reader.ReadString("String");
            StringArray = reader.ReadStringArray("StringArray");
            Guid = reader.ReadObject<Guid>("Guid");
            GuidArray = reader.ReadGuidArray("GuidArray");
        }
    }
}
