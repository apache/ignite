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

namespace Apache.Ignite.Core.Binary
{
    using System;
    using System.Collections;

    /// <summary>
    /// Raw writer for binary objects. 
    /// </summary>
    public interface IBinaryRawWriter
    {
        /// <summary>
        /// Write byte value.
        /// </summary>
        /// <param name="val">Byte value.</param>
        void WriteByte(byte val);

        /// <summary>
        /// Write byte array.
        /// </summary>
        /// <param name="val">Byte array.</param>
        void WriteByteArray(byte[] val);

        /// <summary>
        /// Write char value.
        /// </summary>
        /// <param name="val">Char value.</param>
        void WriteChar(char val);

        /// <summary>
        /// Write char array.
        /// </summary>
        /// <param name="val">Char array.</param>
        void WriteCharArray(char[] val);

        /// <summary>
        /// Write short value.
        /// </summary>
        /// <param name="val">Short value.</param>
        void WriteShort(short val);

        /// <summary>
        /// Write short array.
        /// </summary>
        /// <param name="val">Short array.</param>
        void WriteShortArray(short[] val);

        /// <summary>
        /// Write int value.
        /// </summary>
        /// <param name="val">Int value.</param>
        void WriteInt(int val);

        /// <summary>
        /// Write int array.
        /// </summary>
        /// <param name="val">Int array.</param>
        void WriteIntArray(int[] val);

        /// <summary>
        /// Write long value.
        /// </summary>
        /// <param name="val">Long value.</param>
        void WriteLong(long val);

        /// <summary>
        /// Write long array.
        /// </summary>
        /// <param name="val">Long array.</param>
        void WriteLongArray(long[] val);

        /// <summary>
        /// Write boolean value.
        /// </summary>
        /// <param name="val">Boolean value.</param>
        void WriteBoolean(bool val);

        /// <summary>
        /// Write boolean array.
        /// </summary>
        /// <param name="val">Boolean array.</param>
        void WriteBooleanArray(bool[] val);

        /// <summary>
        /// Write float value.
        /// </summary>
        /// <param name="val">Float value.</param>
        void WriteFloat(float val);

        /// <summary>
        /// Write float array.
        /// </summary>
        /// <param name="val">Float array.</param>
        void WriteFloatArray(float[] val);

        /// <summary>
        /// Write double value.
        /// </summary>
        /// <param name="val">Double value.</param>
        void WriteDouble(double val);

        /// <summary>
        /// Write double array.
        /// </summary>
        /// <param name="val">Double array.</param>
        void WriteDoubleArray(double[] val);

        /// <summary>
        /// Write decimal value.
        /// </summary>
        /// <param name="val">Decimal value.</param>
        void WriteDecimal(decimal? val);

        /// <summary>
        /// Write decimal array.
        /// </summary>
        /// <param name="val">Decimal array.</param>
        void WriteDecimalArray(decimal?[] val);

        /// <summary>
        /// Write date value.
        /// </summary>
        /// <param name="val">Date value.</param>
        void WriteTimestamp(DateTime? val);

        /// <summary>
        /// Write date array.
        /// </summary>
        /// <param name="val">Date array.</param>
        void WriteTimestampArray(DateTime?[] val);

        /// <summary>
        /// Write string value.
        /// </summary>
        /// <param name="val">String value.</param>
        void WriteString(string val);

        /// <summary>
        /// Write string array.
        /// </summary>
        /// <param name="val">String array.</param>
        void WriteStringArray(string[] val);

        /// <summary>
        /// Write GUID value.
        /// </summary>
        /// <param name="val">GUID value.</param>
        void WriteGuid(Guid? val);

        /// <summary>
        /// Write GUID array.
        /// </summary>
        /// <param name="val">GUID array.</param>
        void WriteGuidArray(Guid?[] val);

        /// <summary>
        /// Write enum value.
        /// </summary>
        /// <param name="val">Enum value.</param>
        void WriteEnum<T>(T val);

        /// <summary>
        /// Write enum array.
        /// </summary>
        /// <param name="val">Enum array.</param>
        void WriteEnumArray<T>(T[] val);

        /// <summary>
        /// Write object value.
        /// </summary>
        /// <param name="val">Object value.</param>
        void WriteObject<T>(T val);

        /// <summary>
        /// Write object array.
        /// </summary>
        /// <param name="val">Object array.</param>
        void WriteArray<T>(T[] val);

        /// <summary>
        /// Writes a collection in interoperable form.
        /// 
        /// Use this method to communicate with other platforms 
        /// or with nodes that need to read collection elements in binary form.
        /// 
        /// When there is no need for binarization or interoperability, please use <see cref="WriteObject{T}" />,
        /// which will properly preserve generic collection type.
        /// </summary>
        /// <param name="val">Collection.</param>
        void WriteCollection(ICollection val);

        /// <summary>
        /// Writes a dictionary in interoperable form.
        /// 
        /// Use this method to communicate with other platforms 
        /// or with nodes that need to read dictionary elements in binary form.
        /// 
        /// When there is no need for binarization or interoperability, please use <see cref="WriteObject{T}" />,
        /// which will properly preserve generic dictionary type.
        /// </summary>
        /// <param name="val">Dictionary.</param>
        void WriteDictionary(IDictionary val);
    }
}
