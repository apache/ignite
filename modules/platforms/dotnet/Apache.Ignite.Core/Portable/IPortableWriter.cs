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

namespace Apache.Ignite.Core.Portable 
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// Writer for portable objects. 
    /// </summary>
    public interface IPortableWriter 
    {
        /// <summary>
        /// Write named byte value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Byte value.</param>
        void WriteByte(string fieldName, byte val);

        /// <summary>
        /// Write named byte array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Byte array.</param>
        void WriteByteArray(string fieldName, byte[] val);

        /// <summary>
        /// Write named char value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Char value.</param>
        void WriteChar(string fieldName, char val);

        /// <summary>
        /// Write named char array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Char array.</param>
        void WriteCharArray(string fieldName, char[] val);

        /// <summary>
        /// Write named short value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Short value.</param>
        void WriteShort(string fieldName, short val);

        /// <summary>
        /// Write named short array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Short array.</param>
        void WriteShortArray(string fieldName, short[] val);

        /// <summary>
        /// Write named int value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Int value.</param>
        void WriteInt(string fieldName, int val);

        /// <summary>
        /// Write named int array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Int array.</param>
        void WriteIntArray(string fieldName, int[] val);

        /// <summary>
        /// Write named long value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Long value.</param>
        void WriteLong(string fieldName, long val);

        /// <summary>
        /// Write named long array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Long array.</param>
        void WriteLongArray(string fieldName, long[] val);

        /// <summary>
        /// Write named boolean value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Boolean value.</param>
        void WriteBoolean(string fieldName, bool val);

        /// <summary>
        /// Write named boolean array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Boolean array.</param>
        void WriteBooleanArray(string fieldName, bool[] val);

        /// <summary>
        /// Write named float value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Float value.</param>
        void WriteFloat(string fieldName, float val);

        /// <summary>
        /// Write named float array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Float array.</param>
        void WriteFloatArray(string fieldName, float[] val);

        /// <summary>
        /// Write named double value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Double value.</param>
        void WriteDouble(string fieldName, double val);

        /// <summary>
        /// Write named double array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Double array.</param>
        void WriteDoubleArray(string fieldName, double[] val);

        /// <summary>
        /// Write named decimal value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Decimal value.</param>
        void WriteDecimal(string fieldName, decimal val);

        /// <summary>
        /// Write named decimal array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Decimal array.</param>
        void WriteDecimalArray(string fieldName, decimal[] val);

        /// <summary>
        /// Write named date value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Date value.</param>
        void WriteDate(string fieldName, DateTime? val);

        /// <summary>
        /// Write named date array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Date array.</param>
        void WriteDateArray(string fieldName, DateTime?[] val);

        /// <summary>
        /// Write named string value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">String value.</param>
        void WriteString(string fieldName, string val);

        /// <summary>
        /// Write named string array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">String array.</param>
        void WriteStringArray(string fieldName, string[] val);

        /// <summary>
        /// Write named GUID value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">GUID value.</param>
        void WriteGuid(string fieldName, Guid? val);

        /// <summary>
        /// Write named GUID array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">GUID array.</param>
        void WriteGuidArray(string fieldName, Guid?[] val);

        /// <summary>
        /// Write named enum value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Enum value.</param>
        void WriteEnum<T>(string fieldName, T val);

        /// <summary>
        /// Write named enum array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Enum array.</param>
        void WriteEnumArray<T>(string fieldName, T[] val);

        /// <summary>
        /// Write named object value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Object value.</param>
        void WriteObject<T>(string fieldName, T val);

        /// <summary>
        /// Write named object array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Object array.</param>
        void WriteObjectArray<T>(string fieldName, T[] val);

        /// <summary>
        /// Write named collection.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Collection.</param>
        void WriteCollection(string fieldName, ICollection val);

        /// <summary>
        /// Write named generic collection.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Collection.</param>
        void WriteGenericCollection<T>(string fieldName, ICollection<T> val);

        /// <summary>
        /// Write named dictionary.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Dictionary.</param>
        void WriteDictionary(string fieldName, IDictionary val);

        /// <summary>
        /// Write named generic dictionary.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="val">Dictionary.</param>
        void WriteGenericDictionary<TK, TV>(string fieldName, IDictionary<TK, TV> val);

        /// <summary>
        /// Get raw writer. 
        /// </summary>
        /// <returns>Raw writer.</returns>
        IPortableRawWriter RawWriter();
    }
}
