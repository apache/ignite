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
    /// Delegate for collection creation.
    /// </summary>
    /// <param name="size">Collection size.</param>
    /// <returns>Collection.</returns>
    public delegate ICollection PortableCollectionFactory(int size);

    /// <summary>
    /// Delegate for adding element to collection.
    /// </summary>
    /// <param name="col">Collection.</param>
    /// <param name="elem">Element to add.</param>
    public delegate void PortableCollectionAdder(ICollection col, object elem);

    /// <summary>
    /// Delegate for generic collection creation.
    /// </summary>
    /// <param name="size">Collection size.</param>
    /// <returns>Collection.</returns>
    public delegate ICollection<T> PortableGenericCollectionFactory<T>(int size);

    /// <summary>
    /// Delegate for dictionary creation.
    /// </summary>
    /// <param name="size">Dictionary size.</param>
    /// <returns>Dictionary.</returns>
    public delegate IDictionary PortableDictionaryFactory(int size);

    /// <summary>
    /// Delegate for generic collection creation.
    /// </summary>
    /// <param name="size">Collection size.</param>
    /// <returns>Collection.</returns>
    public delegate IDictionary<TK, TV> PortableGenericDictionaryFactory<TK, TV>(int size);

    /// <summary>
    /// Reader for portable objects. 
    /// </summary>
    public interface IPortableReader 
    {
        /// <summary>
        /// Read named byte value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Byte value.</returns>
        byte ReadByte(string fieldName);
        
        /// <summary>
        /// Read named byte array. 
        /// </summary>
        /// <returns>Byte array.</returns>
        byte[] ReadByteArray(string fieldName);
        
        /// <summary>
        /// Read named char value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Char value.</returns>
        char ReadChar(string fieldName);

        /// <summary>
        /// Read named char array. 
        /// </summary>
        /// <returns>Char array.</returns>
        char[] ReadCharArray(string fieldName);

        /// <summary>
        /// Read named short value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Short value.</returns>
        short ReadShort(string fieldName);

        /// <summary>
        /// Read named short array. 
        /// </summary>
        /// <returns>Short array.</returns>
        short[] ReadShortArray(string fieldName);        

        /// <summary>
        /// Read named int value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Int value.</returns>
        int ReadInt(string fieldName);

        /// <summary>
        /// Read named int array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Int array.</returns>
        int[] ReadIntArray(string fieldName);

        /// <summary>
        /// Read named long value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Long value.</returns>
        long ReadLong(string fieldName);

        /// <summary>
        /// Read named long array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Long array.</returns>
        long[] ReadLongArray(string fieldName);

        /// <summary>
        /// Read named boolean value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Boolean value.</returns>
        bool ReadBoolean(string fieldName);

        /// <summary>
        /// Read named boolean array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Boolean array.</returns>
        bool[] ReadBooleanArray(string fieldName);

        /// <summary>
        /// Read named float value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Float value.</returns>
        float ReadFloat(string fieldName);

        /// <summary>
        /// Read named float array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Float array.</returns>
        float[] ReadFloatArray(string fieldName);

        /// <summary>
        /// Read named double value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Double value.</returns>
        double ReadDouble(string fieldName);        

        /// <summary>
        /// Read named double array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Double array.</returns>
        double[] ReadDoubleArray(string fieldName);

        /// <summary>
        /// Read named decimal value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Decimal value.</returns>
        decimal ReadDecimal(string fieldName);

        /// <summary>
        /// Read named decimal array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Decimal array.</returns>
        decimal[] ReadDecimalArray(string fieldName);

        /// <summary>
        /// Read named date value in UTC form. Shortcut for <c>ReadDate(fieldName, false)</c>.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Date value.</returns>
        DateTime? ReadDate(string fieldName);

        /// <summary>
        /// Read named date value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="local">Whether to read date in local (<c>true</c>) or UTC (<c>false</c>) form.</param>
        /// <returns>Date vaule.</returns>
        DateTime? ReadDate(string fieldName, bool local);

        /// <summary>
        /// Read named date array in UTC form. Shortcut for <c>ReadDateArray(fieldName, false)</c>.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Date array.</returns>
        DateTime?[] ReadDateArray(string fieldName);

        /// <summary>
        /// Read named date array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="local">Whether to read date in local (<c>true</c>) or UTC (<c>false</c>) form.</param>
        /// <returns>Date array.</returns>
        DateTime?[] ReadDateArray(string fieldName, bool local);

        /// <summary>
        /// Read named string value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>String value.</returns>
        string ReadString(string fieldName);

        /// <summary>
        /// Read named string array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>String array.</returns>
        string[] ReadStringArray(string fieldName);

        /// <summary>
        /// Read named GUID value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>GUID value.</returns>
        Guid? ReadGuid(string fieldName);

        /// <summary>
        /// Read named GUID array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>GUID array.</returns>
        Guid?[] ReadGuidArray(string fieldName);
        
        /// <summary>
        /// Read named enum value.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Enum value.</returns>
        T ReadEnum<T>(string fieldName);

        /// <summary>
        /// Read named enum array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Enum array.</returns>
        T[] ReadEnumArray<T>(string fieldName);

        /// <summary>
        /// Read named object.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Object.</returns>
        T ReadObject<T>(string fieldName);

        /// <summary>
        /// Read named object array.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Object array.</returns>
        T[] ReadObjectArray<T>(string fieldName);

        /// <summary>
        /// Read named collection.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Collection.</returns>
        ICollection ReadCollection(string fieldName);

        /// <summary>
        /// Read named collection.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="factory">Factory.</param>
        /// <param name="adder">Adder.</param>
        /// <returns>Collection.</returns>
        ICollection ReadCollection(string fieldName, PortableCollectionFactory factory, PortableCollectionAdder adder);

        /// <summary>
        /// Read named generic collection.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Collection.</returns>
        ICollection<T> ReadGenericCollection<T>(string fieldName);

        /// <summary>
        /// Read named generic collection.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="factory">Factory.</param>
        /// <returns>Collection.</returns>
        ICollection<T> ReadGenericCollection<T>(string fieldName, PortableGenericCollectionFactory<T> factory);

        /// <summary>
        /// Read named dictionary.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Dictionary.</returns>
        IDictionary ReadDictionary(string fieldName);

        /// <summary>
        /// Read named dictionary.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="factory">Factory.</param>
        /// <returns>Dictionary.</returns>
        IDictionary ReadDictionary(string fieldName, PortableDictionaryFactory factory);

        /// <summary>
        /// Read named generic dictionary.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <returns>Dictionary.</returns>
        IDictionary<TK, TV> ReadGenericDictionary<TK, TV>(string fieldName);

        /// <summary>
        /// Read named generic dictionary.
        /// </summary>
        /// <param name="fieldName">Field name.</param>
        /// <param name="factory">Factory.</param>
        /// <returns>Dictionary.</returns>
        IDictionary<TK, TV> ReadGenericDictionary<TK, TV>(string fieldName, PortableGenericDictionaryFactory<TK, TV> factory);

        /// <summary>
        /// Get raw reader. 
        /// </summary>
        /// <returns>Raw reader.</returns>
        IPortableRawReader RawReader();
    }
}