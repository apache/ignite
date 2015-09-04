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
    /// Raw reader for portable objects. 
    /// </summary>
    public interface IPortableRawReader
    {
        /// <summary>
        /// Read byte value. 
        /// </summary>
        /// <returns>Byte value.</returns>
        byte ReadByte();

        /// <summary>
        /// Read byte array. 
        /// </summary>
        /// <returns>Byte array.</returns>
        byte[] ReadByteArray();

        /// <summary>
        /// Read char value. 
        /// </summary>
        /// <returns>Char value.</returns>
        char ReadChar();

        /// <summary>
        /// Read char array. 
        /// </summary>
        /// <returns>Char array.</returns>
        char[] ReadCharArray();

        /// <summary>
        /// Read short value. 
        /// </summary>
        /// <returns>Short value.</returns>
        short ReadShort();

        /// <summary>
        /// Read short array. 
        /// </summary>
        /// <returns>Short array.</returns>
        short[] ReadShortArray();

        /// <summary>
        /// Read int value. 
        /// </summary>
        /// <returns>Int value.</returns>
        int ReadInt();

        /// <summary>
        /// Read int array. 
        /// </summary>
        /// <returns>Int array.</returns>
        int[] ReadIntArray();

        /// <summary>
        /// Read long value. 
        /// </summary>
        /// <returns>Long value.</returns>
        long ReadLong();

        /// <summary>
        /// Read long array. 
        /// </summary>
        /// <returns>Long array.</returns>
        long[] ReadLongArray();

        /// <summary>
        /// Read boolean value. 
        /// </summary>
        /// <returns>Boolean value.</returns>
        bool ReadBoolean();

        /// <summary>
        /// Read boolean array. 
        /// </summary>
        /// <returns>Boolean array.</returns>
        bool[] ReadBooleanArray();

        /// <summary>
        /// Read float value. 
        /// </summary>
        /// <returns>Float value.</returns>
        float ReadFloat();

        /// <summary>
        /// Read float array. 
        /// </summary>
        /// <returns>Float array.</returns>
        float[] ReadFloatArray();

        /// <summary>
        /// Read double value. 
        /// </summary>
        /// <returns>Double value.</returns>
        double ReadDouble();

        /// <summary>
        /// Read double array. 
        /// </summary>
        /// <returns>Double array.</returns>
        double[] ReadDoubleArray();

        /// <summary>
        /// Read decimal value. 
        /// </summary>
        /// <returns>Decimal value.</returns>
        decimal ReadDecimal();

        /// <summary>
        /// Read decimal array. 
        /// </summary>
        /// <returns>Decimal array.</returns>
        decimal[] ReadDecimalArray();

        /// <summary>
        /// Read date value in UTC form. Shortcut for <c>ReadDate(false)</c>.
        /// </summary>
        /// <returns>Date value.</returns>
        DateTime? ReadDate();

        /// <summary>
        /// Read date value.
        /// </summary>
        /// <param name="local">Whether to read date in local (<c>true</c>) or UTC (<c>false</c>) form.</param>
        /// <returns></returns>
        DateTime? ReadDate(bool local);

        /// <summary>
        /// Read date array in UTC form. Shortcut for <c>ReadDateArray(false)</c>.
        /// </summary>
        /// <returns>Date array.</returns>
        DateTime?[] ReadDateArray();

        /// <summary>
        /// Read date array.
        /// </summary>
        /// <param name="local">Whether to read date array in local (<c>true</c>) or UTC (<c>false</c>) form.</param>
        /// <returns>Date array.</returns>
        DateTime?[] ReadDateArray(bool local);

        /// <summary>
        /// Read string value. 
        /// </summary>
        /// <returns>String value.</returns>
        string ReadString();

        /// <summary>
        /// Read string array. 
        /// </summary>
        /// <returns>String array.</returns>
        string[] ReadStringArray();

        /// <summary>
        /// Read GUID value. 
        /// </summary>
        /// <returns>GUID value.</returns>
        Guid? ReadGuid();

        /// <summary>
        /// Read GUID array. 
        /// </summary>
        /// <returns>GUID array.</returns>
        Guid?[] ReadGuidArray();

        /// <summary>
        /// Read enum value.
        /// </summary>
        /// <returns>Enum value.</returns>
        T ReadEnum<T>();

        /// <summary>
        /// Read enum array.
        /// </summary>
        /// <returns>Enum array.</returns>
        T[] ReadEnumArray<T>();
        
        /// <summary>
        /// Read object. 
        /// </summary>
        /// <returns>Object.</returns>
        T ReadObject<T>();

        /// <summary>
        /// Read object array. 
        /// </summary>
        /// <returns>Object array.</returns>
        T[] ReadObjectArray<T>();

        /// <summary>
        /// Read collection. 
        /// </summary>
        /// <returns>Collection.</returns>
        ICollection ReadCollection();

        /// <summary>
        /// Read collection.
        /// </summary>
        /// <param name="factory">Factory.</param>
        /// <param name="adder">Adder.</param>
        /// <returns>Collection.</returns>
        ICollection ReadCollection(PortableCollectionFactory factory, PortableCollectionAdder adder);

        /// <summary>
        /// Read generic collection. 
        /// </summary>
        /// <returns>Collection.</returns>
        ICollection<T> ReadGenericCollection<T>();

        /// <summary>
        /// Read generic collection.
        /// </summary>
        /// <param name="factory">Factory.</param>
        /// <returns>Collection.</returns>
        ICollection<T> ReadGenericCollection<T>(PortableGenericCollectionFactory<T> factory);

        /// <summary>
        /// Read dictionary. 
        /// </summary>
        /// <returns>Dictionary.</returns>
        IDictionary ReadDictionary();

        /// <summary>
        /// Read dictionary.
        /// </summary>
        /// <param name="factory">Factory.</param>
        /// <returns>Dictionary.</returns>
        IDictionary ReadDictionary(PortableDictionaryFactory factory);

        /// <summary>
        /// Read generic dictionary. 
        /// </summary>
        /// <returns>Dictionary.</returns>
        IDictionary<TK, TV> ReadGenericDictionary<TK, TV>();

        /// <summary>
        /// Read generic dictionary.
        /// </summary>
        /// <param name="factory">Factory.</param>
        /// <returns>Dictionary.</returns>
        IDictionary<TK, TV> ReadGenericDictionary<TK, TV>(PortableGenericDictionaryFactory<TK, TV> factory);
    }
}
