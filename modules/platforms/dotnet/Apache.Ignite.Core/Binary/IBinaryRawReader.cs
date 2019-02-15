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

namespace Apache.Ignite.Core.Binary
{
    using System;
    using System.Collections;

    /// <summary>
    /// Raw reader for binary objects. 
    /// </summary>
    public interface IBinaryRawReader
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
        decimal? ReadDecimal();

        /// <summary>
        /// Read decimal array. 
        /// </summary>
        /// <returns>Decimal array.</returns>
        decimal?[] ReadDecimalArray();

        /// <summary>
        /// Read date value in UTC form. Shortcut for <c>ReadTimestamp(false)</c>.
        /// </summary>
        /// <returns>Date value.</returns>
        DateTime? ReadTimestamp();
        
        /// <summary>
        /// Read date array in UTC form. Shortcut for <c>ReadTimestampArray(false)</c>.
        /// </summary>
        /// <returns>Date array.</returns>
        DateTime?[] ReadTimestampArray();
        
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
        T[] ReadArray<T>();

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
        ICollection ReadCollection(Func<int, ICollection> factory, Action<ICollection, object> adder);

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
        IDictionary ReadDictionary(Func<int, IDictionary> factory);
    }
}
