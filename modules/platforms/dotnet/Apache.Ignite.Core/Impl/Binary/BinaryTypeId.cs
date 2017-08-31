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

namespace Apache.Ignite.Core.Impl.Binary
{
    using System;
    using System.Collections;
    using System.Collections.Generic;

    /// <summary>
    /// Binary type IDs.
    /// </summary>
    internal static class BinaryTypeId
    {

        /** Type ids. */
        private static readonly Dictionary<Type, byte> TypeIds = new Dictionary<Type, byte>
        {
            {typeof (bool), BinaryUtils.TypeBool},
            {typeof (byte), BinaryUtils.TypeByte},
            {typeof (sbyte), BinaryUtils.TypeByte},
            {typeof (short), BinaryUtils.TypeShort},
            {typeof (ushort), BinaryUtils.TypeShort},
            {typeof (char), BinaryUtils.TypeChar},
            {typeof (int), BinaryUtils.TypeInt},
            {typeof (uint), BinaryUtils.TypeInt},
            {typeof (long), BinaryUtils.TypeLong},
            {typeof (ulong), BinaryUtils.TypeLong},
            {typeof (float), BinaryUtils.TypeFloat},
            {typeof (double), BinaryUtils.TypeDouble},
            {typeof (string), BinaryUtils.TypeString},
            {typeof (decimal), BinaryUtils.TypeDecimal},
            {typeof (Guid), BinaryUtils.TypeGuid},
            {typeof (Guid?), BinaryUtils.TypeGuid},
            {typeof (ArrayList), BinaryUtils.TypeCollection},
            {typeof (Hashtable), BinaryUtils.TypeDictionary},
            {typeof (bool[]), BinaryUtils.TypeArrayBool},
            {typeof (byte[]), BinaryUtils.TypeArrayByte},
            {typeof (sbyte[]), BinaryUtils.TypeArrayByte},
            {typeof (short[]), BinaryUtils.TypeArrayShort},
            {typeof (ushort[]), BinaryUtils.TypeArrayShort},
            {typeof (char[]), BinaryUtils.TypeArrayChar},
            {typeof (int[]), BinaryUtils.TypeArrayInt},
            {typeof (uint[]), BinaryUtils.TypeArrayInt},
            {typeof (long[]), BinaryUtils.TypeArrayLong},
            {typeof (ulong[]), BinaryUtils.TypeArrayLong},
            {typeof (float[]), BinaryUtils.TypeArrayFloat},
            {typeof (double[]), BinaryUtils.TypeArrayDouble},
            {typeof (string[]), BinaryUtils.TypeArrayString},
            {typeof (decimal?[]), BinaryUtils.TypeArrayDecimal},
            {typeof (Guid?[]), BinaryUtils.TypeArrayGuid},
            {typeof (object[]), BinaryUtils.TypeArray}
        };

        /// <summary>
        /// Get binary type id for a type.
        /// </summary>
        public static byte GetTypeId(Type type)
        {
            byte res;

            if (TypeIds.TryGetValue(type, out res))
                return res;

            if (BinaryUtils.IsIgniteEnum(type))
                return BinaryUtils.TypeEnum;

            if (type.IsArray && BinaryUtils.IsIgniteEnum(type.GetElementType()))
                return BinaryUtils.TypeArrayEnum;

            return BinaryUtils.TypeObject;
        }


    }
}
