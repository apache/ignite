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
        /** Type: object. */
        public const byte TypeObject = BinaryUtils.HdrFull;

        /** Type: unregistered. */
        public const byte TypeUnregistered = 0;

        /** Type: unsigned byte. */
        public const byte TypeByte = 1;

        /** Type: short. */
        public const byte TypeShort = 2;

        /** Type: int. */
        public const byte TypeInt = 3;

        /** Type: long. */
        public const byte TypeLong = 4;

        /** Type: float. */
        public const byte TypeFloat = 5;

        /** Type: double. */
        public const byte TypeDouble = 6;

        /** Type: char. */
        public const byte TypeChar = 7;

        /** Type: boolean. */
        public const byte TypeBool = 8;

        /** Type: decimal. */
        public const byte TypeDecimal = 30;

        /** Type: string. */
        public const byte TypeString = 9;

        /** Type: GUID. */
        public const byte TypeGuid = 10;

        /** Type: date. */
        public const byte TypeTimestamp = 33;

        /** Type: unsigned byte array. */
        public const byte TypeArrayByte = 12;

        /** Type: short array. */
        public const byte TypeArrayShort = 13;

        /** Type: int array. */
        public const byte TypeArrayInt = 14;

        /** Type: long array. */
        public const byte TypeArrayLong = 15;

        /** Type: float array. */
        public const byte TypeArrayFloat = 16;

        /** Type: double array. */
        public const byte TypeArrayDouble = 17;

        /** Type: char array. */
        public const byte TypeArrayChar = 18;

        /** Type: boolean array. */
        public const byte TypeArrayBool = 19;

        /** Type: decimal array. */
        public const byte TypeArrayDecimal = 31;

        /** Type: string array. */
        public const byte TypeArrayString = 20;

        /** Type: GUID array. */
        public const byte TypeArrayGuid = 21;

        /** Type: date array. */
        public const byte TypeArrayTimestamp = 34;

        /** Type: object array. */
        public const byte TypeArray = 23;

        /** Type: collection. */
        public const byte TypeCollection = 24;

        /** Type: map. */
        public const byte TypeDictionary = 25;

        /** Type: binary object. */
        public const byte TypeBinary = 27;

        /** Type: enum. */
        public const byte TypeEnum = 28;

        /** Type: enum array. */
        public const byte TypeArrayEnum = 29;

        /** Type: binary enum. */
        public const byte TypeBinaryEnum = 38;

        /** Type: native job holder. */
        public const byte TypeNativeJobHolder = 77;

        /** Type: function wrapper. */
        public const byte TypeComputeOutFuncJob = 80;

        /** Type: function wrapper. */
        public const byte TypeComputeFuncJob = 81;

        /** Type: continuous query remote filter. */
        public const byte TypeContinuousQueryRemoteFilterHolder = 82;

        /** Type: Compute out func wrapper. */
        public const byte TypeComputeOutFuncWrapper = 83;

        /** Type: Compute func wrapper. */
        public const byte TypeComputeFuncWrapper = 85;

        /** Type: Compute job wrapper. */
        public const byte TypeComputeJobWrapper = 86;

        /** Type: action wrapper. */
        public const byte TypeComputeActionJob = 88;

        /** Type: entry processor holder. */
        public const byte TypeCacheEntryProcessorHolder = 89;

        /** Type: entry predicate holder. */
        public const byte TypeCacheEntryPredicateHolder = 90;

        /** Type: message filter holder. */
        public const byte TypeMessageListenerHolder = 92;

        /** Type: stream receiver holder. */
        public const byte TypeStreamReceiverHolder = 94;

        /** Type: platform object proxy. */
        public const byte TypePlatformJavaObjectFactoryProxy = 99;

        /** Type: platform object proxy. */
        public const int TypeIgniteUuid = 2018070327;

        /** Type ids. */
        private static readonly Dictionary<Type, byte> TypeIds = new Dictionary<Type, byte>
        {
            {typeof (bool), TypeBool},
            {typeof (byte), TypeByte},
            {typeof (sbyte), TypeByte},
            {typeof (short), TypeShort},
            {typeof (ushort), TypeShort},
            {typeof (char), TypeChar},
            {typeof (int), TypeInt},
            {typeof (uint), TypeInt},
            {typeof (long), TypeLong},
            {typeof (ulong), TypeLong},
            {typeof (float), TypeFloat},
            {typeof (double), TypeDouble},
            {typeof (string), TypeString},
            {typeof (decimal), TypeDecimal},
            {typeof (Guid), TypeGuid},
            {typeof (Guid?), TypeGuid},
            {typeof (ArrayList), TypeCollection},
            {typeof (Hashtable), TypeDictionary},
            {typeof (bool[]), TypeArrayBool},
            {typeof (byte[]), TypeArrayByte},
            {typeof (sbyte[]), TypeArrayByte},
            {typeof (short[]), TypeArrayShort},
            {typeof (ushort[]), TypeArrayShort},
            {typeof (char[]), TypeArrayChar},
            {typeof (int[]), TypeArrayInt},
            {typeof (uint[]), TypeArrayInt},
            {typeof (long[]), TypeArrayLong},
            {typeof (ulong[]), TypeArrayLong},
            {typeof (float[]), TypeArrayFloat},
            {typeof (double[]), TypeArrayDouble},
            {typeof (string[]), TypeArrayString},
            {typeof (decimal?[]), TypeArrayDecimal},
            {typeof (Guid?[]), TypeArrayGuid},
            {typeof (object[]), TypeArray}
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
                return TypeEnum;

            if (type.IsArray && BinaryUtils.IsIgniteEnum(type.GetElementType()))
                return TypeArrayEnum;

            return TypeObject;
        }
    }
}
