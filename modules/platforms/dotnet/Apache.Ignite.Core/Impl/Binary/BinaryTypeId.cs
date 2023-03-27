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
        public const byte Object = BinaryUtils.HdrFull;

        /** Type: unregistered. */
        public const byte Unregistered = 0;

        /** Type: unsigned byte. */
        public const byte Byte = 1;

        /** Type: short. */
        public const byte Short = 2;

        /** Type: int. */
        public const byte Int = 3;

        /** Type: long. */
        public const byte Long = 4;

        /** Type: float. */
        public const byte Float = 5;

        /** Type: double. */
        public const byte Double = 6;

        /** Type: char. */
        public const byte Char = 7;

        /** Type: boolean. */
        public const byte Bool = 8;

        /** Type: decimal. */
        public const byte Decimal = 30;

        /** Type: string. */
        public const byte String = 9;

        /** Type: GUID. */
        public const byte Guid = 10;

        /** Type: date. */
        public const byte Timestamp = 33;

        /** Type: unsigned byte array. */
        public const byte ArrayByte = 12;

        /** Type: short array. */
        public const byte ArrayShort = 13;

        /** Type: int array. */
        public const byte ArrayInt = 14;

        /** Type: long array. */
        public const byte ArrayLong = 15;

        /** Type: float array. */
        public const byte ArrayFloat = 16;

        /** Type: double array. */
        public const byte ArrayDouble = 17;

        /** Type: char array. */
        public const byte ArrayChar = 18;

        /** Type: boolean array. */
        public const byte ArrayBool = 19;

        /** Type: decimal array. */
        public const byte ArrayDecimal = 31;

        /** Type: string array. */
        public const byte ArrayString = 20;

        /** Type: GUID array. */
        public const byte ArrayGuid = 21;

        /** Type: date array. */
        public const byte ArrayTimestamp = 34;

        /** Type: object array. */
        public const byte Array = 23;

        /** Type: collection. */
        public const byte Collection = 24;

        /** Type: map. */
        public const byte Dictionary = 25;

        /** Type: binary object. */
        public const byte Binary = 27;

        /** Type: enum. */
        public const byte Enum = 28;

        /** Type: enum array. */
        public const byte ArrayEnum = 29;

        /** Type: binary enum. */
        public const byte BinaryEnum = 38;

        /** Type: native job holder. */
        public const byte NativeJobHolder = 77;

        /** Type: function wrapper. */
        public const byte ComputeOutFuncJob = 80;

        /** Type: function wrapper. */
        public const byte ComputeFuncJob = 81;

        /** Type: continuous query remote filter. */
        public const byte ContinuousQueryRemoteFilterHolder = 82;

        /** Type: Compute out func wrapper. */
        public const byte ComputeOutFuncWrapper = 83;

        /** Type: Compute func wrapper. */
        public const byte ComputeFuncWrapper = 85;

        /** Type: Compute job wrapper. */
        public const byte ComputeJobWrapper = 86;

        /** Type: action wrapper. */
        public const byte ComputeActionJob = 88;

        /** Type: entry processor holder. */
        public const byte CacheEntryProcessorHolder = 89;

        /** Type: entry predicate holder. */
        public const byte CacheEntryPredicateHolder = 90;

        /** Type: message filter holder. */
        public const byte MessageListenerHolder = 92;

        /** Type: stream receiver holder. */
        public const byte StreamReceiverHolder = 94;

        /** Type: platform object proxy. */
        public const byte PlatformJavaObjectFactoryProxy = 99;

        /** Type: object written with Java OptimizedMarshaller. */
        public const byte OptimizedMarshaller = 254;

        /** Type: Ignite UUID. */
        public const int IgniteUuid = 63;

        /** Type: T2 = IgniteBiTuple */
        public const int IgniteBiTuple = 62;

        /** Type ids. */
        private static readonly Dictionary<Type, byte> TypeIds = new Dictionary<Type, byte>
        {
            {typeof (bool), Bool},
            {typeof (byte), Byte},
            {typeof (sbyte), Byte},
            {typeof (short), Short},
            {typeof (ushort), Short},
            {typeof (char), Char},
            {typeof (int), Int},
            {typeof (uint), Int},
            {typeof (long), Long},
            {typeof (ulong), Long},
            {typeof (float), Float},
            {typeof (double), Double},
            {typeof (string), String},
            {typeof (decimal), Decimal},
            {typeof (Guid), Guid},
            {typeof (Guid?), Guid},
            {typeof (ArrayList), Collection},
            {typeof (Hashtable), Dictionary},
            {typeof (bool[]), ArrayBool},
            {typeof (byte[]), ArrayByte},
            {typeof (sbyte[]), ArrayByte},
            {typeof (short[]), ArrayShort},
            {typeof (ushort[]), ArrayShort},
            {typeof (char[]), ArrayChar},
            {typeof (int[]), ArrayInt},
            {typeof (uint[]), ArrayInt},
            {typeof (long[]), ArrayLong},
            {typeof (ulong[]), ArrayLong},
            {typeof (float[]), ArrayFloat},
            {typeof (double[]), ArrayDouble},
            {typeof (string[]), ArrayString},
            {typeof (decimal?[]), ArrayDecimal},
            {typeof (Guid?[]), ArrayGuid},
            {typeof (object[]), Array}
        };

        /// <summary>
        /// Get binary type id for a type.
        /// </summary>
        internal static byte GetTypeId(Type type)
        {
            byte res;

            if (TypeIds.TryGetValue(type, out res))
                return res;

            if (BinaryUtils.IsIgniteEnum(type))
                return Enum;

            if (type.IsArray && BinaryUtils.IsIgniteEnum(type.GetElementType()))
                return ArrayEnum;

            return Object;
        }
    }
}
