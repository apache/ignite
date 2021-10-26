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

        /** Type: binary enum. */
        public const byte IBinaryObject = 39;

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
        private static readonly Dictionary<Type, byte> TypeIds = new Dictionary<Type, byte>();

        /** Type ids. */
        private static readonly Dictionary<byte, Type> IdToType = new Dictionary<byte, Type>();

        static BinaryTypeId()
        {
            void Add(Type type, byte typeId)
            {
                TypeIds.Add(type, typeId);

                if (!IdToType.ContainsKey(typeId))
                    IdToType.Add(typeId, type);
            }

            Add(typeof (bool), Bool);
            Add(typeof (byte), Byte);
            Add(typeof (sbyte), Byte);
            Add(typeof (short), Short);
            Add(typeof (ushort), Short);
            Add(typeof (char), Char);
            Add(typeof (int), Int);
            Add(typeof (uint), Int);
            Add(typeof (long), Long);
            Add(typeof (ulong), Long);
            Add(typeof (float), Float);
            Add(typeof (double), Double);
            Add(typeof (string), String);
            Add(typeof (decimal), Decimal);
            Add(typeof (Guid), Guid);
            Add(typeof (Guid?), Guid);
            Add(typeof(DateTime), Timestamp);
            Add(typeof(DateTime?), Timestamp);
            Add(typeof (ArrayList), Collection);
            Add(typeof (Hashtable), Dictionary);
            Add(typeof (bool[]), ArrayBool);
            Add(typeof (byte[]), ArrayByte);
            Add(typeof (sbyte[]), ArrayByte);
            Add(typeof (short[]), ArrayShort);
            Add(typeof (ushort[]), ArrayShort);
            Add(typeof (char[]), ArrayChar);
            Add(typeof (int[]), ArrayInt);
            Add(typeof (uint[]), ArrayInt);
            Add(typeof (long[]), ArrayLong);
            Add(typeof (ulong[]), ArrayLong);
            Add(typeof (float[]), ArrayFloat);
            Add(typeof (double[]), ArrayDouble);
            Add(typeof (string[]), ArrayString);
            Add(typeof (decimal?[]), ArrayDecimal);
            Add(typeof (Guid?[]), ArrayGuid);
            Add(typeof (Guid[]), ArrayGuid);
            Add(typeof (DateTime?[]), ArrayTimestamp);
            Add(typeof (DateTime[]), ArrayTimestamp);
            Add(typeof (object[]), Array);
        }

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

        /// <summary>
        /// Get binary type id for a type.
        /// </summary>
        internal static Type GetType(byte typeId)
        {
            Type res;

            if (IdToType.TryGetValue(typeId, out res))
                return res;

            return null;
        }
    }
}
