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
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Utilities for binary hash codes.
    /// </summary>
    internal static class BinaryHashCodeUtils
    {
        /// <summary>
        /// Gets the Ignite-specific hash code for the provided value.
        /// </summary>
        public static unsafe int GetHashCode<T>(T val, Marshaller marsh, IDictionary<int, int> affinityKeyFieldIds)
        {
            Debug.Assert(marsh != null);
            Debug.Assert(val != null);

            var type = val.GetType();

            if (type == typeof(int))
                return TypeCaster<int>.Cast(val);

            if (type == typeof(long))
                return GetLongHashCode(TypeCaster<long>.Cast(val));

            if (type == typeof(string))
                return BinaryUtils.GetStringHashCode((string) (object) val);

            if (type == typeof(Guid))
                return GetGuidHashCode(TypeCaster<Guid>.Cast(val));

            if (type == typeof(uint))
            {
                var val0 = TypeCaster<uint>.Cast(val);
                return *(int*) &val0;
            }

            if (type == typeof(ulong))
            {
                var val0 = TypeCaster<ulong>.Cast(val);
                return GetLongHashCode(*(long*) &val0);
            }

            if (type == typeof(bool))
                return TypeCaster<bool>.Cast(val) ? 1231 : 1237;

            if (type == typeof(byte))
                return unchecked((sbyte) TypeCaster<byte>.Cast(val));

            if (type == typeof(short))
                return TypeCaster<short>.Cast(val);

            if (type == typeof(char))
                return TypeCaster<char>.Cast(val);

            if (type == typeof(float))
            {
                var floatVal = TypeCaster<float>.Cast(val);
                return *(int*) &floatVal;
            }

            if (type == typeof(double))
            {
                var doubleVal = TypeCaster<double>.Cast(val);
                return GetLongHashCode(*(long*) &doubleVal);
            }

            if (type == typeof(sbyte))
                return TypeCaster<sbyte>.Cast(val);

            if (type == typeof(ushort))
            {
                var val0 = TypeCaster<ushort>.Cast(val);
                return *(short*) &val0;
            }

            if (type == typeof(IntPtr))
            {
                var val0 = TypeCaster<IntPtr>.Cast(val).ToInt64();
                return GetLongHashCode(val0);
            }

            if (type == typeof(UIntPtr))
            {
                var val0 = TypeCaster<UIntPtr>.Cast(val).ToUInt64();
                return GetLongHashCode(*(long*) &val0);
            }

            if (type.IsArray)
            {
                return GetArrayHashCode(val, marsh, affinityKeyFieldIds);
            }

            // DateTime, when used as key, is always written as BinaryObject.
            return GetComplexTypeHashCode(val, marsh, affinityKeyFieldIds);
        }

        /// <summary>
        /// Gets the Ignite-specific hash code for an array.
        /// </summary>
        private static int GetArrayHashCode<T>(T val, Marshaller marsh, IDictionary<int, int> affinityKeyFieldIds)
        {
            var res = 1;

            var bytes = val as sbyte[];  // Matches byte[] too.

            if (bytes != null)
            {
                foreach (var x in bytes)
                    res = 31 * res + x;

                return res;
            }

            var ints = val as int[]; // Matches uint[] too.

            if (ints != null)
            {
                foreach (var x in ints)
                    res = 31 * res + x;

                return res;
            }

            var longs = val as long[]; // Matches ulong[] too.

            if (longs != null)
            {
                foreach (var x in longs)
                    res = 31 * res + GetLongHashCode(x);

                return res;
            }

            var guids = val as Guid[];

            if (guids != null)
            {
                foreach (var x in guids)
                    res = 31 * res + GetGuidHashCode(x);

                return res;
            }

            var shorts = val as short[]; // Matches ushort[] too.

            if (shorts != null)
            {
                foreach (var x in shorts)
                    res = 31 * res + x;

                return res;
            }

            var chars = val as char[];

            if (chars != null)
            {
                foreach (var x in chars)
                    res = 31 * res + x;

                return res;
            }

            // This covers all other arrays.
            // We don't have special handling for unlikely use cases such as float[] and double[].
            var arr = val as Array;

            Debug.Assert(arr != null);

            if (arr.Rank != 1)
            {
                throw new IgniteException(
                    string.Format("Failed to compute hash code for object '{0}' of type '{1}': " +
                                  "multidimensional arrays are not supported", val, val.GetType()));
            }

            foreach (var element in arr)
            {
                res = 31 * res + (element == null ? 0 : GetHashCode(element, marsh, affinityKeyFieldIds));
            }

            return res;
        }

        // ReSharper disable once ParameterOnlyUsedForPreconditionCheck.Local
        private static int GetComplexTypeHashCode<T>(T val, Marshaller marsh, IDictionary<int, int> affinityKeyFieldIds)
        {
            using (var stream = new BinaryHeapStream(128))
            {
                var writer = marsh.StartMarshal(stream);

                int? hashCode = null;

                writer.OnObjectWritten += (header, obj) =>
                {
                    if (affinityKeyFieldIds != null && affinityKeyFieldIds.ContainsKey(header.TypeId))
                    {
                        var err = string.Format(
                            "Affinity keys are not supported. Object '{0}' has an affinity key.", obj);

                        throw new IgniteException(err);
                    }

                    // In case of composite objects we need the last hash code.
                    hashCode = header.HashCode;
                };

                writer.Write(val);

                if (hashCode != null)
                {
                    // ReSharper disable once PossibleInvalidOperationException (false detection).
                    return hashCode.Value;
                }

                throw new IgniteException(
                    string.Format("Failed to compute hash code for object '{0}' of type '{1}'", val, val.GetType()));
            }
        }

        private static int GetLongHashCode(long longVal)
        {
            return (int) (longVal ^ ((longVal >> 32) & 0xFFFFFFFF));
        }

        private static unsafe int GetGuidHashCode(Guid val)
        {
            var bytes = val.ToByteArray();
            byte* jBytes = stackalloc byte[16];

            jBytes[0] = bytes[6]; // c1
            jBytes[1] = bytes[7]; // c2

            jBytes[2] = bytes[4]; // b1
            jBytes[3] = bytes[5]; // b2

            jBytes[4] = bytes[0]; // a1
            jBytes[5] = bytes[1]; // a2
            jBytes[6] = bytes[2]; // a3
            jBytes[7] = bytes[3]; // a4

            jBytes[8] = bytes[15]; // k
            jBytes[9] = bytes[14]; // j
            jBytes[10] = bytes[13]; // i
            jBytes[11] = bytes[12]; // h
            jBytes[12] = bytes[11]; // g
            jBytes[13] = bytes[10]; // f
            jBytes[14] = bytes[9]; // e
            jBytes[15] = bytes[8]; // d

            var hi = *(long*) &jBytes[0];
            var lo = *(long*) &jBytes[8];

            var hilo = hi ^ lo;

            return (int) (hilo ^ ((hilo >> 32) & 0xFFFFFFFF));
        }
    }
}
