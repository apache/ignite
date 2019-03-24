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
    using System.Diagnostics;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Utilities for binary hash codes.
    /// </summary>
    internal static class BinaryHashCodeUtils
    {
        /// <summary>
        /// Gets the Ignite-specific hash code for the provided value.
        /// </summary>
        public static unsafe int GetHashCode<T>(T val, Marshaller marsh)
        {
            Debug.Assert(marsh != null);
            Debug.Assert(val != null);

            var type = val.GetType();

            if (type == typeof(int))
                return TypeCaster<int>.Cast(val);

            if (type == typeof(long))
                return GetLongHashCode(TypeCaster<long>.Cast(val));

            if (type == typeof(bool))
                return TypeCaster<bool>.Cast(val) ? 1231 : 1237;

            if (type == typeof(byte))
                return TypeCaster<byte>.Cast(val);

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
            {
                var val0 = TypeCaster<sbyte>.Cast(val);
                return *(byte*) &val0;
            }

            if (type == typeof(ushort))
            {
                var val0 = TypeCaster<ushort>.Cast(val);
                return *(short*) &val0;
            }

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

            // TODO: Guid, DateTime
            // TODO: Handle complex types
            return -1;
        }

        private static int GetLongHashCode(long longVal)
        {
            return (int) (longVal ^ ((longVal >> 32) & 0xFFFFFFFF));
        }
    }
}
