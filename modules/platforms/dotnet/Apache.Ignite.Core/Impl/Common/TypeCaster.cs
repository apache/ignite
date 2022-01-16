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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using System.Linq.Expressions;
    using System.Reflection;

    /// <summary>
    /// Does type casts without extra boxing. 
    /// Should be used when casting compile-time incompatible value types instead of "(T)(object)x".
    /// </summary>
    /// <typeparam name="T">Target type</typeparam>
    public static class TypeCaster<T>
    {
        /// <summary>
        /// Efficiently casts an object from TFrom to T.
        /// Does not cause boxing for value types.
        /// </summary>
        /// <typeparam name="TFrom">Source type to cast from.</typeparam>
        /// <param name="obj">The object to cast.</param>
        /// <returns>Casted object.</returns>
        [SuppressMessage("Microsoft.Design", "CA1000:DoNotDeclareStaticMembersOnGenericTypes",
            Justification = "Intended usage to leverage compiler caching.")]
        public static T Cast<TFrom>(TFrom obj)
        {
#if (DEBUG)
            try
            {
                return Casters<TFrom>.Caster(obj);
            }
            catch (InvalidCastException e)
            {
                throw new InvalidCastException(string.Format("Specified cast is not valid: {0} -> {1}", typeof (TFrom),
                    typeof (T)), e);
            }
#else
            return Casters<TFrom>.Caster(obj);
#endif
        }

        /// <summary>
        /// Inner class serving as a cache.
        /// </summary>
        private static class Casters<TFrom>
        {
            /// <summary>
            /// Compiled caster delegate.
            /// </summary>
            [SuppressMessage("Microsoft.Performance", "CA1823:AvoidUnusedPrivateFields", 
                Justification = "Incorrect warning")]
            [SuppressMessage("Microsoft.Design", "CA1000:DoNotDeclareStaticMembersOnGenericTypes",
                Justification = "Intended usage to leverage compiler caching.")]
            internal static readonly Func<TFrom, T> Caster = Compile();

            /// <summary>
            /// Compiles caster delegate.
            /// </summary>
            private static Func<TFrom, T> Compile()
            {
                var fromType = typeof(TFrom);
                var toType = typeof(T);
                
                var fromParamExpr = Expression.Parameter(fromType);

                if (toType == fromType)
                {
                    // Just return what we have
                    return Expression.Lambda<Func<TFrom, T>>(fromParamExpr, fromParamExpr).Compile();
                }

                if (toType == typeof(UIntPtr) && fromType == typeof(long))
                {
                    return l => unchecked((T) (object) (UIntPtr) (ulong) (long) (object) l);
                }

                Expression convertExpr = null;

                if (fromType == typeof(Enum))
                    convertExpr = TryConvertRawEnum(toType, fromParamExpr);
                
                if (convertExpr == null)
                    convertExpr = Expression.Convert(fromParamExpr, toType);

                return Expression.Lambda<Func<TFrom, T>>(convertExpr, fromParamExpr).Compile();
            }

            private static Expression TryConvertRawEnum(Type toType, Expression fromParamExpr)
            {
                string mtdName = null;
                
                if (toType == typeof(byte))
                    mtdName = "ToByte";
                else if (toType == typeof(sbyte))
                    mtdName = "ToSByte";
                else if (toType == typeof(short))
                    mtdName = "ToInt16";
                else if (toType == typeof(ushort))
                    mtdName = "ToUInt16";
                else if (toType == typeof(int))
                    mtdName = "ToInt32";
                else if (toType == typeof(uint))
                    mtdName = "ToUInt32";

                if (mtdName == null) return null;
                
                MethodInfo toIntMtd = typeof(Convert).GetMethod(mtdName, new[] {typeof(object)});
                
                Debug.Assert(toIntMtd != null);

                return Expression.Call(null, toIntMtd, fromParamExpr);
            }
        }
        
        private enum ByteEnum : byte
        {
            Foo = byte.MinValue,
            Bar = byte.MaxValue
        }
        
        private enum SByteEnum : sbyte
        {
            Foo = sbyte.MinValue,
            Bar = sbyte.MaxValue
        }

        private enum ShortEnum : short
        {
            Foo = short.MinValue,
            Bar = short.MaxValue
        }

        private enum UShortEnum : ushort
        {
            Foo = ushort.MinValue,
            Bar = ushort.MaxValue
        }

        private enum IntEnum
        {
            Foo = int.MinValue,
            Bar = int.MaxValue
        }

        private enum UIntEnum : uint
        {
            Foo = uint.MinValue,
            Bar = uint.MaxValue
        }

        private enum LongEnum : long
        {
            Foo = long.MinValue,
            Bar = long.MaxValue
        }

        private enum ULongEnum : ulong
        {
            Foo = ulong.MinValue,
            Bar = ulong.MaxValue
        }
        
        /// <summary>
        /// Holds enums declared as System.Enum.
        /// </summary>
        #pragma warning disable 659
        private class EnumsHolder
        {
            private Enum EnmByteRaw { get; set; } = ByteEnum.Bar;
            private Enum EnmUByteRaw { get; set; } = SByteEnum.Foo;
            private Enum EnmShortRaw { get; set; } = ShortEnum.Bar;
            private Enum EnmUShortRaw { get; set; } = UShortEnum.Foo;
            private Enum EnmIntRaw { get; set; } = IntEnum.Bar;
            private Enum EnmUIntRaw { get; set; } = UIntEnum.Foo;
            private Enum EnmLongRaw { get; set; } = LongEnum.Bar;
            private Enum EnmULongRaw { get; set; } = ULongEnum.Bar;

            private Enum[] EnmRawArr { get; set; } = new Enum[]
            {
                ByteEnum.Bar, SByteEnum.Foo, ShortEnum.Bar,
                UShortEnum.Foo, IntEnum.Bar, UIntEnum.Foo, LongEnum.Bar, ULongEnum.Foo
            };

            public override bool Equals(object obj)
            {
                if (ReferenceEquals(null, obj)) return false;
                if (ReferenceEquals(this, obj)) return true;
                if (obj.GetType() != this.GetType()) return false;

                var other = (EnumsHolder) obj;

                return Equals(EnmByteRaw, other.EnmByteRaw) && Equals(EnmUByteRaw, other.EnmUByteRaw) &&
                       Equals(EnmShortRaw, other.EnmShortRaw) && Equals(EnmUShortRaw, other.EnmUShortRaw) &&
                       Equals(EnmIntRaw, other.EnmIntRaw) && Equals(EnmUIntRaw, other.EnmUIntRaw) &&
                       Equals(EnmLongRaw, other.EnmLongRaw) && Equals(EnmULongRaw, other.EnmULongRaw) &&
                       Enumerable.SequenceEqual(EnmRawArr, other.EnmRawArr);
            }
        }
    }
}