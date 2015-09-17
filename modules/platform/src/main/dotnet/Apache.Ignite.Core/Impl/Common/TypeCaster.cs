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
    using System.Linq.Expressions;

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
        public static T Cast<TFrom>(TFrom obj)
        {
            return Casters<TFrom>.Caster(obj);
        }

        /// <summary>
        /// Inner class serving as a cache.
        /// </summary>
        private static class Casters<TFrom>
        {
            /// <summary>
            /// Compiled caster delegate.
            /// </summary>
            internal static readonly Func<TFrom, T> Caster = Compile();

            /// <summary>
            /// Compiles caster delegate.
            /// </summary>
            private static Func<TFrom, T> Compile()
            {
                if (typeof (T) == typeof (TFrom))
                {
                    // Just return what we have
                    var pExpr = Expression.Parameter(typeof(TFrom));
                    
                    return Expression.Lambda<Func<TFrom, T>>(pExpr, pExpr).Compile();
                }

                var paramExpr = Expression.Parameter(typeof(TFrom));
                var convertExpr = Expression.Convert(paramExpr, typeof(T));

                return Expression.Lambda<Func<TFrom, T>>(convertExpr, paramExpr).Compile();
            }
        }
    }
}