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

namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.Diagnostics.CodeAnalysis;
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
                if (typeof (T) == typeof (TFrom))
                {
                    // Just return what we have
                    var pExpr = Expression.Parameter(typeof(TFrom));

                    return Expression.Lambda<Func<TFrom, T>>(pExpr, pExpr).Compile();
                }

                if (typeof(T) == typeof(UIntPtr) && typeof(TFrom) == typeof(long))
                {
                    return l => unchecked ((T) (object) (UIntPtr) (ulong) (long) (object) l);
                }

                var paramExpr = Expression.Parameter(typeof(TFrom));
                var convertExpr = Expression.Convert(paramExpr, typeof(T));

                return Expression.Lambda<Func<TFrom, T>>(convertExpr, paramExpr).Compile();
            }
        }
    }
}