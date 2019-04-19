/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Impl.Collections
{
    using System.Collections.Generic;

    /// <summary>
    /// Collection extension methods.
    /// </summary>
    public static class CollectionExtensions
    {
        /// <summary>
        /// Returns a read-only System.Collections.Generic.IDictionary{K, V} wrapper for the current collection.
        /// </summary>
        public static IDictionary<TKey, TValue> AsReadOnly<TKey, TValue>(this IDictionary<TKey, TValue> dict)
        {
            return new ReadOnlyDictionary<TKey, TValue>(dict);
        }

        /// <summary>
        /// Returns a read-only System.Collections.Generic.ICollection{K, V} wrapper for the current collection.
        /// </summary>
        public static ICollection<T> AsReadOnly<T>(this ICollection<T> col)
        {
            var list = col as List<T>;

            return list != null ? (ICollection<T>) list.AsReadOnly() : new ReadOnlyCollection<T>(col);
        }
    }
}