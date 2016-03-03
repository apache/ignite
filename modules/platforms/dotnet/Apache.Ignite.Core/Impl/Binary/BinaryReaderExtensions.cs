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
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Reader extensions.
    /// </summary>
    internal static class BinaryReaderExtensions
    {
        /// <summary>
        /// Reads untyped collection as a generic list.
        /// </summary>
        /// <typeparam name="T">Type of list element.</typeparam>
        /// <param name="reader">The reader.</param>
        /// <returns>Resulting generic list.</returns>
        public static List<T> ReadCollectionAsList<T>(this IBinaryRawReader reader)
        {
            return ((List<T>) reader.ReadCollection(size => new List<T>(size),
                (col, elem) => ((List<T>) col).Add((T) elem)));
        }

        /// <summary>
        /// Reads untyped dictionary as generic dictionary.
        /// </summary>
        /// <typeparam name="TKey">The type of the key.</typeparam>
        /// <typeparam name="TValue">The type of the value.</typeparam>
        /// <param name="reader">The reader.</param>
        /// <returns>Resulting dictionary.</returns>
        public static Dictionary<TKey, TValue> ReadDictionaryAsGeneric<TKey, TValue>(this IBinaryRawReader reader)
        {
            return (Dictionary<TKey, TValue>) reader.ReadDictionary(size => new Dictionary<TKey, TValue>(size));
        }

        /// <summary>
        /// Reads long as timespan with range checks.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <returns>TimeSpan.</returns>
        public static TimeSpan ReadLongAsTimespan(this IBinaryRawReader reader)
        {
            long ms = reader.ReadLong();

            if (ms >= TimeSpan.MaxValue.TotalMilliseconds)
                return TimeSpan.MaxValue;

            if (ms <= TimeSpan.MinValue.TotalMilliseconds)
                return TimeSpan.MinValue;

            return TimeSpan.FromMilliseconds(ms);
        }
    }
}
