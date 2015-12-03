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

namespace Apache.Ignite.Core.Impl.Collections
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;

    /// <summary>
    /// Read-only wrapper over IDictionary{K, V}.
    /// </summary>
    internal struct ReadOnlyDictionary<TKey, TValue> : IDictionary<TKey, TValue>
    {
        /** Inner dict. */
        private readonly IDictionary<TKey, TValue> _dict;

        /// <summary>
        /// Initializes a new instance of the <see cref="ReadOnlyDictionary{K, V}"/> class.
        /// </summary>
        /// <param name="dict">The dictionary to wrap.</param>
        public ReadOnlyDictionary(IDictionary<TKey, TValue> dict)
        {
            Debug.Assert(dict != null);

            _dict = dict;
        }

        /** <inheritdoc /> */
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return _dict.GetEnumerator();
        }

        /** <inheritdoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable) _dict).GetEnumerator();
        }

        /** <inheritdoc /> */
        public void Add(KeyValuePair<TKey, TValue> item)
        {
            throw GetReadonlyException();
        }

        /** <inheritdoc /> */
        public void Clear()
        {
            throw GetReadonlyException();
        }

        /** <inheritdoc /> */
        public bool Contains(KeyValuePair<TKey, TValue> item)
        {
            return _dict.Contains(item);
        }

        /** <inheritdoc /> */
        public void CopyTo(KeyValuePair<TKey, TValue>[] array, int arrayIndex)
        {
            _dict.CopyTo(array, arrayIndex);
        }

        /** <inheritdoc /> */
        public bool Remove(KeyValuePair<TKey, TValue> item)
        {
            throw GetReadonlyException();
        }

        /** <inheritdoc /> */
        public int Count
        {
            get { return _dict.Count; }
        }

        /** <inheritdoc /> */
        public bool IsReadOnly
        {
            get { return true; }
        }

        /** <inheritdoc /> */
        public bool ContainsKey(TKey key)
        {
            return _dict.ContainsKey(key);
        }

        /** <inheritdoc /> */
        public void Add(TKey key, TValue value)
        {
            throw GetReadonlyException();
        }

        /** <inheritdoc /> */
        public bool Remove(TKey key)
        {
            return _dict.Remove(key);
        }

        /** <inheritdoc /> */
        public bool TryGetValue(TKey key, out TValue value)
        {
            return _dict.TryGetValue(key, out value);
        }

        /** <inheritdoc /> */
        public TValue this[TKey key]
        {
            get { return _dict[key]; }
            set { throw GetReadonlyException(); }
        }

        /** <inheritdoc /> */
        public ICollection<TKey> Keys
        {
            get { return _dict.Keys; }
        }

        /** <inheritdoc /> */
        public ICollection<TValue> Values
        {
            get { return _dict.Values; }
        }

        /// <summary>
        /// Gets the readonly exception.
        /// </summary>
        private static Exception GetReadonlyException()
        {
            return new NotSupportedException("Dictionary is read-only.");
        }
    }
}
