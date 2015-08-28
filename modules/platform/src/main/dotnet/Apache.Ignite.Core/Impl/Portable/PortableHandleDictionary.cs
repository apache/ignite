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

namespace Apache.Ignite.Core.Impl.Portable
{
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Object handle dictionary.
    /// </summary>
    internal class PortableHandleDictionary<TKey, TValue>
    {
        /** Initial array sizes. */
        private const int InitialSize = 7;

        /** Dictionary. */
        private Dictionary<TKey, TValue> _dict;

        /** First key. */
        private readonly TKey _key1;

        /** First value. */
        private readonly TValue _val1;

        /** Second key. */
        private TKey _key2;

        /** Second value. */
        private TValue _val2;

        /** Third key. */
        private TKey _key3;

        /** Third value. */
        private TValue _val3;

        /// <summary>
        /// Constructor with initial key-value pair.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        [SuppressMessage("ReSharper", "DoNotCallOverridableMethodsInConstructor")]
        public PortableHandleDictionary(TKey key, TValue val)
        {
            Debug.Assert(!Equals(key, EmptyKey));
            
            _key1 = key;
            _val1 = val;

            _key2 = EmptyKey;
            _key3 = EmptyKey;
        }

        /// <summary>
        /// Add value to dictionary.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        public void Add(TKey key, TValue val)
        {
            Debug.Assert(!Equals(key, EmptyKey));

            if (Equals(_key2, EmptyKey))
            {
                _key2 = key;
                _val2 = val;

                return;
            }

            if (Equals(_key3, EmptyKey))
            {
                _key3 = key;
                _val3 = val;

                return;
            }

            if (_dict == null)
                _dict = new Dictionary<TKey, TValue>(InitialSize);

            _dict[key] = val;
        }

        /// <summary>
        /// Try getting value for the given key.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        /// <returns>True if key was found.</returns>
        public bool TryGetValue(TKey key, out TValue val)
        {
            Debug.Assert(!Equals(key, EmptyKey));

            if (Equals(key, _key1))
            {
                val = _val1;

                return true;
            }

            if (Equals(key, _key2))
            {
                val = _val2;

                return true;
            }

            if (Equals(key, _key3))
            {
                val = _val3;

                return true;
            }

            if (_dict == null)
            {
                val = default(TValue);

                return false;
            }

            return _dict.TryGetValue(key, out val);
        }

        /// <summary>
        /// Merge data from another dictionary without overwrite.
        /// </summary>
        /// <param name="that">Other dictionary.</param>
        public void Merge(PortableHandleDictionary<TKey, TValue> that)
        {
            Debug.Assert(that != null, "that == null");
            
            AddIfAbsent(that._key1, that._val1);
            AddIfAbsent(that._key2, that._val2);
            AddIfAbsent(that._key3, that._val3);

            if (that._dict == null)
                return;

            foreach (var pair in that._dict)
                AddIfAbsent(pair.Key, pair.Value);
        }

        /// <summary>
        /// Add key/value pair to the bucket if absent.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="val">Value.</param>
        private void AddIfAbsent(TKey key, TValue val)
        {
            if (Equals(key, EmptyKey))
                return;

            if (Equals(key, _key1) || Equals(key, _key2) || Equals(key, _key3))
                return;

            if (_dict == null || !_dict.ContainsKey(key))
                Add(key, val);
        }

        /// <summary>
        /// Gets the empty key.
        /// </summary>
        protected virtual TKey EmptyKey
        {
            get { return default(TKey); }
        }
    }
}