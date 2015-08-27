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
    using System.Collections.Generic;

    /// <summary>
    /// Multiple-values-per-key dictionary.
    /// </summary>
    public class MultiValueDictionary<TKey, TValue>
    {
        /** Inner dictionary */
        private readonly Dictionary<TKey, object> _dict = new Dictionary<TKey, object>();

        /// <summary>
        /// Adds a value.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="val">The value.</param>
        public void Add(TKey key, TValue val)
        {
            object val0;

            if (_dict.TryGetValue(key, out val0))
            {
                var list = val0 as List<TValue>;

                if (list != null)
                    list.Add(val);
                else
                    _dict[key] = new List<TValue> {(TValue) val0, val};
            }
            else
                _dict[key] = val;
        }

        /// <summary>
        /// Tries the get a value. In case of multiple values for a key, returns the last one.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="val">The value.</param>
        /// <returns>True if value has been found for specified key; otherwise false.</returns>
        public bool TryGetValue(TKey key, out TValue val)
        {
            object val0;
            
            if (!_dict.TryGetValue(key, out val0))
            {
                val = default(TValue);
                return false;
            }

            var list = val0 as List<TValue>;

            if (list != null)
                val = list[list.Count - 1];
            else
                val = (TValue) val0;

            return true;
        }

        /// <summary>
        /// Removes the specified value for the specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="val">The value.</param>
        public void Remove(TKey key, TValue val)
        {
            object val0;

            if (!_dict.TryGetValue(key, out val0))
                return;

            var list = val0 as List<TValue>;

            if (list != null)
            {
                list.Remove(val);

                if (list.Count == 0)
                    _dict.Remove(key);
            }
            else if (Equals(val0, val))
                _dict.Remove(key);
        }

        /// <summary>
        /// Removes the last value for the specified key and returns it.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="val">The value.</param>
        /// <returns>True if value has been found for specified key; otherwise false.</returns>
        public bool TryRemove(TKey key, out TValue val)
        {
            object val0;

            if (!_dict.TryGetValue(key, out val0))
            {
                val = default(TValue);

                return false;
            }

            var list = val0 as List<TValue>;

            if (list != null)
            {
                var index = list.Count - 1;

                val = list[index];

                list.RemoveAt(index);

                if (list.Count == 0)
                    _dict.Remove(key);

                return true;
            }
            
            val = (TValue) val0;

            _dict.Remove(key);

            return true;
        }
    }
}