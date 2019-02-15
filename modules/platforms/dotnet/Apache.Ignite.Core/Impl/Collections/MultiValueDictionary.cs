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

namespace Apache.Ignite.Core.Impl.Collections
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Multiple-values-per-key dictionary.
    /// </summary>
    [SuppressMessage("Microsoft.Naming", "CA1711:IdentifiersShouldNotHaveIncorrectSuffix")]
    internal class MultiValueDictionary<TKey, TValue>
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