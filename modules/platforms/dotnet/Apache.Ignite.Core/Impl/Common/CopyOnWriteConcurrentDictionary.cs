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

// ReSharper disable InconsistentlySynchronizedField
namespace Apache.Ignite.Core.Impl.Common
{
    using System;
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Concurrent dictionary with CopyOnWrite mechanism inside. 
    /// Good for frequent reads / infrequent writes scenarios.
    /// </summary>
    [SuppressMessage("Microsoft.Naming", "CA1711:IdentifiersShouldNotHaveIncorrectSuffix")]
    [SuppressMessage("Microsoft.Naming", "CA1710:IdentifiersShouldHaveCorrectSuffix")]
    public class CopyOnWriteConcurrentDictionary<TKey, TValue> : IEnumerable<KeyValuePair<TKey, TValue>>
    {
        /** */
        private volatile Dictionary<TKey, TValue> _dict = new Dictionary<TKey, TValue>();

        /// <summary>
        /// Gets the value associated with the specified key.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="val">The value.</param>
        /// <returns>true if the dictionary contains an element with the specified key; otherwise, false.</returns>
        public bool TryGetValue(TKey key, out TValue val)
        {
            return _dict.TryGetValue(key, out val);
        }

        /// <summary>
        /// Adds a key/value pair if the key does not already exist.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="valueFactory">The function used to generate a value for the key.</param>
        /// <returns>The value for the key.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public TValue GetOrAdd(TKey key, Func<TKey, TValue> valueFactory)
        {
            lock (this)
            {
                TValue res;

                if (_dict.TryGetValue(key, out res))
                    return res;

                var dict0 = new Dictionary<TKey, TValue>(_dict);

                res = valueFactory(key);

                dict0[key] = res;

                _dict = dict0;

                return res;
            }
        }

        /// <summary>
        /// Sets a value for the key unconditionally.
        /// </summary>
        /// <param name="key">The key.</param>
        /// <param name="value">The value.</param>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public void Set(TKey key, TValue value)
        {
            lock (this)
            {
                var dict0 = new Dictionary<TKey, TValue>(_dict);

                dict0[key] = value;

                _dict = dict0;
            }
        }
        
        /** <inheritDoc /> */
        public IEnumerator<KeyValuePair<TKey, TValue>> GetEnumerator()
        {
            return _dict.GetEnumerator();
        }

        /** <inheritDoc /> */
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
    }
}