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

namespace Apache.Ignite.Core.Datastream
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Convenience adapter to visit every key-value tuple in the stream.
    /// Note that the visitor does not update the cache.
    /// </summary>
    /// <typeparam name="TK">The type of the cache key.</typeparam>
    /// <typeparam name="TV">The type of the cache value.</typeparam>
    [Serializable]
    public sealed class StreamVisitor<TK, TV> : IStreamReceiver<TK, TV>
    {
        /** Visitor action */
        private readonly Action<ICache<TK, TV>, ICacheEntry<TK, TV>> _action;

        /// <summary>
        /// Initializes a new instance of the <see cref="StreamVisitor{K, V}"/> class.
        /// </summary>
        /// <param name="action">The action to be called on each stream entry.</param>
        public StreamVisitor(Action<ICache<TK, TV>, ICacheEntry<TK, TV>> action)
        {
            IgniteArgumentCheck.NotNull(action, "action");

            _action = action;
        }

        /// <summary>
        /// Updates cache with batch of entries.
        /// </summary>
        /// <param name="cache">Cache.</param>
        /// <param name="entries">Entries.</param>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        public void Receive(ICache<TK, TV> cache, ICollection<ICacheEntry<TK, TV>> entries)
        {
            foreach (var entry in entries)
                _action(cache, entry);
        }
    }
}