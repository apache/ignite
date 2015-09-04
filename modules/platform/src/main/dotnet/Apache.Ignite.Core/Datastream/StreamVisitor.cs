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

namespace Apache.Ignite.Core.Datastream
{
    using System;
    using System.Collections.Generic;
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

        /** <inheritdoc /> */
        public void Receive(ICache<TK, TV> cache, ICollection<ICacheEntry<TK, TV>> entries)
        {
            foreach (var entry in entries)
                _action(cache, entry);
        }
    }
}