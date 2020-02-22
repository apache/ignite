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

namespace Apache.Ignite.Core.Impl.Cache.Near
{
    using System;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Non-generic near cache facade.
    /// </summary>
    internal interface INearCache
    {
        /// <summary>
        /// Gets a value indicating whether near cache has been stopped.
        /// Happens during normal cache destroy, or after client reconnect with full cluster restart.
        /// </summary>
        bool IsStopped { get; }

        /// <summary>
        /// Reads cache key and value from a stream and updates near cache.
        /// </summary>
        void Update(IBinaryStream stream, Marshaller marshaller);

        /// <summary>
        /// Stops the cache, enters bypass mode.
        /// </summary>
        void Stop();

        bool TryGetValue<TKey, TVal>(TKey key, out TVal val);
        
        TVal GetOrAdd<TKey, TVal>(TKey key, Func<TKey, TVal> valueFactory);
        
        TVal GetOrAdd<TKey, TVal>(TKey key, TVal val);
        
        /// <summary>
        /// Gets the size.
        /// </summary>
        int GetSize();

        bool ContainsKey<TKey, TVal>(TKey key);

        /// <summary>
        /// Removes all mappings from the cache.
        /// </summary>
        void Clear();
    }
}