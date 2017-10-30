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

namespace Apache.Ignite.Core.Client.Cache
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Cache;
    using Apache.Ignite.Core.Cache.Configuration;
    using Apache.Ignite.Core.Cache.Query;

    /// <summary>
    /// Client cache API. See <see cref="IIgniteClient.GetCache{K, V}"/>.
    /// </summary>
    // ReSharper disable once TypeParameterCanBeVariant (ICache shoul not be variant, more methods will be added)
    public interface ICacheClient<TK, TV>
    {
        /// <summary>
        /// Name of this cache (<c>null</c> for default cache).
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Associates the specified value with the specified key in the cache.
        /// <para />
        /// If the cache previously contained a mapping for the key,
        /// the old value is replaced by the specified value.
        /// </summary>
        /// <param name="key">Key with which the specified value is to be associated.</param>
        /// <param name="val">Value to be associated with the specified key.</param>
        void Put(TK key, TV val);

        /// <summary>
        /// Retrieves value mapped to the specified key from cache.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>Value.</returns>
        /// <exception cref="KeyNotFoundException">If the key is not present in the cache.</exception>
        TV Get(TK key);

        /// <summary>
        /// Retrieves value mapped to the specified key from cache.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <param name="value">When this method returns, the value associated with the specified key,
        /// if the key is found; otherwise, the default value for the type of the value parameter.
        /// This parameter is passed uninitialized.</param>
        /// <returns>
        /// true if the cache contains an element with the specified key; otherwise, false.
        /// </returns>
        bool TryGet(TK key, out TV value);

        /// <summary>
        /// Retrieves values mapped to the specified keys from cache.
        /// </summary>
        /// <param name="keys">Keys.</param>
        /// <returns>Map of key-value pairs.</returns>
        ICollection<ICacheEntry<TK, TV>> GetAll(IEnumerable<TK> keys);

        /// <summary>
        /// Gets or sets a cache value with the specified key.
        /// Shortcut to <see cref="Get"/> and <see cref="Put"/>
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>Cache value with the specified key.</returns>
        /// <exception cref="KeyNotFoundException">If the key is not present in the cache.</exception>
        TV this[TK key] { get; set; }

        /// <summary>
        /// Check if cache contains mapping for this key.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>True if cache contains mapping for this key.</returns>
        bool ContainsKey(TK key);

        /// <summary>
        /// Check if cache contains mapping for these keys.
        /// </summary>
        /// <param name="keys">Keys.</param>
        /// <returns>True if cache contains mapping for all these keys.</returns>
        bool ContainsKeys(IEnumerable<TK> keys);

        /// <summary>
        /// Executes a Scan query.
        /// </summary>
        /// <param name="scanQuery">Scan query.</param>
        /// <returns>Query cursor.</returns>
        IQueryCursor<ICacheEntry<TK, TV>> Query(ScanQuery<TK, TV> scanQuery);

        /// <summary>
        /// Associates the specified value with the specified key in this cache,
        /// returning an existing value if one existed.
        /// </summary>
        /// <param name="key">Key with which the specified value is to be associated.</param>
        /// <param name="val">Value to be associated with the specified key.</param>
        /// <returns>
        /// The value associated with the key at the start of the operation.
        /// </returns>
        CacheResult<TV> GetAndPut(TK key, TV val);

        /// <summary>
        /// Atomically replaces the value for a given key if and only if there is a value currently mapped by the key.
        /// </summary>
        /// <param name="key">Key with which the specified value is to be associated.</param>
        /// <param name="val">Value to be associated with the specified key.</param>
        /// <returns>
        /// The previous value associated with the specified key.
        /// </returns>
        CacheResult<TV> GetAndReplace(TK key, TV val);

        /// <summary>
        /// Atomically removes the entry for a key only if currently mapped to some value.
        /// </summary>
        /// <param name="key">Key with which the specified value is associated.</param>
        /// <returns>The value if one existed.</returns>
        CacheResult<TV> GetAndRemove(TK key);

        /// <summary>
        /// Atomically associates the specified key with the given value if it is not already associated with a value.
        /// </summary>
        /// <param name="key">Key with which the specified value is to be associated.</param>
        /// <param name="val">Value to be associated with the specified key.</param>
        /// <returns>True if a value was set.</returns>
        bool PutIfAbsent(TK key, TV val);

        /// <summary>
        /// Stores given key-value pair in cache only if cache had no previous mapping for it.
        /// </summary>
        /// <param name="key">Key to store in cache.</param>
        /// <param name="val">Value to be associated with the given key.</param>
        /// <returns>
        /// Previously contained value regardless of whether put happened or not.
        /// </returns>
        CacheResult<TV> GetAndPutIfAbsent(TK key, TV val);

        /// <summary>
        /// Stores given key-value pair in cache only if there is a previous mapping for it.
        /// </summary>
        /// <param name="key">Key to store in cache.</param>
        /// <param name="val">Value to be associated with the given key.</param>
        /// <returns>True if the value was replaced.</returns>
        bool Replace(TK key, TV val);

        /// <summary>
        /// Stores given key-value pair in cache only if only if the previous value is equal to the
        /// old value passed as argument.
        /// </summary>
        /// <param name="key">Key to store in cache.</param>
        /// <param name="oldVal">Old value to match.</param>
        /// <param name="newVal">Value to be associated with the given key.</param>
        /// <returns>True if replace happened, false otherwise.</returns>
        bool Replace(TK key, TV oldVal, TV newVal);

        /// <summary>
        /// Stores given key-value pairs in cache.
        /// </summary>
        /// <param name="vals">Key-value pairs to store in cache.</param>
        void PutAll(IEnumerable<KeyValuePair<TK, TV>> vals);

        /// <summary>
        /// Clears the contents of the cache, without notifying listeners or CacheWriters.
        /// </summary>
        void Clear();

        /// <summary>
        /// Clear entry from the cache, without notifying listeners or CacheWriters.
        /// </summary>
        /// <param name="key">Key to clear.</param>
        void Clear(TK key);

        /// <summary>
        /// Clear entries from the cache, without notifying listeners or CacheWriters.
        /// </summary>
        /// <param name="keys">Keys to clear.</param>
        void ClearAll(IEnumerable<TK> keys);

        /// <summary>
        /// Removes given key mapping from cache, notifying listeners and cache writers.
        /// </summary>
        /// <param name="key">Key to remove.</param>
        /// <returns>True if entry was removed, false otherwise.</returns>
        bool Remove(TK key);

        /// <summary>
        /// Removes given key mapping from cache if one exists and value is equal to the passed in value.
        /// </summary>
        /// <param name="key">Key whose mapping is to be removed from cache.</param>
        /// <param name="val">Value to match against currently cached value.</param>
        /// <returns>True if entry was removed, false otherwise.</returns>
        bool Remove(TK key, TV val);

        /// <summary>
        /// Removes given key mappings from cache, notifying listeners and cache writers.
        /// </summary>
        /// <param name="keys">Keys to be removed from cache.</param>
        void RemoveAll(IEnumerable<TK> keys);

        /// <summary>
        /// Removes all mappings from cache, notifying listeners and cache writers.
        /// </summary>
        void RemoveAll();

        /// <summary>
        /// Gets the number of all entries cached across all nodes.
        /// <para />
        /// NOTE: this operation is distributed and will query all participating nodes for their cache sizes.
        /// </summary>
        /// <param name="modes">Optional peek modes. If not provided, then total cache size is returned.</param>
        /// <returns>Cache size across all nodes.</returns>
        long GetSize(params CachePeekMode[] modes);

        /// <summary>
        /// Gets the cache configuration.
        /// </summary>
        CacheClientConfiguration GetConfiguration();
    }
}
