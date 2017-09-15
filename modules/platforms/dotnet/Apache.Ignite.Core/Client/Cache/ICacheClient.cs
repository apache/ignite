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
    /// <summary>
    /// Client cache API. See <see cref="IIgniteClient.GetCache{K, V}"/>.
    /// </summary>
    public interface ICacheClient<TK, TV>
    {
        /// <summary>
        /// Name of this cache (<c>null</c> for default cache).
        /// </summary>
        string Name { get; }

        /// <summary>
        /// Ignite hosting this cache.
        /// </summary>
        IIgniteClient Ignite { get; }

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
        /// Retrieves value mapped to the specified key from cache. Throws an exception if t
        ///
        /// If the value is not present in cache, then it will be looked up from swap storage. If
        /// it's not present in swap, or if swap is disable, and if read-through is allowed, value
        /// will be loaded from persistent store.
        /// This method is transactional and will enlist the entry into ongoing transaction if there is one.
        /// If key is not present in cache, KeyNotFoundException will be thrown.
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>Value.</returns>
        TV Get(TK key);

        /// <summary>
        /// Gets or sets a cache value with the specified key.
        /// Shortcut to <see cref="Get"/> and <see cref="Put"/>
        /// </summary>
        /// <param name="key">Key.</param>
        /// <returns>Cache value with the specified key.</returns>
        TV this[TK key] { get; set; }
    }
}
