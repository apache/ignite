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

namespace Apache.Ignite.Core.Cache
{
    /// <summary>
    /// Mutable representation of <see cref="ICacheEntry{K, V}"/>
    /// </summary>
    /// <typeparam name="TK">Key type.</typeparam>
    /// <typeparam name="TV">Value type.</typeparam>
    public interface IMutableCacheEntry<out TK, TV> : ICacheEntry<TK, TV>
    {
        /// <summary>
        /// Gets a value indicating whether cache entry exists in cache.
        /// </summary>
        bool Exists { get; }

        /// <summary>
        /// Removes the entry from the Cache.
        /// </summary>
        void Remove();

        /// <summary>
        /// Gets, sets or replaces the value associated with the key.
        /// <para />
        /// If <see cref="Exists"/> is false and setter is called then a mapping is added to the cache 
        /// visible once the EntryProcessor completes.
        /// <para />
        /// After setter invocation <see cref="Exists"/> will return true.
        /// </summary>
        new TV Value { get; set; }
    }
}