/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache
{
    /// <summary>
    /// Mutable representation of <see cref="ICacheEntry{K, V}"/>
    /// </summary>
    /// <typeparam name="K">Key type.</typeparam>
    /// <typeparam name="V">Value type.</typeparam>
    public interface IMutableCacheEntry<out K, V> : ICacheEntry<K, V>
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
        new V Value { get; set; }
    }
}