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
    /// Cache entry predicate.
    /// </summary>
    /// <typeparam name="K">Type of cache entry key.</typeparam>
    /// <typeparam name="V">Type of cache entry value.</typeparam>
    public interface ICacheEntryFilter<in K, in V>
    {
        /// <summary>
        /// Returns a value indicating whether provided cache entry satisfies this predicate.
        /// </summary>
        /// <param name="entry">Cache entry.</param>
        /// <returns>Value indicating whether provided cache entry satisfies this predicate.</returns>
        bool Invoke(ICacheEntry<K, V> entry);
    }
}