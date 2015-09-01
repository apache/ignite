/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Cache
{
    /// <summary>
    /// An invocable function that allows applications to perform compound operations
    /// on a cache entry atomically, according the defined consistency of a cache.
    /// <para />
    /// Any cache entry mutations will not take effect until after
    /// the <see cref="Process" /> method has completedS execution.
    /// <para />
    /// If an exception is thrown by an entry processor, a Caching Implementation
    /// must wrap any exception thrown wrapped in an <see cref="CacheEntryProcessorException" />
    /// If this occurs no mutations will be made to the cache entry.
    /// </summary>
    /// <typeparam name="K">The type of the cache key.</typeparam>
    /// <typeparam name="V">The type of the cache value.</typeparam>
    /// <typeparam name="A">The type of the processor argument.</typeparam>
    /// <typeparam name="R">The type of the processor result.</typeparam>
    public interface ICacheEntryProcessor<in K, V, in A, out R>
    {
        /// <summary>
        /// Process an entry.
        /// </summary>
        /// <param name="entry">The entry to process.</param>
        /// <param name="arg">The argument.</param>
        /// <returns>Processing result.</returns>
        R Process(IMutableCacheEntry<K, V> entry, A arg);
    }
}