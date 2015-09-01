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
    /// Represents a result of processing <see cref="ICacheEntry{K, V}"/> 
    /// by <see cref="ICacheEntryProcessor{K, V, A, R}"/>.
    /// </summary>
    /// <typeparam name="T">Processor result type.</typeparam>
    public interface ICacheEntryProcessorResult<out T>
    {
        /// <summary>
        /// Gets the result of processing an entry.
        /// <para />
        /// If an exception was thrown during the processing of an entry, 
        /// either by the <see cref="ICacheEntryProcessor{K, V, A, R}"/> itself 
        /// or by the Caching implementation, the exceptions will be wrapped and re-thrown as a 
        /// <see cref="CacheEntryProcessorException"/> when calling this property.
        /// </summary>
        /// <value>
        /// The result.
        /// </value>
        T Result { get; }
    }
}