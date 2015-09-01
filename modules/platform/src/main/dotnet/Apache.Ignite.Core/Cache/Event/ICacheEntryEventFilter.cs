/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Cache.Event
{
    /// <summary>
    /// Cache entry event filter.
    /// </summary>
    public interface ICacheEntryEventFilter<K, V>
    {
        /// <summary>
        /// Evaluates cache entry event.
        /// </summary>
        /// <param name="evt">Event.</param>
        bool Evaluate(ICacheEntryEvent<K, V> evt);
    }
}
