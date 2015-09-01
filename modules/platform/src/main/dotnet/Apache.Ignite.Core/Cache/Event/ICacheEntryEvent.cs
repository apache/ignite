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
    /// Cache entry event.
    /// </summary>
    public interface ICacheEntryEvent<K, V> : ICacheEntry<K, V>
    {
        /// <summary>
        /// Event type.
        /// </summary>
        CacheEntryEventType EventType { get; }

        /// <summary>
        /// Gets old the value.
        /// </summary>
        V OldValue { get; }

        /// <summary>
        /// Whether old value exists.
        /// </summary>
        bool HasOldValue { get; }
    }
}
