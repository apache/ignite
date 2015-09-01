/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Cache.Event
{
    /// <summary>
    /// Cache event type.
    /// </summary>
    public enum CacheEntryEventType
    {
        /// <summary>
        /// An event type indicating that the cache entry was created.
        /// </summary>
        CREATED,

        /// <summary>
        /// An event type indicating that the cache entry was updated. i.e. a previous
        /// mapping existed.
        /// </summary>
        UPDATED,

        /// <summary>
        /// An event type indicating that the cache entry was removed.
        /// </summary>
        REMOVED
    }
}