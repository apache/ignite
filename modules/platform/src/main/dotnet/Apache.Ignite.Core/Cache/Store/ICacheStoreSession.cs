/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Cache.Store
{
    using System.Collections.Generic;

    /// <summary>
    /// Session for the cache store operations. The main purpose of cache store session
    /// is to hold context between multiple store invocations whenever in transaction. For example,
    /// you can save current database connection in the session <see cref="Properties"/> map. You can then
    /// commit this connection in the <see cref="ICacheStore.SessionEnd(bool)"/> method.
    /// </summary>
    public interface ICacheStoreSession
    {
        /// <summary>
        /// Cache name for the current store operation. Note that if the same store
        /// is reused between different caches, then the cache name will change between
        /// different store operations.
        /// </summary>
        string CacheName { get; }

        /// <summary>
        /// Current session properties. You can add properties directly to the returned map.
        /// </summary>
        IDictionary<object, object> Properties { get; }
    }
}
