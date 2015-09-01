/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Cache.Store
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;

    using GridGain.Cache.Store;

    /// <summary>
    /// Store session proxy.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class CacheStoreSessionProxy : ICacheStoreSession
    {
        /** Session. */
        private readonly ThreadLocal<CacheStoreSession> target = new ThreadLocal<CacheStoreSession>();

        /** <inheritdoc /> */ 
        public string CacheName
        {
            get { return target.Value.CacheName; }
        }

        /** <inheritdoc /> */ 
        public IDictionary<object, object> Properties
        {
            get { return target.Value.Properties; }
        }

        /// <summary>
        /// Set thread-bound session.
        /// </summary>
        /// <param name="ses">Session.</param>
        internal void SetSession(CacheStoreSession ses)
        {
            target.Value = ses;
        }

        /// <summary>
        /// Clear thread-bound session.
        /// </summary>
        internal void ClearSession()
        {
            target.Value = null;
        }
    }
}
