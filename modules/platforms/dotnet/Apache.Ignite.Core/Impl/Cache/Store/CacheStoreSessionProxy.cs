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

namespace Apache.Ignite.Core.Impl.Cache.Store
{
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using Apache.Ignite.Core.Cache.Store;

    /// <summary>
    /// Store session proxy.
    /// </summary>
    [SuppressMessage("Microsoft.Design", "CA1001:TypesThatOwnDisposableFieldsShouldBeDisposable")]
    internal class CacheStoreSessionProxy : ICacheStoreSession
    {
        /** Session. */
        private readonly ThreadLocal<CacheStoreSession> _target = new ThreadLocal<CacheStoreSession>();

        /** <inheritdoc /> */ 
        public string CacheName
        {
            get { return _target.Value.CacheName; }
        }

        /** <inheritdoc /> */ 
        public IDictionary<object, object> Properties
        {
            get { return _target.Value.Properties; }
        }

        /// <summary>
        /// Set thread-bound session.
        /// </summary>
        /// <param name="ses">Session.</param>
        internal void SetSession(CacheStoreSession ses)
        {
            _target.Value = ses;
        }

        /// <summary>
        /// Clear thread-bound session.
        /// </summary>
        internal void ClearSession()
        {
            _target.Value = null;
        }
    }
}
