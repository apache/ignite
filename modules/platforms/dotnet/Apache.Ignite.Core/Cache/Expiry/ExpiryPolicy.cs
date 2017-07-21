/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Cache.Expiry
{
    using System;

    /// <summary>
    /// Default expiry policy implementation with all durations deinfed explicitly.
    /// </summary>
    public class ExpiryPolicy : IExpiryPolicy
    {
        /** Expiry for create. */
        private readonly TimeSpan? _create;

        /** Expiry for update. */
        private readonly TimeSpan? _update;

        /** Expiry for access. */
        private readonly TimeSpan? _access;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="create">Expiry for create.</param>
        /// <param name="update">Expiry for udpate.</param>
        /// <param name="access">Expiry for access.</param>
        public ExpiryPolicy(TimeSpan? create, TimeSpan? update, TimeSpan? access)
        {
            _create = create;
            _update = update;
            _access = access;
        }

        /// <summary>
        /// Gets expiry for create operation.
        /// <para />
        /// If <c>TimeSpan.ZERO</c> is returned, cache entry is considered immediately expired
        /// and will not be added to cache. 
        /// <para />
        /// If <c>null</c> is returned, no change to previously understood expiry is performed.
        /// </summary>
        /// <returns>Expiry for create opeartion.</returns>
        public TimeSpan? GetExpiryForCreate()
        {
            return _create;
        }

        /// <summary>
        /// Gets expiry for update operation.
        /// <para />
        /// If <c>TimeSpan.ZERO</c> is returned, cache entry is considered immediately expired.
        /// <para />
        /// If <c>null</c> is returned, no change to previously understood expiry is performed.
        /// </summary>
        /// <returns>Expiry for update operation.</returns>
        public TimeSpan? GetExpiryForUpdate()
        {
            return _update;
        }

        /// <summary>
        /// Gets expiry for access operation.
        /// <para />
        /// If <c>TimeSpan.ZERO</c> is returned, cache entry is considered immediately expired.
        /// <para />
        /// If <c>null</c> is returned, no change to previously understood expiry is performed.
        /// </summary>
        /// <returns>Expiry for access operation.</returns>
        public TimeSpan? GetExpiryForAccess()
        {
            return _access;
        }
    }
}
