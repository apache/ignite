/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
