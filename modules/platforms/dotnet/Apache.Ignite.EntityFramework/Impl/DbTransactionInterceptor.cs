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

namespace Apache.Ignite.EntityFramework.Impl
{
    using System.Collections.Concurrent;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Data.Entity.Core.Metadata.Edm;
    using System.Data.Entity.Infrastructure.Interception;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Intercepts transaction events.
    /// </summary>
    internal class DbTransactionInterceptor : IDbTransactionInterceptor
    {
        /** Cache. */
        private readonly DbCache _cache;

        /** Map from tx to dependent sets. HashSet because same sets can be affected multiple times within a tx. */
        private readonly ConcurrentDictionary<DbTransaction, HashSet<EntitySetBase>> _entitySets 
            = new ConcurrentDictionary<DbTransaction, HashSet<EntitySetBase>>();

        /// <summary>
        /// Initializes a new instance of the <see cref="DbTransactionInterceptor"/> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        public DbTransactionInterceptor(DbCache cache)
        {
            _cache = cache;
        }

        /** <inheritDoc /> */
        public void InvalidateCache(ICollection<EntitySetBase> entitySets, DbTransaction transaction)
        {
            if (transaction == null)
            {
                // Invalidate immediately.
                _cache.InvalidateSets(entitySets);
            }
            else
            {
                // Postpone until commit.
                var sets = _entitySets.GetOrAdd(transaction, _ => new HashSet<EntitySetBase>());

                foreach (var set in entitySets)
                    sets.Add(set);
            }
        }

        /** <inheritDoc /> */
        public void ConnectionGetting(DbTransaction transaction, DbTransactionInterceptionContext<DbConnection> interceptionContext)
        {
            // No-op
        }

        /** <inheritDoc /> */
        public void ConnectionGot(DbTransaction transaction, DbTransactionInterceptionContext<DbConnection> interceptionContext)
        {
            // No-op
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public void IsolationLevelGetting(DbTransaction transaction, DbTransactionInterceptionContext<IsolationLevel> interceptionContext)
        {
            // No-op
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public void IsolationLevelGot(DbTransaction transaction, DbTransactionInterceptionContext<IsolationLevel> interceptionContext)
        {
            // No-op
        }

        /** <inheritDoc /> */
        public void Committing(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op
        }

        /** <inheritDoc /> */
        public void Committed(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            HashSet<EntitySetBase> entitySets;
            if (_entitySets.TryGetValue(transaction, out entitySets))
                _cache.InvalidateSets(entitySets);
        }

        /** <inheritDoc /> */
        public void Disposing(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op
        }

        /** <inheritDoc /> */
        public void Disposed(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            HashSet<EntitySetBase> val;
            _entitySets.TryRemove(transaction, out val);
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public void RollingBack(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op
        }

        /** <inheritDoc /> */
        [ExcludeFromCodeCoverage]
        public void RolledBack(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op
        }
    }
}
