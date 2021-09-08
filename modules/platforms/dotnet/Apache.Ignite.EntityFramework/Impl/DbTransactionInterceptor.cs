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
            // Stored procedure is used, nothing to invalidate.
            if (entitySets == null)
                return;

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
