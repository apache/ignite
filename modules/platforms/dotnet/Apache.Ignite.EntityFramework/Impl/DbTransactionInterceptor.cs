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
    using System.Collections.Generic;
    using System.Data;
    using System.Data.Common;
    using System.Data.Entity.Core.Metadata.Edm;
    using System.Data.Entity.Infrastructure.Interception;

    /// <summary>
    /// Intercepts transaction events.
    /// </summary>
    internal class DbTransactionInterceptor : IDbTransactionInterceptor
    {
        /** */
        private readonly DbCache _cache;

        /// <summary>
        /// Initializes a new instance of the <see cref="DbTransactionInterceptor"/> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        public DbTransactionInterceptor(DbCache cache)
        {
            _cache = cache;
        }

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
                // TODO:
                
            }
        }

        public void ConnectionGetting(DbTransaction transaction, DbTransactionInterceptionContext<DbConnection> interceptionContext)
        {
            // No-op
        }

        public void ConnectionGot(DbTransaction transaction, DbTransactionInterceptionContext<DbConnection> interceptionContext)
        {
            // No-op
        }

        public void IsolationLevelGetting(DbTransaction transaction, DbTransactionInterceptionContext<IsolationLevel> interceptionContext)
        {
            // No-op
        }

        public void IsolationLevelGot(DbTransaction transaction, DbTransactionInterceptionContext<IsolationLevel> interceptionContext)
        {
            // No-op
        }

        public void Committing(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op
        }

        public void Committed(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // TODO: This is called on SaveChanges, this is where we have to invalidate cache! 
            // EFCache implementation seems to be correct.
        }

        public void Disposing(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op
        }

        public void Disposed(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op
        }

        public void RollingBack(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op
        }

        public void RolledBack(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op
        }
    }
}
