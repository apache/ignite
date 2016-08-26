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
    using System.Data;
    using System.Data.Common;
    using System.Data.Entity.Infrastructure.Interception;
    using System.Diagnostics;

    internal class TransactionInterceptor : IDbTransactionInterceptor
    {
        /** */
        private readonly IDbCache _cache;

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionInterceptor"/> class.
        /// </summary>
        /// <param name="cache">The cache.</param>
        public TransactionInterceptor(IDbCache cache)
        {
            Debug.Assert(cache != null);

            _cache = cache;
        }

        /** <inheritDoc /> */
        public void ConnectionGetting(DbTransaction transaction, DbTransactionInterceptionContext<DbConnection> interceptionContext)
        {
            // No-op.
        }

        /** <inheritDoc /> */
        public void ConnectionGot(DbTransaction transaction, DbTransactionInterceptionContext<DbConnection> interceptionContext)
        {
            // No-op.
        }

        /** <inheritDoc /> */
        public void IsolationLevelGetting(DbTransaction transaction, DbTransactionInterceptionContext<IsolationLevel> interceptionContext)
        {
            // No-op.
        }

        /** <inheritDoc /> */
        public void IsolationLevelGot(DbTransaction transaction, DbTransactionInterceptionContext<IsolationLevel> interceptionContext)
        {
            // No-op.
        }

        /** <inheritDoc /> */
        public void Committing(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op.
        }

        /** <inheritDoc /> */
        public void Committed(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op.
        }

        /** <inheritDoc /> */
        public void Disposing(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op.
        }

        /** <inheritDoc /> */
        public void Disposed(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op.
        }

        /** <inheritDoc /> */
        public void RollingBack(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op.
        }

        /** <inheritDoc /> */
        public void RolledBack(DbTransaction transaction, DbTransactionInterceptionContext interceptionContext)
        {
            // No-op.
        }
    }
}
