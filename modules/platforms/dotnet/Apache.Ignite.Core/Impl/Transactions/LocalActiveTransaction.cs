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

namespace Apache.Ignite.Core.Impl.Transactions
{
    using System;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Rollback only view of local active transaction.
    /// </summary>
    internal class LocalActiveTransaction : ITransaction
    {
        /** Transactions facade. */
        private readonly TransactionsImpl _txs;

        /** Unique transaction view ID.*/
        private readonly long _id;

        public LocalActiveTransaction(
            TransactionsImpl txs, 
            long id,
            TransactionConcurrency concurrency,
            TransactionIsolation isolation,
            TimeSpan timeout,
            string label,
            Guid nodeId)
        {
            _txs = txs;
            _id = id;
            NodeId = nodeId;
            Isolation = isolation;
            Concurrency = concurrency;
            Timeout = timeout;
            Label = label;
        }

        public Guid NodeId { get; private set; }

        public long ThreadId
        {
            get { throw GetInvalidOperationException(); }
        }

        public DateTime StartTime
        {
            get { throw  GetInvalidOperationException(); }
        }

        public TransactionIsolation Isolation { get; private set; }
        public TransactionConcurrency Concurrency { get; private set; }
        public TransactionState State 
        {
            get { throw  GetInvalidOperationException(); }
        }
        public TimeSpan Timeout { get; private set; }
        public string Label { get; private set; }
        public bool IsRollbackOnly
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public bool SetRollbackonly()
        {
            throw new InvalidOperationException();
        }

        /** <inheritDoc /> */
        public void Commit()
        {
            throw new InvalidOperationException();
        }

        /** <inheritDoc /> */
        public Task CommitAsync()
        {
            throw new InvalidOperationException();
        }

        /** <inheritDoc /> */
        public void Rollback()
        {
            throw new InvalidOperationException();
        }

        /** <inheritDoc /> */
        public Task RollbackAsync()
        {
            throw new InvalidOperationException();
        }

        /** <inheritDoc /> */
        public void AddMeta<TV>(string name, TV val)
        {
            throw new InvalidOperationException();
        }

        public TV Meta<TV>(string name)
        {
            throw new InvalidOperationException();
        }

        /** <inheritDoc /> */
        public TV RemoveMeta<TV>(string name)
        {
            throw new InvalidOperationException();
        }
        
        /** <inheritDoc /> */
        public void Dispose()
        {
            // No-op.
        }

        private static InvalidOperationException GetInvalidOperationException()
        {
            return new InvalidOperationException("Operation is not supported by readonly transaction");
        }
    }
}