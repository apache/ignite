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
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Ignite transaction facade.
    /// </summary>
    internal sealed class Transaction : ITransaction
    {
        /** */
        private readonly TransactionImpl _tx;

        /// <summary>
        /// Initializes a new instance of the <see cref="Transaction" /> class.
        /// </summary>
        /// <param name="tx">The tx to wrap.</param>
        public Transaction(TransactionImpl tx)
        {
            _tx = tx;
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            _tx.Dispose();
        }

        /** <inheritDoc /> */
        public Guid NodeId
        {
            get { return _tx.NodeId; }
        }

        /** <inheritDoc /> */
        public long ThreadId
        {
            get { return _tx.ThreadId; }
        }

        /** <inheritDoc /> */
        public DateTime StartTime
        {
            get { return _tx.StartTime; }
        }

        /** <inheritDoc /> */
        public TransactionIsolation Isolation
        {
            get { return _tx.Isolation; }
        }

        /** <inheritDoc /> */
        public TransactionConcurrency Concurrency
        {
            get { return _tx.Concurrency; }
        }

        /** <inheritDoc /> */
        public TransactionState State
        {
            get { return _tx.State; }
        }

        /** <inheritDoc /> */
        public TimeSpan Timeout
        {
            get { return _tx.Timeout; }
        }

        /** <inheritDoc /> */
        public bool IsRollbackOnly
        {
            get { return _tx.IsRollbackOnly; }
        }

        /** <inheritDoc /> */
        public bool SetRollbackonly()
        {
            return _tx.SetRollbackOnly();
        }

        /** <inheritDoc /> */
        public void Commit()
        {
            _tx.Commit();
        }

        /** <inheritDoc /> */
        public Task CommitAsync()
        {
            return _tx.GetTask(() => _tx.CommitAsync());
        }

        /** <inheritDoc /> */
        public void Rollback()
        {
            _tx.Rollback();
        }

        /** <inheritDoc /> */
        public Task RollbackAsync()
        {
            return _tx.GetTask(() => _tx.RollbackAsync());
        }

        /** <inheritDoc /> */
        public void AddMeta<TV>(string name, TV val)
        {
            _tx.AddMeta(name, val);
        }

        /** <inheritDoc /> */
        public TV Meta<TV>(string name)
        {
            return _tx.Meta<TV>(name);
        }

        /** <inheritDoc /> */
        public TV RemoveMeta<TV>(string name)
        {
            return _tx.RemoveMeta<TV>(name);
        }

        /// <summary>
        /// Executes prepare step of the two phase commit.
        /// </summary>
        public void Prepare()
        {
            _tx.Prepare();
        }
    }
}