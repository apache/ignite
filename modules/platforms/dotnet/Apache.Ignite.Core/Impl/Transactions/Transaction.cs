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
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Ignite transaction facade.
    /// </summary>
    internal class Transaction : ITransaction
    {
        /** */
        protected readonly TransactionImpl Tx;

        /// <summary>
        /// Initializes a new instance of the <see cref="Transaction" /> class.
        /// </summary>
        /// <param name="tx">The tx to wrap.</param>
        public Transaction(TransactionImpl tx)
        {
            Tx = tx;
        }

        /** <inheritDoc /> */
        public void Dispose()
        {
            Tx.Dispose();
        }

        /** <inheritDoc /> */
        public ITransaction WithAsync()
        {
            return new AsyncTransaction(Tx);
        }

        /** <inheritDoc /> */
        public virtual bool IsAsync
        {
            get { return false; }
        }

        /** <inheritDoc /> */
        public virtual IFuture GetFuture()
        {
            throw IgniteUtils.GetAsyncModeDisabledException();
        }
        
        /** <inheritDoc /> */
        public virtual IFuture<TResult> GetFuture<TResult>()
        {
            throw IgniteUtils.GetAsyncModeDisabledException();
        }

        /** <inheritDoc /> */
        public Guid NodeId
        {
            get { return Tx.NodeId; }
        }

        /** <inheritDoc /> */
        public long ThreadId
        {
            get { return Tx.ThreadId; }
        }

        /** <inheritDoc /> */
        public DateTime StartTime
        {
            get { return Tx.StartTime; }
        }

        /** <inheritDoc /> */
        public TransactionIsolation Isolation
        {
            get { return Tx.Isolation; }
        }

        /** <inheritDoc /> */
        public TransactionConcurrency Concurrency
        {
            get { return Tx.Concurrency; }
        }

        /** <inheritDoc /> */
        public TransactionState State
        {
            get { return Tx.State; }
        }

        /** <inheritDoc /> */
        public TimeSpan Timeout
        {
            get { return Tx.Timeout; }
        }

        /** <inheritDoc /> */
        public bool IsRollbackOnly
        {
            get { return Tx.IsRollbackOnly; }
        }

        /** <inheritDoc /> */
        public bool SetRollbackonly()
        {
            return Tx.SetRollbackOnly();
        }

        /** <inheritDoc /> */
        public virtual void Commit()
        {
            Tx.Commit();
        }

        /** <inheritDoc /> */
        public virtual void Rollback()
        {
            Tx.Rollback();
        }

        /** <inheritDoc /> */
        public void AddMeta<TV>(string name, TV val)
        {
            Tx.AddMeta(name, val);
        }

        /** <inheritDoc /> */
        public TV Meta<TV>(string name)
        {
            return Tx.Meta<TV>(name);
        }

        /** <inheritDoc /> */
        public TV RemoveMeta<TV>(string name)
        {
            return Tx.RemoveMeta<TV>(name);
        }
    }
}