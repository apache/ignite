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
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Transactions facade.
    /// </summary>
    internal class TransactionsImpl : PlatformTarget, ITransactions
    {
        /** */
        private const int OpCacheConfigParameters = 1;

        /** */
        private const int OpMetrics = 2;

        /** */
        private const int OpStart = 3;

        /** */
        private const int OpCommit = 4;

        /** */
        private const int OpRollback = 5;

        /** */
        private const int OpClose = 6;

        /** */
        private const int OpState = 7;

        /** */
        private const int OpSetRollbackOnly = 8;

        /** */
        private const int OpCommitAsync = 9;

        /** */
        private const int OpRollbackAsync = 10;

        /** */
        private const int OpResetMetrics = 11;

        /** */
        private const int OpPrepare = 12;

        /** */
        private readonly TransactionConcurrency _dfltConcurrency;

        /** */
        private readonly TransactionIsolation _dfltIsolation;

        /** */
        private readonly TimeSpan _dfltTimeout;

        /** */
        private readonly Guid _localNodeId;

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionsImpl" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="localNodeId">Local node id.</param>
        public TransactionsImpl(IUnmanagedTarget target, Marshaller marsh,
            Guid localNodeId) : base(target, marsh)
        {
            _localNodeId = localNodeId;

            TransactionConcurrency concurrency = default(TransactionConcurrency);
            TransactionIsolation isolation = default(TransactionIsolation);
            TimeSpan timeout = default(TimeSpan);

            DoInOp(OpCacheConfigParameters, stream =>
            {
                var reader = marsh.StartUnmarshal(stream).GetRawReader();

                concurrency = (TransactionConcurrency) reader.ReadInt();
                isolation = (TransactionIsolation) reader.ReadInt();
                timeout = reader.ReadLongAsTimespan();
            });

            _dfltConcurrency = concurrency;
            _dfltIsolation = isolation;
            _dfltTimeout = timeout;
        }

        /** <inheritDoc /> */
        public ITransaction TxStart()
        {
            return TxStart(_dfltConcurrency, _dfltIsolation);
        }

        /** <inheritDoc /> */
        public ITransaction TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation)
        {
            return TxStart(concurrency, isolation, _dfltTimeout, 0);
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Reliability", "CA2000:Dispose objects before losing scope")]
        public ITransaction TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
            TimeSpan timeout, int txSize)
        {
            var id = DoOutInOp(OpStart, w =>
            {
                w.WriteInt((int) concurrency);
                w.WriteInt((int) isolation);
                w.WriteTimeSpanAsLong(timeout);
                w.WriteInt(txSize);
            }, s => s.ReadLong());

            var innerTx = new TransactionImpl(id, this, concurrency, isolation, timeout, _localNodeId);
            
            return new Transaction(innerTx);
        }

        /** <inheritDoc /> */
        public ITransaction Tx
        {
            get { return TransactionImpl.Current; }
        }

        /** <inheritDoc /> */
        public ITransactionMetrics GetMetrics()
        {
            return DoInOp(OpMetrics, stream =>
            {
                IBinaryRawReader reader = Marshaller.StartUnmarshal(stream, false);

                return new TransactionMetricsImpl(reader);
            });
        }

        /** <inheritDoc /> */
        public void ResetMetrics()
        {
            DoOutInOp(OpResetMetrics);
        }

        /** <inheritDoc /> */
        public TransactionConcurrency DefaultTransactionConcurrency
        {
            get { return _dfltConcurrency; }
        }

        /** <inheritDoc /> */
        public TransactionIsolation DefaultTransactionIsolation
        {
            get { return _dfltIsolation; }
        }

        /** <inheritDoc /> */
        public TimeSpan DefaultTimeout
        {
            get { return _dfltTimeout; }
        }

        /// <summary>
        /// Executes prepare step of the two phase commit.
        /// </summary>
        internal void TxPrepare(TransactionImpl tx)
        {
            DoOutInOp(OpPrepare, tx.Id);
        }

        /// <summary>
        /// Commit transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Final transaction state.</returns>
        internal TransactionState TxCommit(TransactionImpl tx)
        {
            return (TransactionState) DoOutInOp(OpCommit, tx.Id);
        }

        /// <summary>
        /// Rollback transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Final transaction state.</returns>
        internal TransactionState TxRollback(TransactionImpl tx)
        {
            return (TransactionState) DoOutInOp(OpRollback, tx.Id);
        }

        /// <summary>
        /// Close transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Final transaction state.</returns>
        internal int TxClose(TransactionImpl tx)
        {
            return (int) DoOutInOp(OpClose, tx.Id);
        }

        /// <summary>
        /// Get transaction current state.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Transaction current state.</returns>
        internal TransactionState TxState(TransactionImpl tx)
        {
            return (TransactionState) DoOutInOp(OpState, tx.Id);
        }

        /// <summary>
        /// Set transaction rollback-only flag.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns><c>true</c> if the flag was set.</returns>
        internal bool TxSetRollbackOnly(TransactionImpl tx)
        {
            return DoOutInOp(OpSetRollbackOnly, tx.Id) == True;
        }

        /// <summary>
        /// Commits tx in async mode.
        /// </summary>
        internal Task CommitAsync(TransactionImpl tx)
        {
            return DoOutOpAsync(OpCommitAsync, w => w.WriteLong(tx.Id));
        }

        /// <summary>
        /// Rolls tx back in async mode.
        /// </summary>
        internal Task RollbackAsync(TransactionImpl tx)
        {
            return DoOutOpAsync(OpRollbackAsync, w => w.WriteLong(tx.Id));
        }
    }
}