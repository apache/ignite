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
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Transactions;
    using UU = Apache.Ignite.Core.Impl.Unmanaged.UnmanagedUtils;

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
        public TransactionsImpl(IUnmanagedTarget target, PortableMarshaller marsh,
            Guid localNodeId) : base(target, marsh)
        {
            _localNodeId = localNodeId;

            TransactionConcurrency concurrency = default(TransactionConcurrency);
            TransactionIsolation isolation = default(TransactionIsolation);
            TimeSpan timeout = default(TimeSpan);

            DoInOp(OpCacheConfigParameters, stream =>
            {
                var reader = marsh.StartUnmarshal(stream).RawReader();

                concurrency = reader.ReadEnum<TransactionConcurrency>();
                isolation = reader.ReadEnum<TransactionIsolation>();
                timeout = TimeSpan.FromMilliseconds(reader.ReadLong());
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
        public ITransaction TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
            TimeSpan timeout, int txSize)
        {
            var id = UU.TransactionsStart(Target, (int)concurrency, (int)isolation, (long)timeout.TotalMilliseconds,
                txSize);

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
                IPortableRawReader reader = Marshaller.StartUnmarshal(stream, false);

                return new TransactionMetricsImpl(reader);
            });
        }

        /** <inheritDoc /> */
        public void ResetMetrics()
        {
            UU.TransactionsResetMetrics(Target);
        }

        /// <summary>
        /// Commit transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Final transaction state.</returns>
        internal TransactionState TxCommit(TransactionImpl tx)
        {
            return (TransactionState) UU.TransactionsCommit(Target, tx.Id);
        }

        /// <summary>
        /// Rollback transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Final transaction state.</returns>
        internal TransactionState TxRollback(TransactionImpl tx)
        {
            return (TransactionState)UU.TransactionsRollback(Target, tx.Id);
        }

        /// <summary>
        /// Close transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Final transaction state.</returns>
        internal int TxClose(TransactionImpl tx)
        {
            return UU.TransactionsClose(Target, tx.Id);
        }

        /// <summary>
        /// Get transaction current state.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Transaction current state.</returns>
        internal TransactionState TxState(TransactionImpl tx)
        {
            return GetTransactionState(UU.TransactionsState(Target, tx.Id));
        }

        /// <summary>
        /// Set transaction rollback-only flag.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns><c>true</c> if the flag was set.</returns>
        internal bool TxSetRollbackOnly(TransactionImpl tx)
        {
            return UU.TransactionsSetRollbackOnly(Target, tx.Id);
        }

        /// <summary>
        /// Commits tx in async mode.
        /// </summary>
        internal IFuture CommitAsync(TransactionImpl tx)
        {
            return GetFuture<object>((futId, futTyp) => UU.TransactionsCommitAsync(Target, tx.Id, futId));
        }

        /// <summary>
        /// Rolls tx back in async mode.
        /// </summary>
        internal IFuture RollbackAsync(TransactionImpl tx)
        {
            return GetFuture<object>((futId, futTyp) => UU.TransactionsRollbackAsync(Target, tx.Id, futId));
        }
 
        /// <summary>
        /// Gets the state of the transaction from int.
        /// </summary>
        private static TransactionState GetTransactionState(int state)
        {
            return (TransactionState)state;
        }
    }
}