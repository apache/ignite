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
    using System.Collections.Generic;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Transactions facade.
    /// </summary>
    internal class TransactionsImpl : PlatformTargetAdapter, ITransactions
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
        private const int OpLocalActiveTransactions = 13;

        /** */
        private const int OpLocalActiveTransactionsRemove = 14;

        /** */
        private readonly TransactionConcurrency _dfltConcurrency;

        /** */
        private readonly TransactionIsolation _dfltIsolation;

        /** */
        private readonly TimeSpan _dfltTimeout;

        /** */
        private readonly TimeSpan _dfltTimeoutOnPartitionMapExchange;

        /** */
        private readonly Guid _localNodeId;

        /** */
        private readonly Ignite _ignite;

        /** */
        private readonly string _label;

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionsImpl" /> class.
        /// </summary>
        /// <param name="ignite">Parent target, actually <see cref="Ignite"/> (used for withLabel)</param>
        /// <param name="target">Target.</param>
        /// <param name="localNodeId">Local node id.</param>
        /// <param name="label">TX label. </param>
        public TransactionsImpl(Ignite ignite, IPlatformTargetInternal target, Guid localNodeId, string label = null) 
            : base(target)
        {
            _localNodeId = localNodeId;

            var res = target.OutStream(OpCacheConfigParameters, reader => Tuple.Create(
                (TransactionConcurrency) reader.ReadInt(),
                (TransactionIsolation) reader.ReadInt(),
                reader.ReadLongAsTimespan(),
                reader.ReadLongAsTimespan()
            ));

            _dfltConcurrency = res.Item1;
            _dfltIsolation = res.Item2;
            _dfltTimeout = res.Item3;
            _dfltTimeoutOnPartitionMapExchange = res.Item4;
            _ignite = ignite;
            _label = label;
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

            var innerTx = new TransactionImpl(id, this, concurrency, isolation, timeout, _label, _localNodeId);

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
        public ITransactions WithLabel(string label)
        {
            IgniteArgumentCheck.NotNullOrEmpty(label, "label");

            return _ignite.GetTransactionsWithLabel(label);
        }

        /** <inheritDoc /> */
        public ITransactions WithTracing()
        {
            return this;
        }

        /** <inheritDoc /> */
        public ITransactionCollection GetLocalActiveTransactions()
        {
            return DoInOp(OpLocalActiveTransactions, stream =>
            {
                var reader = Marshaller.StartUnmarshal(stream);

                var size = reader.ReadInt();

                var result = new List<ITransaction>(size);

                for (var i = 0; i < size; i++)
                {
                    var id = reader.ReadLong();

                    var concurrency = reader.ReadInt();

                    var isolation = reader.ReadInt();

                    var timeout = reader.ReadLongAsTimespan();

                    var label = reader.ReadString();

                    var innerTx = new TransactionRollbackOnlyProxy(this, id, (TransactionConcurrency) concurrency,
                        (TransactionIsolation) isolation, timeout, label, _localNodeId);

                    result.Add(innerTx);
                }

                return new TransactionCollectionImpl(result);
            });
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

        /** <inheritDoc /> */
        public TimeSpan DefaultTimeoutOnPartitionMapExchange
        {
            get { return _dfltTimeoutOnPartitionMapExchange; }
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
        /// Rollback transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        internal void TxRollback(TransactionRollbackOnlyProxy tx)
        {
            DoOutInOp(OpRollback, tx.Id);
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
        /// Close transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Final transaction state.</returns>
        public void TxClose(TransactionRollbackOnlyProxy tx)
        {
            DoOutInOp(OpClose, tx.Id);
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
        /// Get transaction current state.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Transaction current state.</returns>
        internal TransactionState TxState(TransactionRollbackOnlyProxy tx)
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
        /// Remove transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        internal void TxRemove(TransactionRollbackOnlyProxy tx)
        {
            DoOutInOp(OpLocalActiveTransactionsRemove, tx.Id);
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
        
        /// <summary>
        /// Rolls tx back in async mode.
        /// </summary>
        internal Task TxRollbackAsync(TransactionRollbackOnlyProxy tx)
        {
            return DoOutOpAsync(OpRollbackAsync, w => w.WriteLong(tx.Id));
        }
    }
}