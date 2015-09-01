/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Transactions
{
    using System;

    using GridGain.Common;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Unmanaged;
    using GridGain.Portable;
    using GridGain.Transactions;

    using U = GridUtils;
    using UU = GridGain.Impl.Unmanaged.UnmanagedUtils;

    /// <summary>
    /// Transactions facade.
    /// </summary>
    internal class TransactionsImpl : GridTarget, ITransactions
    {
        /** */
        private const int OP_CACHE_CONFIG_PARAMETERS = 1;

        /** */
        private const int OP_METRICS = 2;
        
        /** */
        private readonly TransactionConcurrency dfltConcurrency;

        /** */
        private readonly TransactionIsolation dfltIsolation;

        /** */
        private readonly TimeSpan dfltTimeout;

        /** */
        private readonly Guid localNodeId;

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionsImpl" /> class.
        /// </summary>
        /// <param name="target">Target.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="localNodeId">Local node id.</param>
        public TransactionsImpl(IUnmanagedTarget target, PortableMarshaller marsh,
            Guid localNodeId) : base(target, marsh)
        {
            this.localNodeId = localNodeId;

            TransactionConcurrency concurrency = default(TransactionConcurrency);
            TransactionIsolation isolation = default(TransactionIsolation);
            TimeSpan timeout = default(TimeSpan);

            DoInOp(OP_CACHE_CONFIG_PARAMETERS, stream =>
            {
                var reader = marsh.StartUnmarshal(stream).RawReader();

                concurrency = reader.ReadEnum<TransactionConcurrency>();
                isolation = reader.ReadEnum<TransactionIsolation>();
                timeout = TimeSpan.FromMilliseconds(reader.ReadLong());
            });

            dfltConcurrency = concurrency;
            dfltIsolation = isolation;
            dfltTimeout = timeout;
        }

        /** <inheritDoc /> */
        public ITransaction TxStart()
        {
            return TxStart(dfltConcurrency, dfltIsolation);
        }

        /** <inheritDoc /> */
        public ITransaction TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation)
        {
            return TxStart(concurrency, isolation, dfltTimeout, 0);
        }

        /** <inheritDoc /> */
        public ITransaction TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
            TimeSpan timeout, int txSize)
        {
            var id = UU.TransactionsStart(target, (int)concurrency, (int)isolation, (long)timeout.TotalMilliseconds,
                txSize);

            var innerTx = new TransactionImpl(id, this, concurrency, isolation, timeout, localNodeId);
            
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
            return DoInOp(OP_METRICS, stream =>
            {
                IPortableRawReader reader = marsh.StartUnmarshal(stream, false);

                return new TransactionMetricsImpl(reader);
            });
        }

        /** <inheritDoc /> */
        public void ResetMetrics()
        {
            UU.TransactionsResetMetrics(target);
        }

        /// <summary>
        /// Commit transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Final transaction state.</returns>
        internal TransactionState TxCommit(TransactionImpl tx)
        {
            return (TransactionState) UU.TransactionsCommit(target, tx.Id);
        }

        /// <summary>
        /// Rollback transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Final transaction state.</returns>
        internal TransactionState TxRollback(TransactionImpl tx)
        {
            return (TransactionState)UU.TransactionsRollback(target, tx.Id);
        }

        /// <summary>
        /// Close transaction.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Final transaction state.</returns>
        internal int TxClose(TransactionImpl tx)
        {
            return UU.TransactionsClose(target, tx.Id);
        }

        /// <summary>
        /// Get transaction current state.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns>Transaction current state.</returns>
        internal TransactionState TxState(TransactionImpl tx)
        {
            return GetTransactionState(UU.TransactionsState(target, tx.Id));
        }

        /// <summary>
        /// Set transaction rollback-only flag.
        /// </summary>
        /// <param name="tx">Transaction.</param>
        /// <returns><c>true</c> if the flag was set.</returns>
        internal bool TxSetRollbackOnly(TransactionImpl tx)
        {
            return UU.TransactionsSetRollbackOnly(target, tx.Id);
        }

        /// <summary>
        /// Commits tx in async mode.
        /// </summary>
        internal IFuture CommitAsync(TransactionImpl tx)
        {
            return GetFuture<object>((futId, futTyp) => UU.TransactionsCommitAsync(target, tx.Id, futId));
        }

        /// <summary>
        /// Rolls tx back in async mode.
        /// </summary>
        internal IFuture RollbackAsync(TransactionImpl tx)
        {
            return GetFuture<object>((futId, futTyp) => UU.TransactionsRollbackAsync(target, tx.Id, futId));
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