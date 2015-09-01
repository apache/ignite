namespace GridGain.Impl.Transactions
{
    using System;
    using Apache.Ignite.Core.Common;
    using GridGain.Common;
    using GridGain.Transactions;

    using U = GridGain.Impl.GridUtils;

    /// <summary>
    /// Grid transaction facade.
    /// </summary>
    internal class Transaction : ITransaction
    {
        /** */
        protected readonly TransactionImpl tx;

        /// <summary>
        /// Initializes a new instance of the <see cref="Transaction" /> class.
        /// </summary>
        /// <param name="tx">The tx to wrap.</param>
        public Transaction(TransactionImpl tx)
        {
            this.tx = tx;
        }

        /** <inheritDoc /> */
        public void Dispose()
        {
            tx.Dispose();
        }

        /** <inheritDoc /> */
        public ITransaction WithAsync()
        {
            return new AsyncTransaction(tx);
        }

        /** <inheritDoc /> */
        public virtual bool IsAsync
        {
            get { return false; }
        }

        /** <inheritDoc /> */
        public virtual IFuture GetFuture()
        {
            throw U.GetAsyncModeDisabledException();
        }
        
        /** <inheritDoc /> */
        public virtual IFuture<TResult> GetFuture<TResult>()
        {
            throw U.GetAsyncModeDisabledException();
        }

        /** <inheritDoc /> */
        public Guid NodeId
        {
            get { return tx.NodeId; }
        }

        /** <inheritDoc /> */
        public long ThreadId
        {
            get { return tx.ThreadId; }
        }

        /** <inheritDoc /> */
        public DateTime StartTime
        {
            get { return tx.StartTime; }
        }

        /** <inheritDoc /> */
        public TransactionIsolation Isolation
        {
            get { return tx.Isolation; }
        }

        /** <inheritDoc /> */
        public TransactionConcurrency Concurrency
        {
            get { return tx.Concurrency; }
        }

        /** <inheritDoc /> */
        public TransactionState State
        {
            get { return tx.State; }
        }

        /** <inheritDoc /> */
        public TimeSpan Timeout
        {
            get { return tx.Timeout; }
        }

        /** <inheritDoc /> */
        public bool IsRollbackOnly
        {
            get { return tx.IsRollbackOnly; }
        }

        /** <inheritDoc /> */
        public bool SetRollbackonly()
        {
            return tx.SetRollbackOnly();
        }

        /** <inheritDoc /> */
        public virtual void Commit()
        {
            tx.Commit();
        }

        /** <inheritDoc /> */
        public virtual void Rollback()
        {
            tx.Rollback();
        }

        /** <inheritDoc /> */
        public void AddMeta<V>(string name, V val)
        {
            tx.AddMeta(name, val);
        }

        /** <inheritDoc /> */
        public V Meta<V>(string name)
        {
            return tx.Meta<V>(name);
        }

        /** <inheritDoc /> */
        public V RemoveMeta<V>(string name)
        {
            return tx.RemoveMeta<V>(name);
        }
    }
}