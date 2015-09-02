namespace Apache.Ignite.Core.Impl.Transactions
{
    using System;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Transactions;
    using U = GridUtils;

    /// <summary>
    /// Grid transaction facade.
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