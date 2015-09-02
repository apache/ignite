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
    using System.Threading;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Grid cache transaction implementation.
    /// </summary>
    internal sealed class TransactionImpl
    {
        /** Metadatas. */
        private object[] metas;

        /** Unique  transaction ID.*/
        private readonly long id;

        /** Cache. */
        private readonly TransactionsImpl txs;

        /** TX concurrency. */
        private readonly TransactionConcurrency concurrency;

        /** TX isolation. */
        private readonly TransactionIsolation isolation;

        /** Timeout. */
        private readonly TimeSpan timeout;

        /** Start time. */
        private readonly DateTime startTime;

        /** Owning thread ID. */
        private readonly int threadId;

        /** Originating node ID. */
        private readonly Guid nodeId;

        /** State holder. */
        private StateHolder state;

        // ReSharper disable once InconsistentNaming
        /** Transaction for this thread. */
        [ThreadStatic]
        private static TransactionImpl THREAD_TX;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        /// <param name="txs">Transactions.</param>
        /// <param name="concurrency">TX concurrency.</param>
        /// <param name="isolation">TX isolation.</param>
        /// <param name="timeout">Timeout.</param>
        /// <param name="nodeId">The originating node identifier.</param>
        public TransactionImpl(long id, TransactionsImpl txs, TransactionConcurrency concurrency,
            TransactionIsolation isolation, TimeSpan timeout, Guid nodeId) {
            this.id = id;
            this.txs = txs;
            this.concurrency = concurrency;
            this.isolation = isolation;
            this.timeout = timeout;
            this.nodeId = nodeId;

            startTime = DateTime.Now;

            threadId = Thread.CurrentThread.ManagedThreadId;

            THREAD_TX = this;
        }    

        /// <summary>
        /// Transaction assigned to this thread.
        /// </summary>
        public static Transaction Current
        {
            get
            {
                var tx = THREAD_TX;

                if (tx == null)
                    return null;

                if (tx.IsClosed)
                {
                    THREAD_TX = null;

                    return null;
                }

                return new Transaction(tx);
            }
        }

        /// <summary>
        /// Commits this tx and closes it.
        /// </summary>
        public void Commit()
        {
            lock (this)
            {
                ThrowIfClosed();

                state = new StateHolder(txs.TxCommit(this));
            }
        }

        /// <summary>
        /// Rolls this tx back and closes it.
        /// </summary>
        public void Rollback()
        {
            lock (this)
            {
                ThrowIfClosed();

                state = new StateHolder(txs.TxRollback(this));
            }
        }

        /// <summary>
        /// Sets the rollback only flag.
        /// </summary>
        public bool SetRollbackOnly()
        {
            lock (this)
            {
                ThrowIfClosed();

                return txs.TxSetRollbackOnly(this);
            }
        }

        /// <summary>
        /// Gets a value indicating whether this instance is rollback only.
        /// </summary>
        public bool IsRollbackOnly
        {
            get
            {
                lock (this)
                {
                    var state0 = state == null ? State : state.State;

                    return state0 == TransactionState.MARKED_ROLLBACK ||
                           state0 == TransactionState.ROLLING_BACK ||
                           state0 == TransactionState.ROLLED_BACK;
                }
            }
        }

        /// <summary>
        /// Gets the state.
        /// </summary>
        public TransactionState State
        {
            get
            {
                lock (this)
                {
                    return state != null ? state.State : txs.TxState(this);
                }
            }
        }

        /// <summary>
        /// Gets the isolation.
        /// </summary>
        public TransactionIsolation Isolation
        {
            get { return isolation; }
        }

        /// <summary>
        /// Gets the concurrency.
        /// </summary>
        public TransactionConcurrency Concurrency
        {
            get { return concurrency; }
        }

        /// <summary>
        /// Gets the timeout.
        /// </summary>
        public TimeSpan Timeout
        {
            get { return timeout; }
        }

        /// <summary>
        /// Gets the start time.
        /// </summary>
        public DateTime StartTime
        {
            get { return startTime; }
        }


        /// <summary>
        /// Gets the node identifier.
        /// </summary>
        public Guid NodeId
        {
            get { return nodeId; }
        }

        /// <summary>
        /// Gets the thread identifier.
        /// </summary>
        public long ThreadId
        {
            get { return threadId; }
        }

        /// <summary>
        /// Adds a new metadata.
        /// </summary>
        public void AddMeta<V>(string name, V val)
        {
            if (name == null)
                throw new ArgumentException("Meta name cannot be null.");

            lock (this)
            {
                if (metas != null)
                {
                    int putIdx = -1;

                    for (int i = 0; i < metas.Length; i += 2)
                    {
                        if (name.Equals(metas[i]))
                        {
                            metas[i + 1] = val;

                            return;
                        }
                        else if (metas[i] == null && putIdx == -1)
                            // Preserve empty space index.
                            putIdx = i;
                    }

                    // No meta with the given name found.
                    if (putIdx == -1)
                    {
                        // Extend array.
                        putIdx = metas.Length;

                        object[] metas0 = new object[putIdx + 2];

                        Array.Copy(metas, metas0, putIdx);

                        metas = metas0;
                    }
                    
                    metas[putIdx] = name;
                    metas[putIdx + 1] = val;
                }
                else
                    metas = new object[] { name, val };
            }
        }

        /// <summary>
        /// Gets metadata by name.
        /// </summary>
        public V Meta<V>(string name)
        {
            if (name == null)
                throw new ArgumentException("Meta name cannot be null.");

            lock (this)
            {
                if (metas != null)
                {
                    for (int i = 0; i < metas.Length; i += 2)
                    {
                        if (name.Equals(metas[i]))
                            return (V)metas[i + 1];
                    }
                }

                return default(V);
            }
        }

        /// <summary>
        /// Removes metadata by name.
        /// </summary>
        public V RemoveMeta<V>(string name)
        {
            if (name == null)
                throw new ArgumentException("Meta name cannot be null.");

            lock (this)
            {
                if (metas != null)
                {
                    for (int i = 0; i < metas.Length; i += 2)
                    {
                        if (name.Equals(metas[i]))
                        {
                            V val = (V)metas[i + 1];

                            metas[i] = null;
                            metas[i + 1] = null;

                            return val;
                        }
                    }
                }

                return default(V);
            }
        }

        /// <summary>
        /// Commits tx in async mode.
        /// </summary>
        internal IFuture CommitAsync()
        {
            lock (this)
            {
                ThrowIfClosed();

                var fut = txs.CommitAsync(this);

                CloseWhenComplete(fut);

                return fut;
            }
        }

        /// <summary>
        /// Rolls tx back in async mode.
        /// </summary>
        internal IFuture RollbackAsync()
        {
            lock (this)
            {
                ThrowIfClosed();

                var fut = txs.RollbackAsync(this);

                CloseWhenComplete(fut);

                return fut;
            }
        }

        /// <summary>
        /// Transaction ID.
        /// </summary>
        internal long Id
        {
            get { return id; }
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            try
            {
                Close();
            }
            finally
            {
                GC.SuppressFinalize(this);
            }
        }

        /// <summary>
        /// Gets a value indicating whether this transaction is closed.
        /// </summary>
        internal bool IsClosed
        {
            get { return state != null; }
        }

        /// <summary>
        /// Gets the closed exception.
        /// </summary>
        private InvalidOperationException GetClosedException()
        {
            return new InvalidOperationException(string.Format("Transaction {0} is closed, state is {1}", Id, State));
        }

        /// <summary>
        /// Creates a future via provided factory if IsClosed is false; otherwise, return a future with an error.
        /// </summary>
        internal IFuture GetFutureOrError(Func<IFuture> operationFactory)
        {
            lock (this)
            {
                return IsClosed ? GetExceptionFuture() : operationFactory();
            }
        }

        /// <summary>
        /// Gets the future that throws an exception.
        /// </summary>
        private IFuture GetExceptionFuture()
        {
            var fut = new Future<object>();

            fut.OnError(GetClosedException());

            return fut;
        }

        /// <summary>
        /// Closes the transaction and releases unmanaged resources.
        /// </summary>
        private void Close()
        {
            lock (this)
            {
                state = state ?? new StateHolder((TransactionState) txs.TxClose(this));
            }
        }

        /// <summary>
        /// Throws and exception if transaction is closed.
        /// </summary>
        private void ThrowIfClosed()
        {
            if (IsClosed)
                throw GetClosedException();
        }

        /// <summary>
        /// Closes this transaction upon future completion.
        /// </summary>
        private void CloseWhenComplete(IFuture fut)
        {
            fut.Listen(Close);
        }

        /** <inheritdoc /> */
        ~TransactionImpl()
        {
            Dispose();
        }

        /// <summary>
        /// State holder.
        /// </summary>
        private class StateHolder
        {
            /** Current state. */
            private readonly TransactionState state;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="state">State.</param>
            public StateHolder(TransactionState state)
            {
                this.state = state;
            }

            /// <summary>
            /// Current state.
            /// </summary>
            public TransactionState State
            {
                get { return state; }
            }
        }
    }
}
