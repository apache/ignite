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
    using System.Globalization;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Grid cache transaction implementation.
    /// </summary>
    internal sealed class TransactionImpl : IDisposable
    {
        /** Metadatas. */
        private object[] _metas;

        /** Unique  transaction ID.*/
        private readonly long _id;

        /** Cache. */
        private readonly TransactionsImpl _txs;

        /** TX concurrency. */
        private readonly TransactionConcurrency _concurrency;

        /** TX isolation. */
        private readonly TransactionIsolation _isolation;

        /** Timeout. */
        private readonly TimeSpan _timeout;

        /** Start time. */
        private readonly DateTime _startTime;

        /** Owning thread ID. */
        private readonly int _threadId;

        /** Originating node ID. */
        private readonly Guid _nodeId;

        /** State holder. */
        private StateHolder _state;

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
            _id = id;
            _txs = txs;
            _concurrency = concurrency;
            _isolation = isolation;
            _timeout = timeout;
            _nodeId = nodeId;

            _startTime = DateTime.Now;

            _threadId = Thread.CurrentThread.ManagedThreadId;

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
        /// Executes prepare step of the two phase commit.
        /// </summary>
        public void Prepare()
        {
            lock (this)
            {
                ThrowIfClosed();

                _txs.TxPrepare(this);
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

                _state = new StateHolder(_txs.TxCommit(this));
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

                _state = new StateHolder(_txs.TxRollback(this));
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

                return _txs.TxSetRollbackOnly(this);
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
                    var state0 = _state == null ? State : _state.State;

                    return state0 == TransactionState.MarkedRollback ||
                           state0 == TransactionState.RollingBack ||
                           state0 == TransactionState.RolledBack;
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
                    return _state != null ? _state.State : _txs.TxState(this);
                }
            }
        }

        /// <summary>
        /// Gets the isolation.
        /// </summary>
        public TransactionIsolation Isolation
        {
            get { return _isolation; }
        }

        /// <summary>
        /// Gets the concurrency.
        /// </summary>
        public TransactionConcurrency Concurrency
        {
            get { return _concurrency; }
        }

        /// <summary>
        /// Gets the timeout.
        /// </summary>
        public TimeSpan Timeout
        {
            get { return _timeout; }
        }

        /// <summary>
        /// Gets the start time.
        /// </summary>
        public DateTime StartTime
        {
            get { return _startTime; }
        }


        /// <summary>
        /// Gets the node identifier.
        /// </summary>
        public Guid NodeId
        {
            get { return _nodeId; }
        }

        /// <summary>
        /// Gets the thread identifier.
        /// </summary>
        public long ThreadId
        {
            get { return _threadId; }
        }

        /// <summary>
        /// Adds a new metadata.
        /// </summary>
        public void AddMeta<TV>(string name, TV val)
        {
            if (name == null)
                throw new ArgumentException("Meta name cannot be null.");

            lock (this)
            {
                if (_metas != null)
                {
                    int putIdx = -1;

                    for (int i = 0; i < _metas.Length; i += 2)
                    {
                        if (name.Equals(_metas[i]))
                        {
                            _metas[i + 1] = val;

                            return;
                        }
                        if (_metas[i] == null && putIdx == -1)
                            // Preserve empty space index.
                            putIdx = i;
                    }

                    // No meta with the given name found.
                    if (putIdx == -1)
                    {
                        // Extend array.
                        putIdx = _metas.Length;

                        object[] metas0 = new object[putIdx + 2];

                        Array.Copy(_metas, metas0, putIdx);

                        _metas = metas0;
                    }
                    
                    _metas[putIdx] = name;
                    _metas[putIdx + 1] = val;
                }
                else
                    _metas = new object[] { name, val };
            }
        }

        /// <summary>
        /// Gets metadata by name.
        /// </summary>
        public TV Meta<TV>(string name)
        {
            if (name == null)
                throw new ArgumentException("Meta name cannot be null.");

            lock (this)
            {
                if (_metas != null)
                {
                    for (int i = 0; i < _metas.Length; i += 2)
                    {
                        if (name.Equals(_metas[i]))
                            return (TV)_metas[i + 1];
                    }
                }

                return default(TV);
            }
        }

        /// <summary>
        /// Removes metadata by name.
        /// </summary>
        public TV RemoveMeta<TV>(string name)
        {
            if (name == null)
                throw new ArgumentException("Meta name cannot be null.");

            lock (this)
            {
                if (_metas != null)
                {
                    for (int i = 0; i < _metas.Length; i += 2)
                    {
                        if (name.Equals(_metas[i]))
                        {
                            TV val = (TV)_metas[i + 1];

                            _metas[i] = null;
                            _metas[i + 1] = null;

                            return val;
                        }
                    }
                }

                return default(TV);
            }
        }

        /// <summary>
        /// Commits tx in async mode.
        /// </summary>
        internal Task CommitAsync()
        {
            lock (this)
            {
                ThrowIfClosed();

                return CloseWhenComplete(_txs.CommitAsync(this));
            }
        }

        /// <summary>
        /// Rolls tx back in async mode.
        /// </summary>
        internal Task RollbackAsync()
        {
            lock (this)
            {
                ThrowIfClosed();

                return CloseWhenComplete(_txs.RollbackAsync(this));
            }
        }

        /// <summary>
        /// Transaction ID.
        /// </summary>
        internal long Id
        {
            get { return _id; }
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
        private bool IsClosed
        {
            get { return _state != null; }
        }

        /// <summary>
        /// Gets the closed exception.
        /// </summary>
        private InvalidOperationException GetClosedException()
        {
            return new InvalidOperationException(string.Format(CultureInfo.InvariantCulture, 
                "Transaction {0} is closed, state is {1}", Id, State));
        }

        /// <summary>
        /// Creates a task via provided factory if IsClosed is false; otherwise, return a task with an error.
        /// </summary>
        internal Task GetTask(Func<Task> operationFactory)
        {
            lock (this)
            {
                return IsClosed ? GetExceptionTask() : operationFactory();
            }
        }

        /// <summary>
        /// Gets the task that throws an exception.
        /// </summary>
        private Task GetExceptionTask()
        {
            var tcs = new TaskCompletionSource<object>();
            
            tcs.SetException(GetClosedException());
            
            return tcs.Task;
        }

        /// <summary>
        /// Closes the transaction and releases unmanaged resources.
        /// </summary>
        private void Close()
        {
            lock (this)
            {
                _state = _state ?? new StateHolder((TransactionState) _txs.TxClose(this));
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
        /// Closes this transaction upon task completion.
        /// </summary>
        private Task CloseWhenComplete(Task task)
        {
            return task.ContinueWith(x => Close());
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
            private readonly TransactionState _state;

            /// <summary>
            /// Constructor.
            /// </summary>
            /// <param name="state">State.</param>
            public StateHolder(TransactionState state)
            {
                _state = state;
            }

            /// <summary>
            /// Current state.
            /// </summary>
            public TransactionState State
            {
                get { return _state; }
            }
        }
    }
}
