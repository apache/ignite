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
    using System.Globalization;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Cache transaction proxy which supports only implicit rollback operations and getters.
    /// <para/>
    /// Does not support the following operations:
    /// <list type="bullet">
    ///     <item><description><see cref="Commit"/>.</description></item>
    ///     <item><description><see cref="CommitAsync"/>.</description></item>
    ///     <item><description>Get <see cref="Meta{TV}"/>.</description></item>
    ///     <item><description>Get <see cref="AddMeta{TV}"/>.</description></item>
    ///     <item><description>Get <see cref="RemoveMeta{TV}"/>.</description></item>
    ///     <item><description>Get <see cref="StartTime"/>.</description></item>
    ///     <item><description>Get <see cref="ThreadId"/>.</description></item>
    /// </list>
    /// </summary>
    internal class TransactionRollbackOnlyProxy : ITransaction
    {
        /** Transactions facade. */
        private readonly TransactionsImpl _txs;

        /** Unique transaction view ID. */
        private readonly long _id;

        /** Is closed. */
        private volatile bool _isClosed;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        /// <param name="txs">Transactions.</param>
        /// <param name="concurrency">TX concurrency.</param>
        /// <param name="isolation">TX isolation.</param>
        /// <param name="timeout">Timeout.</param>
        /// <param name="label">TX label.</param>
        /// <param name="nodeId">The originating node identifier.</param>
        public TransactionRollbackOnlyProxy(
            TransactionsImpl txs,
            long id,
            TransactionConcurrency concurrency,
            TransactionIsolation isolation,
            TimeSpan timeout,
            string label,
            Guid nodeId)
        {
            _txs = txs;
            _id = id;
            NodeId = nodeId;
            Isolation = isolation;
            Concurrency = concurrency;
            Timeout = timeout;
            Label = label;
        }

        /** <inheritdoc /> */
        public Guid NodeId { get; private set; }

        /** <inheritdoc /> */
        public long ThreadId
        {
            get { throw GetInvalidOperationException(); }
        }

        /** <inheritdoc /> */
        public DateTime StartTime
        {
            get { throw GetInvalidOperationException(); }
        }

        /** <inheritdoc /> */
        public TransactionIsolation Isolation { get; private set; }

        /** <inheritdoc /> */
        public TransactionConcurrency Concurrency { get; private set; }

        /** <inheritdoc /> */
        public TransactionState State
        {
            get
            {
                lock (this)
                {
                    ThrowIfClosed();

                    return _txs.TxState(this);   
                }
            }
        }

        /** <inheritdoc /> */
        public TimeSpan Timeout { get; private set; }

        /** <inheritdoc /> */
        public string Label { get; private set; }

        /** <inheritdoc /> */
        public bool IsRollbackOnly
        {
            get { return true; }
        }

        /** <inheritDoc /> */
        public bool SetRollbackonly()
        {
            return true;
        }

        /** <inheritDoc /> */
        public void Commit()
        {
            throw GetInvalidOperationException();
        }

        /** <inheritDoc /> */
        public Task CommitAsync()
        {
            throw GetInvalidOperationException();
        }

        /** <inheritDoc /> */
        public void Rollback()
        {
            lock (this)
            {
                ThrowIfClosed();

                try
                {
                    _txs.TxRollback(this);
                }
                finally
                {
                    _isClosed = true;
                }
            }
        }

        /** <inheritDoc /> */
        public Task RollbackAsync()
        {
            lock (this)
            {
                ThrowIfClosed();

                return _txs.TxRollbackAsync(this)
                    .ContWith(t =>
                    {
                        try
                        {
                            _txs.TxClose(this);
                        }
                        finally
                        {
                            _isClosed = true;
                        }
                    });
            }
        }

        /** <inheritDoc /> */
        public void AddMeta<TV>(string name, TV val)
        {
            throw GetInvalidOperationException();
        }

        /** <inheritdoc /> */
        public TV Meta<TV>(string name)
        {
            throw GetInvalidOperationException();
        }

        /** <inheritDoc /> */
        public TV RemoveMeta<TV>(string name)
        {
            throw GetInvalidOperationException();
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "Dispose should not throw.")]
        public void Dispose()
        {
            if (!_isClosed)
            {
                try
                {
                    _txs.TxRemove(this);
                }
                catch(IgniteIllegalStateException)
                {
                    // No-op. Dispose should not throw.
                }
                finally
                {
                    _isClosed = true;
                    GC.SuppressFinalize(this);
                }
            }
        }

        /// <summary>
        /// Unique transaction view ID.
        /// </summary>
        internal long Id
        {
            get { return _id; }
        }

        /// <summary>
        /// Throws and exception if transaction is closed.
        /// </summary>
        private void ThrowIfClosed()
        {
            if (_isClosed)
                throw GetClosedException();
        }

        /// <summary>
        /// Gets the closed exception.
        /// </summary>
        private InvalidOperationException GetClosedException()
        {
            return new InvalidOperationException(string.Format(CultureInfo.InvariantCulture,
                "Transaction {0} is closed", Id));
        }

        /// <summary>
        /// Gets invalid operation exception.
        /// </summary>
        private static InvalidOperationException GetInvalidOperationException()
        {
            return new InvalidOperationException("Operation is not supported by rollback only transaction.");
        }
    }
}
