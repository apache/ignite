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

namespace Apache.Ignite.Core.Impl.Client.Transactions
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Client.Transactions;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Ignite Thin Client transactions facade.
    /// </summary>
    internal class TransactionsClient : ITransactionsClient, IDisposable
    {
        /** Default transaction configuration. */
        private readonly TransactionClientConfiguration _cfg;

        /** Transaction for this thread and client. */
        private readonly ThreadLocal<TransactionClient> _currentTx = new ThreadLocal<TransactionClient>();

        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Transaction manager. */
        private readonly ClientCacheTransactionManager _txManager;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="cfg"></param>
        public TransactionsClient(IgniteClient ignite, TransactionClientConfiguration cfg)
        {
            _ignite = ignite;
            _cfg = cfg ?? new TransactionClientConfiguration();
            _txManager = new ClientCacheTransactionManager(this);
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            _currentTx.Dispose();
            _txManager.Dispose();
        }

        /// <summary>
        /// Starts ambient transaction if needed.
        /// </summary>
        internal void StartTxIfNeeded()
        {
            _txManager.StartTxIfNeeded();
        }

        /** <inheritDoc /> */
        ITransactionClient ITransactionsClient.Tx
        {
            get { return Tx; }
        }

        /// <summary>
        /// Gets transaction started by this thread or null if this thread does not have a transaction.
        /// </summary>
        internal TransactionClient Tx
        {
            get
            {
                var tx = _currentTx.Value;
                if (tx == null)
                    return null;

                if (tx.Closed)
                {
                    _currentTx.Value = null;

                    return null;
                }

                return tx;
            }
        }

        /** <inheritDoc /> */
        public TransactionConcurrency DefaultTransactionConcurrency
        {
            get { return _cfg.DefaultTransactionConcurrency; }
        }

        /** <inheritDoc /> */
        public TransactionIsolation DefaultTransactionIsolation
        {
            get { return _cfg.DefaultTransactionIsolation; }
        }

        /** <inheritDoc /> */
        public TimeSpan DefaultTimeout
        {
            get { return _cfg.DefaultTimeout; }
        }

        /** <inheritDoc /> */
        public ITransactionClient TxStart()
        {
            return TxStart(_cfg.DefaultTransactionConcurrency, _cfg.DefaultTransactionIsolation);
        }

        /** <inheritDoc /> */
        public ITransactionClient TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation)
        {
            return TxStart(concurrency, isolation, _cfg.DefaultTimeout);
        }

        /** <inheritDoc /> */
        public ITransactionClient TxStart(TransactionConcurrency concurrency,
            TransactionIsolation isolation,
            TimeSpan timeout)
        {
            return TxStart(concurrency, isolation, timeout, null);
        }

        /** <inheritDoc /> */
        public ITransactionsClient WithLabel(string label)
        {
            IgniteArgumentCheck.NotNullOrEmpty(label, "label");

            return new TransactionsClientWithLabel(this, label);
        }

        /// <summary>
        /// Starts a new transaction with the specified concurrency, isolation, timeout and label.
        /// </summary>
        private ITransactionClient TxStart(TransactionConcurrency concurrency,
            TransactionIsolation isolation,
            TimeSpan timeout,
            string label)
        {
            if (Tx != null)
            {
                throw new IgniteClientException("A transaction has already been started by the current thread.");
            }

            var tx = _ignite.Socket.DoOutInOp(
                ClientOp.TxStart,
                ctx =>
                {
                    ctx.Writer.WriteByte((byte) concurrency);
                    ctx.Writer.WriteByte((byte) isolation);
                    ctx.Writer.WriteTimeSpanAsLong(timeout);
                    ctx.Writer.WriteString(label);
                },
                ctx => new TransactionClient(
                    ctx.Reader.ReadInt(),
                    ctx.Socket,
                    concurrency,
                    isolation,
                    timeout,
                    label)
            );

            _currentTx.Value = tx;

            return tx;
        }

        /// <summary>
        /// Wrapper for transactions with label.
        /// </summary>
        private class TransactionsClientWithLabel : ITransactionsClient
        {
            /** Label. */
            private readonly string _label;

            /** Transactions. */
            private readonly TransactionsClient _transactions;

            /// <summary>
            /// Client transactions wrapper with label.
            /// </summary>
            public TransactionsClientWithLabel(TransactionsClient transactions, string label)
            {
                _transactions = transactions;
                _label = label;
            }

            /** <inheritDoc /> */
            public ITransactionClient TxStart()
            {
                return TxStart(DefaultTransactionConcurrency, DefaultTransactionIsolation);
            }

            /** <inheritDoc /> */
            public ITransactionClient TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation)
            {
                return TxStart(concurrency, isolation, DefaultTimeout);
            }

            /** <inheritDoc /> */
            public ITransactionClient TxStart(
                TransactionConcurrency concurrency,
                TransactionIsolation isolation,
                TimeSpan timeout)
            {
                return _transactions.TxStart(concurrency, isolation, timeout, _label);
            }

            /** <inheritDoc /> */
            public ITransactionsClient WithLabel(string label)
            {
                return new TransactionsClientWithLabel(_transactions, label);
            }

            /** <inheritDoc /> */
            public ITransactionClient Tx
            {
                get { return _transactions.Tx; }
            }

            /** <inheritDoc /> */
            public TransactionConcurrency DefaultTransactionConcurrency
            {
                get { return _transactions.DefaultTransactionConcurrency; }
            }

            /** <inheritDoc /> */
            public TransactionIsolation DefaultTransactionIsolation
            {
                get { return _transactions.DefaultTransactionIsolation; }
            }

            /** <inheritDoc /> */
            public TimeSpan DefaultTimeout
            {
                get { return _transactions.DefaultTimeout; }
            }
        }
    }
}
