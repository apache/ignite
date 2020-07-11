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
    using System.Diagnostics;
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
    internal class TransactionsClient : ITransactionsClientInternal, IDisposable
    {
        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Default transaction concurrency. */
        private const TransactionConcurrency _dfltConcurrency = TransactionConcurrency.Pessimistic;

        /** Default transaction isolation. */
        private const TransactionIsolation _dfltIsolation = TransactionIsolation.RepeatableRead;

        /** Default transaction timeout. */
        private readonly TimeSpan _dfltTimeout = TimeSpan.Zero;

        /** Transaction for this thread and client. */
        private readonly ThreadLocal<TransactionClient> _currentTx = new ThreadLocal<TransactionClient>();

        /** Transaction manager. */
        private readonly ClientCacheTransactionManager _txManager;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        public TransactionsClient(IgniteClient ignite)
        {
            _ignite = ignite;
            _txManager = new ClientCacheTransactionManager(this);
        }

        /** <inheritdoc /> */
        public ITransactionClientInternal CurrentTx
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
        public void StartTxIfNeeded()
        {
            _txManager.StartTxIfNeeded();
        }

        /** <inheritDoc /> */
        public TransactionConcurrency DefaultTxConcurrency
        {
            get { return _dfltConcurrency; }
        }

        /** <inheritDoc /> */
        public TransactionIsolation DefaultTxIsolation
        {
            get { return _dfltIsolation; }
        }

        /** <inheritDoc /> */
        public TimeSpan DefaultTimeout
        {
            get { return _dfltTimeout; }
        }

        /** <inheritDoc /> */
        public ITransactionClient TxStart()
        {
            return TxStart(_dfltConcurrency, _dfltIsolation);
        }

        /** <inheritDoc /> */
        public ITransactionClient TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation)
        {
            return TxStart(concurrency, isolation, _dfltTimeout);
        }

        /** <inheritDoc /> */
        public ITransactionClient TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
            TimeSpan timeout)
        {
            return TxStart(concurrency, isolation, timeout, null);
        }

        private ITransactionClient TxStart(TransactionConcurrency concurrency,
            TransactionIsolation isolation,
            TimeSpan timeout,
            string label)
        {
            if (CurrentTx != null)
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
                    _ignite,
                    ctx.Socket)
            );

            _currentTx.Value = tx;
            return _currentTx.Value;
        }

        /** <inheritDoc /> */
        public ITransactionsClient WithLabel(string label)
        {
            IgniteArgumentCheck.NotNullOrEmpty(label, "label");

            return new TransactionsClientWithLabel(this, label); 
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Usage",
            "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            _currentTx.Dispose();
        }

        /// <summary>
        /// Wrapper for transactions with label.
        /// </summary>
        private class TransactionsClientWithLabel : ITransactionsClientInternal
        {
            /** Transactions. */
            private readonly TransactionsClient _transactions;

            /** Label. */
            private readonly string _label;

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
                return TxStart(DefaultTxConcurrency, DefaultTxIsolation);
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
            public ITransactionClientInternal CurrentTx
            {
                get { return _transactions.CurrentTx; }
            }

            /** <inheritDoc /> */
            public void StartTxIfNeeded()
            {
                Debug.Fail("Labeled transactions are not supported for ambient transactions");

                _transactions._txManager.StartTxIfNeeded();
            }

            /** <inheritDoc /> */
            public TransactionConcurrency DefaultTxConcurrency
            {
                get { return _transactions.DefaultTxConcurrency; }
            }

            /** <inheritDoc /> */
            public TransactionIsolation DefaultTxIsolation
            {
                get { return _transactions.DefaultTxIsolation; }
            }

            /** <inheritDoc /> */
            public TimeSpan DefaultTimeout
            {
                get { return _transactions.DefaultTimeout; }
            }

            /** <inheritDoc /> */
            public ITransactionClient TxStart(TransactionConcurrency concurrency,
                TransactionIsolation isolation,
                TimeSpan timeout,
                string label)
            {
                return _transactions.TxStart(concurrency, isolation, timeout, label);
            }
        }
    }
}
