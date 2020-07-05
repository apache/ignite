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
    using System.Threading;
    using System.Transactions;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Cache transaction enlistment manager, 
    /// allows using Ignite transactions via standard <see cref="TransactionScope"/>.
    /// </summary>
    internal class ClientCacheTransactionManager : IEnlistmentNotification
    {
        /** */
        private readonly IClientTransactionsInternal _transactions;

        /** */
        private readonly ThreadLocal<Enlistment> _enlistment = new ThreadLocal<Enlistment>();

        /// <summary>
        /// Initializes a new instance of <see cref="ClientCacheTransactionManager"/> class.
        /// </summary>
        /// <param name="transactions">Transactions.</param>
        public ClientCacheTransactionManager(IClientTransactionsInternal transactions)
        {
            _transactions = transactions;
        }

        /// <summary>
        /// If ambient transaction is present, starts an Ignite transaction and enlists it.
        /// </summary>
        /// <param name="label">Label.</param>
        public void StartTxIfNeeded()
        {
            if (_transactions.CurrentTx != null)
            {
                // Ignite transaction is already present.
                // We have either enlisted it already, or it has been started manually and should not be enlisted.
                // Java enlists existing Ignite tx in this case (see CacheJtaManager.java), but we do not.
                return;
            }

            if (_enlistment.Value != null)
            {
                // We are already enlisted.
                // .NET transaction mechanism allows nested transactions,
                // and they can be processed differently depending on TransactionScopeOption.
                // Ignite, however, allows only one active transaction per thread.
                // Therefore we enlist only once on the first transaction that we encounter.
                return;
            }

            var ambientTx = Transaction.Current;

            if (ambientTx != null && ambientTx.TransactionInformation.Status == TransactionStatus.Active)
            {
                _transactions.TxStart(_transactions.DefaultTxConcurrency,
                    ConvertTransactionIsolation(ambientTx.IsolationLevel),
                    _transactions.DefaultTimeout);

                _enlistment.Value = ambientTx.EnlistVolatile(this, EnlistmentOptions.None);
            }
        }

        void IEnlistmentNotification.Prepare(PreparingEnlistment preparingEnlistment)
        {
            preparingEnlistment.Prepared();
        }

        void IEnlistmentNotification.Commit(Enlistment enlistment)
        {
            Debug.Assert(enlistment != null);

            var igniteTx = _transactions.CurrentTx;

            if (igniteTx != null && _enlistment.Value != null)
            {
                Debug.Assert(ReferenceEquals(enlistment, _enlistment.Value));

                igniteTx.Commit();
                igniteTx.Dispose();
                _enlistment.Value = null;
            }

            enlistment.Done();
        }

        void IEnlistmentNotification.Rollback(Enlistment enlistment)
        {
            Debug.Assert(enlistment != null);

            var igniteTx = _transactions.CurrentTx;

            if (igniteTx != null && _enlistment.Value != null)
            {
                igniteTx.Rollback();
                igniteTx.Dispose();
                _enlistment.Value = null;
            }

            enlistment.Done();
        }

        void IEnlistmentNotification.InDoubt(Enlistment enlistment)
        {
            Debug.Assert(enlistment != null);

            enlistment.Done();
        }

        // TODO
        /// <summary>
        /// Converts the isolation level from .NET-specific to Ignite-specific.
        /// </summary>
        private static TransactionIsolation ConvertTransactionIsolation(IsolationLevel isolation)
        {
            switch (isolation)
            {
                case IsolationLevel.Serializable:
                    return TransactionIsolation.Serializable;
                case IsolationLevel.RepeatableRead:
                    return TransactionIsolation.RepeatableRead;
                case IsolationLevel.ReadCommitted:
                case IsolationLevel.ReadUncommitted:
                case IsolationLevel.Snapshot:
                case IsolationLevel.Chaos:
                    return TransactionIsolation.ReadCommitted;
                default:
                    throw new ArgumentOutOfRangeException("isolation", isolation, 
                        "Unsupported transaction isolation level: " + isolation);
            }
        }
    }
}