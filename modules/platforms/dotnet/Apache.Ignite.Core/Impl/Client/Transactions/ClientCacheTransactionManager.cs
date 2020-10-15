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
    using System.Transactions;
    using Apache.Ignite.Core.Client.Transactions;
    using Apache.Ignite.Core.Impl.Transactions;

    /// <summary>
    /// Cache transaction enlistment manager, 
    /// allows using Ignite transactions via standard <see cref="TransactionScope"/>.
    /// </summary>
    internal class ClientCacheTransactionManager : ISinglePhaseNotification, IDisposable
    {
        /** */
        private ITransactionsClient _transactions;

        /** */
        private ThreadLocal<Enlistment> _enlistment = new ThreadLocal<Enlistment>();

        /// <summary>
        /// Initializes a new instance of <see cref="ClientCacheTransactionManager"/> class.
        /// </summary>
        /// <param name="transactions">Transactions.</param>
        public ClientCacheTransactionManager(ITransactionsClient transactions)
        {
            _transactions = transactions;
        }

        /// <summary>
        /// If ambient transaction is present, starts an Ignite transaction and enlists it.
        /// </summary>
        public void StartTxIfNeeded()
        {
            if (_transactions.Tx != null)
            {
                // Ignite transaction is already present.
                // We have either enlisted it already, or it has been started manually and should not be enlisted.
                // Java enlists existing Ignite tx in this case (see CacheJtaManager.java), but we do not.
                return;
            }

            var ambientTx = System.Transactions.Transaction.Current;

            if (ambientTx != null && ambientTx.TransactionInformation.Status == TransactionStatus.Active)
            {
                _transactions.TxStart(_transactions.DefaultTransactionConcurrency,
                    CacheTransactionManager.ConvertTransactionIsolation(ambientTx.IsolationLevel),
                    _transactions.DefaultTimeout);
                _enlistment.Value = ambientTx.EnlistVolatile(this, EnlistmentOptions.None);
            }
        }

        /** <inheritDoc /> */
        void IEnlistmentNotification.Prepare(PreparingEnlistment preparingEnlistment)
        {
            preparingEnlistment.Prepared();
        }

        /** <inheritDoc /> */
        void IEnlistmentNotification.Commit(Enlistment enlistment)
        {
            Debug.Assert(enlistment != null);

            var igniteTx = _transactions.Tx;

            if (igniteTx != null && _enlistment.Value != null)
            {
                Debug.Assert(ReferenceEquals(enlistment, _enlistment.Value));

                igniteTx.Commit();
                igniteTx.Dispose();
                _enlistment.Value = null;
            }

            enlistment.Done();
        }

        /** <inheritDoc /> */
        void IEnlistmentNotification.Rollback(Enlistment enlistment)
        {
            Debug.Assert(enlistment != null);

            var igniteTx = _transactions.Tx;

            if (igniteTx != null && _enlistment.Value != null)
            {
                igniteTx.Rollback();
                igniteTx.Dispose();
                _enlistment.Value = null;
            }

            enlistment.Done();
        }

        /** <inheritDoc /> */
        void IEnlistmentNotification.InDoubt(Enlistment enlistment)
        {
            Debug.Assert(enlistment != null);

            enlistment.Done();
        }

        /** <inheritDoc /> */
        void ISinglePhaseNotification.SinglePhaseCommit(SinglePhaseEnlistment enlistment)
        {
            Debug.Assert(enlistment != null);

            var igniteTx = _transactions.Tx;

            if (igniteTx != null && _enlistment.Value != null)
            {
                igniteTx.Commit();
                igniteTx.Dispose();
                _enlistment.Value = null;
            }

            enlistment.Committed();
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Usage",
            "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            _transactions = null;
            var localEnlistment = _enlistment;
            if (localEnlistment != null)
            {
                localEnlistment.Dispose();
                _enlistment = null;
            }
        }
    }
}
