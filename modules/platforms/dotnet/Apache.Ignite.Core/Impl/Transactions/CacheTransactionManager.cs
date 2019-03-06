/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

namespace Apache.Ignite.Core.Impl.Transactions
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Threading;
    using System.Transactions;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Cache transaction enlistment manager, 
    /// allows using Ignite transactions via standard <see cref="TransactionScope"/>.
    /// </summary>
    // ReSharper disable once ClassNeverInstantiated.Global
    internal class CacheTransactionManager : IEnlistmentNotification
    {
        /** */
        private readonly ITransactions _transactions;

        /** */
        private static readonly ThreadLocal<Enlistment> Enlistment = new ThreadLocal<Enlistment>();

        /// <summary>
        /// Initializes a new instance of <see cref="CacheTransactionManager"/> class.
        /// </summary>
        /// <param name="transactions">Transactions.</param>
        public CacheTransactionManager(ITransactions transactions)
        {
            Debug.Assert(transactions != null);

            _transactions = transactions;
        }

        /// <summary>
        /// If ambient transaction is present, starts an Ignite transaction and enlists it.
        /// </summary>
        public void StartTx()
        {
            if (_transactions.Tx != null)
            {
                // Ignite transaction is already present.
                // We have either enlisted it already, or it has been started manually and should not be enlisted.
                // Java enlists existing Ignite tx in this case (see CacheJtaManager.java), but we do not.
                return;
            }

            if (Enlistment.Value != null)
            {
                // We are already enlisted.
                // .NET transaction mechanism allows nested transactions,
                // and they can be processed differently depending on TransactionScopeOption.
                // Ignite, however, allows only one active transaction per thread.
                // Therefore we enlist only once on the first transaction that we encounter.
                return;
            }

            var ambientTx = System.Transactions.Transaction.Current;

            if (ambientTx != null && ambientTx.TransactionInformation.Status == TransactionStatus.Active)
            {
                _transactions.TxStart(_transactions.DefaultTransactionConcurrency, 
                    ConvertTransactionIsolation(ambientTx.IsolationLevel));

                Enlistment.Value = ambientTx.EnlistVolatile(this, EnlistmentOptions.None);
            }
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        void IEnlistmentNotification.Prepare(PreparingEnlistment preparingEnlistment)
        {
            Debug.Assert(preparingEnlistment != null);

            var igniteTx = _transactions.Tx;

            if (igniteTx != null && Enlistment.Value != null)
            {
                ((Transaction) igniteTx).Prepare();
            }

            preparingEnlistment.Prepared();
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        void IEnlistmentNotification.Commit(Enlistment enlistment)
        {
            Debug.Assert(enlistment != null);

            var igniteTx = _transactions.Tx;

            if (igniteTx != null && Enlistment.Value != null)
            {
                Debug.Assert(ReferenceEquals(enlistment, Enlistment.Value));

                igniteTx.Commit();

                igniteTx.Dispose();

                Enlistment.Value = null;
            }

            enlistment.Done();
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        void IEnlistmentNotification.Rollback(Enlistment enlistment)
        {
            Debug.Assert(enlistment != null);

            var igniteTx = _transactions.Tx;

            if (igniteTx != null && Enlistment.Value != null)
            {
                igniteTx.Rollback();

                igniteTx.Dispose();

                Enlistment.Value = null;
            }

            enlistment.Done();
        }

        /** <inheritdoc /> */
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods")]
        void IEnlistmentNotification.InDoubt(Enlistment enlistment)
        {
            Debug.Assert(enlistment != null);

            enlistment.Done();
        }

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