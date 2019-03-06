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
    using System.Diagnostics.CodeAnalysis;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Ignite transaction facade.
    /// </summary>
    internal sealed class Transaction : ITransaction
    {
        /** */
        private readonly TransactionImpl _tx;

        /// <summary>
        /// Initializes a new instance of the <see cref="Transaction" /> class.
        /// </summary>
        /// <param name="tx">The tx to wrap.</param>
        public Transaction(TransactionImpl tx)
        {
            _tx = tx;
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Usage", "CA1816:CallGCSuppressFinalizeCorrectly",
            Justification = "There is no finalizer.")]
        public void Dispose()
        {
            _tx.Dispose();
        }

        /** <inheritDoc /> */
        public Guid NodeId
        {
            get { return _tx.NodeId; }
        }

        /** <inheritDoc /> */
        public long ThreadId
        {
            get { return _tx.ThreadId; }
        }

        /** <inheritDoc /> */
        public DateTime StartTime
        {
            get { return _tx.StartTime; }
        }

        /** <inheritDoc /> */
        public TransactionIsolation Isolation
        {
            get { return _tx.Isolation; }
        }

        /** <inheritDoc /> */
        public TransactionConcurrency Concurrency
        {
            get { return _tx.Concurrency; }
        }

        /** <inheritDoc /> */
        public TransactionState State
        {
            get { return _tx.State; }
        }

        /** <inheritDoc /> */
        public TimeSpan Timeout
        {
            get { return _tx.Timeout; }
        }

        /** <inheritDoc /> */
        public string Label
        {
            get { return _tx.Label; } 
        }

        /** <inheritDoc /> */
        public bool IsRollbackOnly
        {
            get { return _tx.IsRollbackOnly; }
        }

        /** <inheritDoc /> */
        public bool SetRollbackonly()
        {
            return _tx.SetRollbackOnly();
        }

        /** <inheritDoc /> */
        public void Commit()
        {
            _tx.Commit();
        }

        /** <inheritDoc /> */
        public Task CommitAsync()
        {
            return _tx.GetTask(() => _tx.CommitAsync());
        }

        /** <inheritDoc /> */
        public void Rollback()
        {
            _tx.Rollback();
        }

        /** <inheritDoc /> */
        public Task RollbackAsync()
        {
            return _tx.GetTask(() => _tx.RollbackAsync());
        }

        /** <inheritDoc /> */
        public void AddMeta<TV>(string name, TV val)
        {
            _tx.AddMeta(name, val);
        }

        /** <inheritDoc /> */
        public TV Meta<TV>(string name)
        {
            return _tx.Meta<TV>(name);
        }

        /** <inheritDoc /> */
        public TV RemoveMeta<TV>(string name)
        {
            return _tx.RemoveMeta<TV>(name);
        }

        /// <summary>
        /// Executes prepare step of the two phase commit.
        /// </summary>
        public void Prepare()
        {
            _tx.Prepare();
        }
    }
}