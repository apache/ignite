/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Internal.Transactions
{
    using System.Threading;
    using System.Threading.Tasks;
    using System.Transactions;
    using Buffers;
    using Ignite.Transactions;
    using MessagePack;
    using Proto;

    /// <summary>
    /// Ignite transaction.
    /// </summary>
    internal class Transaction : ITransaction
    {
        /** Open state. */
        private const int StateOpen = 0;

        /** Committed state. */
        private const int StateCommitted = 1;

        /** Rolled back state. */
        private const int StateRolledBack = 2;

        /** State. */
        private int _state = StateOpen;

        /// <summary>
        /// Initializes a new instance of the <see cref="Transaction"/> class.
        /// </summary>
        /// <param name="id">Transaction id.</param>
        /// <param name="socket">Associated connection.</param>
        /// <param name="failoverSocket">Associated connection multiplexer.</param>
        public Transaction(long id, ClientSocket socket, ClientFailoverSocket failoverSocket)
        {
            Id = id;
            Socket = socket;
            FailoverSocket = failoverSocket;
        }

        /// <summary>
        /// Gets the owner socket.
        /// </summary>
        public ClientSocket Socket { get; }

        /// <summary>
        /// Gets the owner multiplexer socket.
        /// </summary>
        public ClientFailoverSocket FailoverSocket { get; }

        /// <summary>
        /// Gets the transaction id.
        /// </summary>
        public long Id { get; }

        /// <inheritdoc/>
        public async Task CommitAsync()
        {
            SetState(StateCommitted);

            using var writer = new PooledArrayBufferWriter();
            Write(writer.GetMessageWriter());

            await Socket.DoOutInOpAsync(ClientOp.TxCommit, writer).ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async Task RollbackAsync()
        {
            SetState(StateRolledBack);

            await RollbackAsyncInternal().ConfigureAwait(false);
        }

        /// <inheritdoc/>
        public async ValueTask DisposeAsync()
        {
            // Roll back if the transaction is still open, otherwise do nothing.
            if (TrySetState(StateRolledBack))
            {
                await RollbackAsyncInternal().ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Rolls back the transaction without state check.
        /// </summary>
        private async Task RollbackAsyncInternal()
        {
            using var writer = new PooledArrayBufferWriter();
            Write(writer.GetMessageWriter());

            await Socket.DoOutInOpAsync(ClientOp.TxRollback, writer).ConfigureAwait(false);
        }

        /// <summary>
        /// Attempts to set the specified state.
        /// </summary>
        /// <param name="state">State to set.</param>
        /// <returns>True when specified state was set successfully; false otherwise.</returns>
        private bool TrySetState(int state)
        {
            return Interlocked.CompareExchange(ref _state, state, StateOpen) == StateOpen;
        }

        /// <summary>
        /// Sets the specified state. Throws <see cref="TransactionException"/> when current state is different from
        /// <see cref="StateOpen"/>.
        /// </summary>
        /// <param name="state">State to set.</param>
        /// <exception cref="TransactionException">When current state is not <see cref="StateOpen"/>.</exception>
        private void SetState(int state)
        {
            var oldState = Interlocked.CompareExchange(ref _state, state, StateOpen);

            if (oldState == StateOpen)
            {
                return;
            }

            var message = oldState == StateCommitted
                ? "Transaction is already committed."
                : "Transaction is already rolled back.";

            throw new TransactionException(message);
        }

        /// <summary>
        /// Writes the transaction.
        /// </summary>
        /// <param name="writer">Writer.</param>
        private void Write(MessagePackWriter writer)
        {
            writer.Write(Id);
            writer.Flush();
        }
    }
}
