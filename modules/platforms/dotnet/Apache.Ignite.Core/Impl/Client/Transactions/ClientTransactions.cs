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
    using Apache.Ignite.Core.Client.Transactions;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Ignite Thin Client transactions facade.
    /// </summary>
    internal class ClientTransactions : IClientTransactions
    {
        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Label. */
        private readonly string _label;

        /** Default transaction concurrency. */
        private const TransactionConcurrency _dfltConcurrency = TransactionConcurrency.Pessimistic;

        /** Default transaction isolation. */
        private const TransactionIsolation _dfltIsolation = TransactionIsolation.RepeatableRead;

        /** Default transaction timeout. */
        private readonly TimeSpan _dfltTimeout = TimeSpan.Zero;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="ignite">Ignite.</param>
        /// <param name="label">Label.</param>
        public ClientTransactions(IgniteClient ignite, string label = null)
        {
            _ignite = ignite;
            _label = label;
        }

        /** <inheritDoc /> */
        public IClientTransaction TxStart()
        {
            return TxStart(_dfltConcurrency, _dfltIsolation);
        }

        /** <inheritDoc /> */
        public IClientTransaction TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation)
        {
            return TxStart(concurrency, isolation, _dfltTimeout);
        }

        /** <inheritDoc /> */
        public IClientTransaction TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
            TimeSpan timeout)
        {
            var txId = _ignite.Socket.DoOutInOp(
                ClientOp.TxStart,
                ctx =>
                {
                    ctx.Writer.WriteByte((byte) concurrency);
                    ctx.Writer.WriteByte((byte) isolation);
                    ctx.Writer.WriteTimeSpanAsLong(timeout);
                    ctx.Writer.WriteString(_label);
                },
                ctx => ctx.Reader.ReadInt()
            );
            
            return new ClientTransaction(txId, _ignite);
        }

        /** <inheritDoc /> */
        public IClientTransactions WithLabel(string label)
        {
            return new ClientTransactions(_ignite, _label); 
        }
    }
}