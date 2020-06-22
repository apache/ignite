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
    using System.Threading;

    /// <summary>
    /// Ignite Thin Client transaction facade.
    /// </summary>
    internal class ClientTransaction: IClientTransactionInternal
    {
        /** Unique  transaction ID.*/
        private readonly int _id;

        /** Ignite. */
        private readonly IgniteClient _ignite;

        /* Transactions. */
        private readonly ClientTransactions _transactions;

        /** Transaction is closed. */
        private volatile bool _closed; 

        /** Owning thread ID. */
        private readonly int _threadId;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        /// <param name="ignite"></param>
        /// <param name="transactions"></param>
        public ClientTransaction(int id, IgniteClient ignite, ClientTransactions transactions)
        {
            _id = id;
            _ignite = ignite;
            _transactions = transactions;
            _threadId = Thread.CurrentThread.ManagedThreadId;
        }

        /** <inheritdoc /> */
        public void Commit()
        {
            Close(true);
        }

        /** <inheritdoc /> */
        public void Rollback()
        {
            Close(false);
        }

        /** <inheritdoc /> */
        public void Dispose()
        {
            try
            {
                Close(false);
            }
            finally
            {
                GC.SuppressFinalize(this);
            }
        }

        /** <inheritdoc /> */
        public int Id
        {
            get { return _id; }
        }

        /** <inheritdoc /> */
        public bool Closed
        {
            get { return _closed; }
        }

        /// <summary>
        /// Closes the transaction. 
        /// </summary>
        private void Close(bool commit)
        {
            if (!_closed)
            {
                try
                {
                    _ignite.Socket.DoOutInOp<object>(ClientOp.TxEnd,
                        ctx =>
                        {
                            ctx.Writer.WriteInt(_id);
                            ctx.Writer.WriteBoolean(commit);
                        },
                        null);
                }
                finally
                {
                    // _transactions.ClearCurrentTx();
                    _closed = true;
                }
            }
        }

        /** <inheritdoc /> */
        ~ClientTransaction()
        {
            Dispose();
        }
    }
}