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
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Client.Transactions;

    /// <summary>
    /// Ignite Thin Client transaction facade.
    /// </summary>
    internal class ClientTransaction: IClientTransaction
    {
        /** Unique  transaction ID.*/
        private readonly int _id;

        /** Ignite. */
        private readonly IgniteClient _ignite;

        /** Transaction is closed. */
        private volatile bool _closed; 

        // ReSharper disable once InconsistentNaming
        /** Transaction for this thread. */
        [ThreadStatic]
        private static ClientTransaction THREAD_TX;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="id">ID.</param>
        public ClientTransaction(int id, IgniteClient ignite)
        {
            _id = id;
            _ignite = ignite;
            THREAD_TX = this;
        }

        public void Commit()
        {
            throw new System.NotImplementedException();
        }

        public Task CommitAsync()
        {
            throw new System.NotImplementedException();
        }

        public void Rollback()
        {
            throw new System.NotImplementedException();
        }

        public Task RollbackAsync()
        {
            throw new System.NotImplementedException();
        }

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

        private void Close(bool committed)
        {
            if (!_closed)
            {
                try
                {
                    _ignite.Socket.DoOutInOp<object>(ClientOp.TxEnd,
                        ctx =>
                        {
                            ctx.Writer.WriteBoolean(committed);
                            ctx.Writer.WriteInt(THREAD_TX._id);
                        },
                        null);
                }
                finally
                {
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