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

namespace Apache.Ignite.Core.Client.Transactions
{
    using System;
    using System.Transactions;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Ignite Thin Client transactions facade.
    /// <para />
    /// Transactions are bound to the thread started the transaction. After that, each cache operation within this thread
    /// will belong to the corresponding transaction until the transaction is committed, rolled back or closed.
    /// <para />
    /// Should not be used with async calls.
    /// <example>
    ///     You can use cache transactions as follows:
    ///     <code>
    ///     using (var tx = igniteClient.GetTransactions().TxStart())
    ///     {
    ///         int v1 = cache&lt;string, int&gt;.Get("k1");
    ///
    ///         // Check if v1 satisfies some condition before doing a put.
    ///         if (v1 > 0)
    ///             cache.Put&lt;string, int&gt;("k1", 2);
    ///
    ///         cache.Remove("k2");
    ///
    ///         // Commit the transaction.
    ///         tx.Commit();
    ///     }
    ///     </code>
    /// </example>
    ///
    /// Alternatively, <see cref="TransactionScope"/> can be used to start Ignite transactions.
    /// <example>
    ///     <code>
    ///     using (var ts = new TransactionScope())
    ///     {
    ///         int v1 = cache&lt;string, int&gt;.Get("k1");
    ///
    ///         // Check if v1 satisfies some condition before doing a put.
    ///         if (v1 > 0)
    ///             cache.Put&lt;string, int&gt;("k1", 2);
    ///
    ///         cache.Remove("k2");
    ///
    ///         // Commit the transaction.
    ///         ts.Complete();
    ///     }
    ///     </code>
    /// </example>
    /// </summary>
    public interface ITransactionsClient
    {
        /// <summary>
        /// Gets transaction started by this thread or null if this thread does not have a transaction.
        /// </summary>
        /// <value>
        /// Transaction started by this thread or null if this thread does not have a transaction.
        /// </value>
        ITransactionClient Tx { get; }

        /// <summary>
        /// Gets the default transaction concurrency.
        /// </summary>
        TransactionConcurrency DefaultTransactionConcurrency { get; }

        /// <summary>
        /// Gets the default transaction isolation.
        /// </summary>
        TransactionIsolation DefaultTransactionIsolation { get; }

        /// <summary>
        /// Gets the default transaction timeout.
        /// </summary>
        TimeSpan DefaultTimeout { get; }

        /// <summary>
        /// Starts a new transaction with the default isolation level, concurrency and timeout.
        /// <para />
        /// Default values for transaction isolation level, concurrency and timeout can be configured via
        /// <see cref="TransactionClientConfiguration" />.
        /// <para />
        /// Should not be used with async calls.
        /// </summary>
        /// <returns>New transaction.</returns>
        ITransactionClient TxStart();

        /// <summary>
        /// Starts a new transaction with the specified concurrency and isolation.
        /// <para />
        /// Should not be used with async calls.
        /// </summary>
        /// <param name="concurrency">Concurrency.</param>
        /// <param name="isolation">Isolation.</param>
        /// <returns>New transaction.</returns>
        ITransactionClient TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation);

        /// <summary>
        /// Starts a new transaction with the specified concurrency, isolation and timeout.
        /// <para />
        /// Should not be used with async calls.
        /// </summary>
        /// <param name="concurrency">Concurrency.</param>
        /// <param name="isolation">Isolation.</param>
        /// <param name="timeout">Timeout. TimeSpan. Zero for indefinite timeout.</param>
        /// <returns>New transaction.</returns>
        ITransactionClient TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
            TimeSpan timeout);

        /// <summary>
        /// Returns instance of <see cref="ITransactionsClient" /> to mark a transaction instance with a special label.
        /// The label is helpful for diagnostic and exposed to some diagnostic tools like
        /// SYS.TRANSACTIONS system view, control.sh commands, JMX TransactionsMXBean,
        /// long-running transactions dump in logs
        /// and <see cref="ITransaction.Label" /> via <see cref="ITransactions.GetLocalActiveTransactions" />.
        /// </summary>
        /// <param name="label">Label.</param>
        /// <returns>
        /// <see cref="Apache.Ignite.Core.Client.Transactions.ITransactionsClient" />
        /// </returns>
        ITransactionsClient WithLabel(string label);
    }
}
