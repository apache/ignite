/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Transactions
{
    using System;
    using System.Diagnostics.CodeAnalysis;

    /// <summary>
    /// Transactions facade.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface ITransactions
    {
        /// <summary>
        /// Starts a transaction with default isolation (<see cref="DefaultTransactionIsolation"/>, 
        /// concurrency (<see cref="DefaultTransactionConcurrency"/>), timeout (<see cref="DefaultTimeout"/>), 
        /// and invalidation policy.
        /// All defaults are set in CacheConfiguration at startup.
        /// </summary>
        /// <returns>New transaction.</returns>
        ITransaction TxStart();

        /// <summary>
        /// Starts new transaction with the specified concurrency and isolation.
        /// </summary>
        /// <param name="concurrency">Concurrency.</param>
        /// <param name="isolation">Isolation.</param>
        /// <returns>New transaction.</returns>
        ITransaction TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation);

        /// <summary>
        /// Starts new transaction with the specified concurrency and isolation.
        /// </summary>
        /// <param name="concurrency">Concurrency.</param>
        /// <param name="isolation">Isolation.</param>
        /// <param name="timeout">Timeout. TimeSpan.Zero for indefinite timeout.</param>
        /// <param name="txSize">Number of entries participating in transaction (may be approximate).</param>
        /// <returns>New transaction.</returns>
        ITransaction TxStart(TransactionConcurrency concurrency, TransactionIsolation isolation, 
            TimeSpan timeout, int txSize);

        /// <summary>
        /// Gets transaction started by this thread or null if this thread does not have a transaction.
        /// </summary>
        /// <value>
        /// Transaction started by this thread or null if this thread does not have a transaction.
        /// </value>
        ITransaction Tx { get; }

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
        /// Gets the default transaction timeout on partition map exchange.
        /// </summary>
        TimeSpan DefaultTimeoutOnPartitionMapExchange { get; }

        /// <summary>
        /// Gets the metrics.
        /// </summary>
        [SuppressMessage("Microsoft.Design", "CA1024:UsePropertiesWhereAppropriate", 
            Justification = "Expensive operation.")]
        ITransactionMetrics GetMetrics();

        /// <summary>
        /// Resets the metrics.
        /// </summary>
        void ResetMetrics();

        /// <summary>
        /// Returns instance of Ignite Transactions to mark a transaction with a special label.
        /// </summary>
        /// <param name="label"></param>
        /// <returns><see cref="ITransactions"/></returns>
        ITransactions WithLabel(string label);

        /// <summary>
        /// Returns a list of active transactions initiated by this node.
        /// </summary>
        /// <returns>Collection of <see cref="ITransactionCollection"/></returns>
        ITransactionCollection GetLocalActiveTransactions();

        /// <summary>
        /// Returns instance of Ignite Transactions to enable tracing for a transaction.
        /// </summary>
        /// <returns><see cref="ITransactions"/></returns>
        ITransactions WithTracing();
    }
}
