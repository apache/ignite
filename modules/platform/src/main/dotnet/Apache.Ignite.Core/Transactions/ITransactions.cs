/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Transactions
{
    using System;

    /// <summary>
    /// Transactions facade.
    /// <para/>
    /// All members are thread-safe and may be used concurrently from multiple threads.
    /// </summary>
    public interface ITransactions
    {
        /// <summary>
        /// Starts a transaction with default isolation, concurrency, timeout, and invalidation policy.
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
        /// <param name="timeout">Timeout.</param>
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
        /// Gets the metrics.
        /// </summary>
        ITransactionMetrics GetMetrics();

        /// <summary>
        /// Resets the metrics.
        /// </summary>
        void ResetMetrics();
    }
}