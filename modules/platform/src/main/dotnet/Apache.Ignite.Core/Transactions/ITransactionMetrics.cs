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
    /// Transaction metrics, shared across all caches.
    /// </summary>
    public interface ITransactionMetrics
    {
        /// <summary>
        /// Gets the last time transaction was committed.
        /// </summary>
        DateTime CommitTime { get; }

        /// <summary>
        /// Gets the last time transaction was rolled back.
        /// </summary>
        DateTime RollbackTime { get; }

        /// <summary>
        /// Gets the total number of transaction commits.
        /// </summary>
        int TxCommits { get; }

        /// <summary>
        /// Gets the total number of transaction rollbacks.
        /// </summary>
        int TxRollbacks { get; }
    }
}