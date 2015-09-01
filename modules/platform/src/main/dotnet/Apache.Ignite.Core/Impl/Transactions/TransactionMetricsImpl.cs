/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace Apache.Ignite.Core.Impl.Transactions
{
    using System;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Transaction metrics.
    /// </summary>
    public class TransactionMetricsImpl : ITransactionMetrics
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionMetricsImpl"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public TransactionMetricsImpl(IPortableRawReader reader)
        {
            CommitTime = reader.ReadDate() ?? default(DateTime);
            RollbackTime = reader.ReadDate() ?? default(DateTime);

            TxCommits = reader.ReadInt();
            TxRollbacks = reader.ReadInt();
        }

        /// <summary>
        /// Gets the last time transaction was committed.
        /// </summary>
        public DateTime CommitTime { get; private set; }

        /// <summary>
        /// Gets the last time transaction was rolled back.
        /// </summary>
        public DateTime RollbackTime { get; private set; }

        /// <summary>
        /// Gets the total number of transaction commits.
        /// </summary>
        public int TxCommits { get; private set; }

        /// <summary>
        /// Gets the total number of transaction rollbacks.
        /// </summary>
        public int TxRollbacks { get; private set; }
    }
}
