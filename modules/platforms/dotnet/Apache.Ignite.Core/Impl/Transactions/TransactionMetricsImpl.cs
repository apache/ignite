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

namespace Apache.Ignite.Core.Impl.Transactions
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Transaction metrics.
    /// </summary>
    internal class TransactionMetricsImpl : ITransactionMetrics
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionMetricsImpl"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public TransactionMetricsImpl(IBinaryRawReader reader)
        {
            var commitTime = reader.ReadTimestamp();
            Debug.Assert(commitTime.HasValue);
            CommitTime = commitTime.Value;

            var rollbackTime = reader.ReadTimestamp();
            Debug.Assert(rollbackTime.HasValue);
            RollbackTime = rollbackTime.Value;

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
