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

namespace Apache.Ignite.Core.Transactions
{
    using System;
    using System.ComponentModel;
    using Apache.Ignite.Core.Common;

    /// <summary>
    /// Transactions configuration.
    /// </summary>
    public class TransactionConfiguration
    {
        /// <summary> The default value for <see cref="DefaultTransactionConcurrency"/> property. </summary>
        public const TransactionConcurrency DefaultDefaultTransactionConcurrency = TransactionConcurrency.Pessimistic;

        /// <summary> The default value for <see cref="DefaultTransactionIsolation"/> property. </summary>
        public const TransactionIsolation DefaultDefaultTransactionIsolation = TransactionIsolation.RepeatableRead;

        /// <summary> The default value for <see cref="DefaultTransactionIsolation"/> property. </summary>
        public static readonly TimeSpan DefaultDefaultTimeout = TimeSpan.Zero;

        /// <summary> The default value for <see cref="PessimisticTransactionLogSize"/> property. </summary>
        public const int DefaultPessimisticTransactionLogSize = 0;

        /// <summary> The default value for <see cref="PessimisticTransactionLogLinger"/> property. </summary>
        public static readonly TimeSpan DefaultPessimisticTransactionLogLinger = TimeSpan.FromMilliseconds(10000);

        /// <summary> The default value for <see cref="DefaultTimeoutOnPartitionMapExchange"/></summary>
        public static readonly TimeSpan DefaultDefaultTimeoutOnPartitionMapExchange = TimeSpan.Zero;

        /// <summary> The default value for <see cref="DeadlockTimeout"/></summary>
        public static readonly TimeSpan DefaultDeadlockTimeout = TimeSpan.FromSeconds(10);

        /// <summary>
        /// Gets or sets the cache transaction concurrency to use when one is not explicitly specified.
        /// </summary>
        [DefaultValue(DefaultDefaultTransactionConcurrency)]
        public TransactionConcurrency DefaultTransactionConcurrency { get; set; }

        /// <summary>
        /// Gets or sets the cache transaction isolation to use when one is not explicitly specified.
        /// </summary>
        [DefaultValue(DefaultDefaultTransactionIsolation)]
        public TransactionIsolation DefaultTransactionIsolation { get; set; }

        /// <summary>
        /// Gets or sets the cache transaction timeout to use when one is not explicitly specified.
        /// <see cref="TimeSpan.Zero"/> for infinite timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:00")]
        public TimeSpan DefaultTimeout { get; set; }

        /// <summary>
        /// Gets or sets the size of pessimistic transactions log stored on node in order to recover
        /// transaction commit if originating node has left grid before it has sent all messages to transaction nodes.
        /// <code>0</code> for unlimited.
        /// </summary>
        [DefaultValue(DefaultPessimisticTransactionLogSize)]
        public int PessimisticTransactionLogSize { get; set; }

        /// <summary>
        /// Gets or sets the delay after which pessimistic recovery entries will be cleaned up for failed node.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:10")]
        public TimeSpan PessimisticTransactionLogLinger { get; set; }

        /// <summary>
        /// Gets or sets transaction timeout for partition map synchronization.
        /// <see cref="TimeSpan.Zero"/> for infinite timeout.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:00")]
        public TimeSpan DefaultTimeoutOnPartitionMapExchange { get; set; }

        /// <summary>
        /// This is an experimental feature. Transactional SQL is currently in a beta status.
        /// <para/>
        /// Gets or sets timeout before starting deadlock detection for caches configured with
        /// <code>TransactionalSnapshot</code> cache atomicity mode.
        /// <see cref="TimeSpan.Zero"/> for disabling deadlock detection.
        /// </summary>
        [DefaultValue(typeof(TimeSpan), "00:00:10")]
        [IgniteExperimentalAttribute]
        public TimeSpan DeadlockTimeout { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionConfiguration" /> class.
        /// </summary>
        public TransactionConfiguration()
        {
            DefaultTransactionConcurrency = DefaultDefaultTransactionConcurrency;
            DefaultTransactionIsolation = DefaultDefaultTransactionIsolation;
            DefaultTimeout = DefaultDefaultTimeout;
            PessimisticTransactionLogSize = DefaultPessimisticTransactionLogSize;
            PessimisticTransactionLogLinger = DefaultPessimisticTransactionLogLinger;
            DefaultTimeoutOnPartitionMapExchange = DefaultDefaultTimeoutOnPartitionMapExchange;
            DeadlockTimeout = DefaultDeadlockTimeout;
        }
    }
}
