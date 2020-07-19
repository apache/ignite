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
    using System.ComponentModel;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Transactions;

    /// <summary>
    /// Transactions configuration.
    /// </summary>
    public class TransactionClientConfiguration
    {
        /// <summary> The default value for <see cref="DefaultTransactionConcurrency"/> property. </summary>
        public const TransactionConcurrency DefaultDefaultTransactionConcurrency = TransactionConcurrency.Pessimistic;

        /// <summary> The default value for <see cref="DefaultTransactionIsolation"/> property. </summary>
        public const TransactionIsolation DefaultDefaultTransactionIsolation = TransactionIsolation.RepeatableRead;

        /// <summary> The default value for <see cref="DefaultTransactionIsolation"/> property. </summary>
        public static readonly TimeSpan DefaultDefaultTimeout = TimeSpan.Zero;

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
        /// Initializes a new instance of the <see cref="TransactionClientConfiguration" /> class.
        /// </summary>
        public TransactionClientConfiguration()
        {
            DefaultTransactionConcurrency = DefaultDefaultTransactionConcurrency;
            DefaultTransactionIsolation = DefaultDefaultTransactionIsolation;
            DefaultTimeout = DefaultDefaultTimeout;
        }
        
        /// <summary>
        /// Initializes a new instance of the <see cref="TransactionClientConfiguration" /> class.
        /// </summary>
        /// <param name="cfg">The configuration to copy.</param>
        public TransactionClientConfiguration(TransactionClientConfiguration cfg)
        {
            IgniteArgumentCheck.NotNull(cfg, "configuration");

            DefaultTransactionConcurrency = cfg.DefaultTransactionConcurrency;
            DefaultTransactionIsolation = cfg.DefaultTransactionIsolation;
            DefaultTimeout =cfg.DefaultTimeout;
        }
    }
}
