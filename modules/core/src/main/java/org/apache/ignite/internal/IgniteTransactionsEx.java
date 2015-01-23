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

package org.apache.ignite.internal;

import org.apache.ignite.*;
import org.apache.ignite.transactions.*;

/**
 * Extended interface to work with system transactions.
 */
public interface IgniteTransactionsEx extends IgniteTransactions {
    /**
     * Starts transaction with specified isolation, concurrency, timeout, invalidation flag,
     * and number of participating entries.
     *
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Number of entries participating in transaction (may be approximate).
     * @return New transaction.
     * @throws IllegalStateException If transaction is already started by this thread.
     * @throws UnsupportedOperationException If cache is {@link org.apache.ignite.cache.CacheAtomicityMode#ATOMIC}.
     */
    public IgniteTx txStartSystem(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation, long timeout,
        int txSize);
}
