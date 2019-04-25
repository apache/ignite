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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteTransactions;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxLocal;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;

/**
 * Extended interface to work with system transactions.
 */
public interface IgniteTransactionsEx extends IgniteTransactions {
    /**
     * @param ctx Cache context.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @param timeout Timeout.
     * @param txSize Number of entries participating in transaction (may be approximate).
     * @return New transaction.
     */
    public GridNearTxLocal txStartEx(GridCacheContext ctx,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        int txSize);

    /**
     * @param ctx Cache context.
     * @param concurrency Concurrency.
     * @param isolation Isolation.
     * @return New transaction.
     */
    public GridNearTxLocal txStartEx(GridCacheContext ctx,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation);
}