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

package org.apache.ignite.internal.processors.cache.transactions;

import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.apache.ignite.internal.IgniteTransactionsEx;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.transactions.Transaction;
import org.apache.ignite.transactions.TransactionConcurrency;
import org.apache.ignite.transactions.TransactionIsolation;
import org.apache.ignite.transactions.TransactionMetrics;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.transactions.TransactionIsolation.SERIALIZABLE;

/**
 * Grid transactions implementation.
 */
public class IgniteTransactionsImpl<K, V> implements IgniteTransactionsEx {
    /** Cache shared context. */
    private GridCacheSharedContext<K, V> cctx;

    /**
     * @param cctx Cache shared context.
     */
    public IgniteTransactionsImpl(GridCacheSharedContext<K, V> cctx) {
        this.cctx = cctx;
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart() throws IllegalStateException {
        TransactionConfiguration cfg = cctx.gridConfig().getTransactionConfiguration();

        return txStart0(
            cfg.getDefaultTxConcurrency(),
            cfg.getDefaultTxIsolation(),
            cfg.getDefaultTxTimeout(),
            0,
            null
        ).proxy();
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation) {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");

        TransactionConfiguration cfg = cctx.gridConfig().getTransactionConfiguration();

        return txStart0(
            concurrency,
            isolation,
            cfg.getDefaultTxTimeout(),
            0,
            null
        ).proxy();
    }

    /** {@inheritDoc} */
    @Override public Transaction txStart(TransactionConcurrency concurrency, TransactionIsolation isolation,
        long timeout, int txSize) {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");
        A.ensure(timeout >= 0, "timeout cannot be negative");
        A.ensure(txSize >= 0, "transaction size cannot be negative");

        return txStart0(
            concurrency,
            isolation,
            timeout,
            txSize,
            null
        ).proxy();
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalTx txStartEx(
        GridCacheContext ctx,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation,
        long timeout,
        int txSize)
    {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");
        A.ensure(timeout >= 0, "timeout cannot be negative");
        A.ensure(txSize >= 0, "transaction size cannot be negative");

        checkTransactional(ctx);

        return txStart0(concurrency,
            isolation,
            timeout,
            txSize,
            ctx.systemTx() ? ctx : null);
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalTx txStartEx(
        GridCacheContext ctx,
        TransactionConcurrency concurrency,
        TransactionIsolation isolation)
    {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");

        checkTransactional(ctx);

        TransactionConfiguration cfg = cctx.gridConfig().getTransactionConfiguration();

        return txStart0(concurrency,
            isolation,
            cfg.getDefaultTxTimeout(),
            0,
            ctx.systemTx() ? ctx : null);
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @param timeout Transaction timeout.
     * @param txSize Expected transaction size.
     * @param sysCacheCtx System cache context.
     * @return Transaction.
     */
    @SuppressWarnings("unchecked")
    private IgniteInternalTx txStart0(TransactionConcurrency concurrency, TransactionIsolation isolation,
        long timeout, int txSize, @Nullable GridCacheContext sysCacheCtx) {
        cctx.kernalContext().gateway().readLock();

        try {
            TransactionConfiguration cfg = cctx.gridConfig().getTransactionConfiguration();

            if (!cfg.isTxSerializableEnabled() && isolation == SERIALIZABLE)
                throw new IllegalArgumentException("SERIALIZABLE isolation level is disabled (to enable change " +
                    "'txSerializableEnabled' configuration property)");

            IgniteInternalTx tx = cctx.tm().userTx(sysCacheCtx);

            if (tx != null)
                throw new IllegalStateException("Failed to start new transaction " +
                    "(current thread already has a transaction): " + tx);

            tx = cctx.tm().newTx(
                false,
                false,
                sysCacheCtx,
                concurrency,
                isolation,
                timeout,
                true,
                txSize
            );

            assert tx != null;

            return tx;
        }
        finally {
            cctx.kernalContext().gateway().readUnlock();
        }
    }

    /** {@inheritDoc} */
    @Nullable @Override public Transaction tx() {
        IgniteInternalTx tx = cctx.tm().userTx();

        return tx != null ? tx.proxy() : null;
    }

    /** {@inheritDoc} */
    @Override public TransactionMetrics metrics() {
        return cctx.txMetrics();
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        cctx.resetTxMetrics();
    }

    /**
     * @param ctx Cache context.
     */
    private void checkTransactional(GridCacheContext ctx) {
        if (!ctx.transactional())
            throw new IgniteException("Failed to start transaction on non-transactional cache: " + ctx.name());
    }
}