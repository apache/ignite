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

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.cache.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.internal.processors.cache.CacheFlag.*;
import static org.apache.ignite.transactions.IgniteTxIsolation.*;

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
    @Override public IgniteTx txStart() throws IllegalStateException {
        TransactionsConfiguration cfg = cctx.gridConfig().getTransactionsConfiguration();

        return txStart0(
            cfg.getDefaultTxConcurrency(),
            cfg.getDefaultTxIsolation(),
            cfg.getDefaultTxTimeout(),
            0,
            false
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteTx txStart(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation) {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");

        TransactionsConfiguration cfg = cctx.gridConfig().getTransactionsConfiguration();

        return txStart0(
            concurrency,
            isolation,
            cfg.getDefaultTxTimeout(),
            0,
            false
        );
    }

    /** {@inheritDoc} */
    @Override public IgniteTx txStart(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation,
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
            false
        );
    }

    @Override public IgniteTx txStartSystem(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation,
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
            true
        );
    }

    /**
     * @param concurrency Transaction concurrency.
     * @param isolation Transaction isolation.
     * @param timeout Transaction timeout.
     * @param txSize Expected transaction size.
     * @param sys System flag.
     * @return Transaction.
     */
    private IgniteTx txStart0(IgniteTxConcurrency concurrency, IgniteTxIsolation isolation,
        long timeout, int txSize, boolean sys) {
        TransactionsConfiguration cfg = cctx.gridConfig().getTransactionsConfiguration();

        if (!cfg.isTxSerializableEnabled() && isolation == SERIALIZABLE)
            throw new IllegalArgumentException("SERIALIZABLE isolation level is disabled (to enable change " +
                "'txSerializableEnabled' configuration property)");

        IgniteTxEx<K, V> tx = (IgniteTxEx<K, V>)cctx.tm().userTx();

        if (tx != null)
            throw new IllegalStateException("Failed to start new transaction " +
                "(current thread already has a transaction): " + tx);

        tx = cctx.tm().newTx(
            false,
            false,
            sys,
            concurrency,
            isolation,
            timeout,
            false,
            true,
            txSize,
            /** group lock keys */null,
            /** partition lock */false
        );

        assert tx != null;

        // Wrap into proxy.
        return new IgniteTxProxyImpl<>(tx, cctx, false);

    }

    /** {@inheritDoc} */
    @Override public IgniteTx txStartAffinity(String cacheName, Object affinityKey, IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, IgniteCheckedException {
        GridCacheAdapter<Object, Object> cache = cctx.kernalContext().cache().internalCache(cacheName);

        if (cache == null)
            throw new IllegalArgumentException("Failed to find cache with given name (cache is not configured): " +
                cacheName);

        return txStartGroupLock(cache.context(), affinityKey, concurrency, isolation, false, timeout, txSize, false);
    }

    /** {@inheritDoc} */
    @Override public IgniteTx txStartPartition(String cacheName, int partId, IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, IgniteCheckedException {
        GridCacheAdapter<Object, Object> cache = cctx.kernalContext().cache().internalCache(cacheName);

        if (cache == null)
            throw new IllegalArgumentException("Failed to find cache with given name (cache is not configured): " +
                cacheName);

        Object grpLockKey = cache.context().affinity().partitionAffinityKey(partId);

        return txStartGroupLock(cache.context(), grpLockKey, concurrency, isolation, true, timeout, txSize, false);
    }

    /**
     * Internal method to start group-lock transaction.
     *
     * @param grpLockKey Group lock key.
     * @param concurrency Transaction concurrency control.
     * @param isolation Transaction isolation level.
     * @param partLock {@code True} if this is a partition-lock transaction. In this case {@code grpLockKey}
     *      should be a unique partition-specific key.
     * @param timeout Tx timeout.
     * @param txSize Expected transaction size.
     * @param sys System flag.
     * @return Started transaction.
     * @throws IllegalStateException If other transaction was already started.
     * @throws IgniteCheckedException In case of error.
     */
    @SuppressWarnings("unchecked")
    private IgniteTx txStartGroupLock(GridCacheContext ctx, Object grpLockKey, IgniteTxConcurrency concurrency,
        IgniteTxIsolation isolation, boolean partLock, long timeout, int txSize, boolean sys)
        throws IllegalStateException, IgniteCheckedException {
        IgniteTx tx = cctx.tm().userTx();

        if (tx != null)
            throw new IllegalStateException("Failed to start new transaction " +
                "(current thread already has a transaction): " + tx);

        IgniteTxLocalAdapter<K, V> tx0 = cctx.tm().newTx(
            false,
            false,
            sys,
            concurrency,
            isolation,
            timeout,
            ctx.hasFlag(INVALIDATE),
            !ctx.hasFlag(SKIP_STORE),
            txSize,
            ctx.txKey(grpLockKey),
            partLock
        );

        assert tx0 != null;

        if (ctx.hasFlag(SYNC_COMMIT))
            tx0.syncCommit(true);

        IgniteFuture<?> lockFut = tx0.groupLockAsync(ctx, (Collection)F.asList(grpLockKey));

        try {
            lockFut.get();
        }
        catch (IgniteCheckedException e) {
            tx0.rollback();

            throw e;
        }

        // Wrap into proxy.
        return new IgniteTxProxyImpl<>(tx0, cctx, false);
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteTx tx() {
        return cctx.tm().userTx();
    }

    /** {@inheritDoc} */
    @Override public IgniteTxMetrics metrics() {
        return cctx.txMetrics();
    }

    /** {@inheritDoc} */
    @Override public void resetMetrics() {
        cctx.resetTxMetrics();
    }
}
