/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.transactions;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.transactions.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.apache.ignite.transactions.GridCacheTxIsolation.*;

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
    @Override public GridCacheTx txStart() throws IllegalStateException {
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
    @Override public GridCacheTx txStart(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) {
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
    @Override public GridCacheTx txStart(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation,
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

    @Override public GridCacheTx txStartSystem(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation,
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
    private GridCacheTx txStart0(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation,
        long timeout, int txSize, boolean sys) {
        TransactionsConfiguration cfg = cctx.gridConfig().getTransactionsConfiguration();

        if (!cfg.isTxSerializableEnabled() && isolation == SERIALIZABLE)
            throw new IllegalArgumentException("SERIALIZABLE isolation level is disabled (to enable change " +
                "'txSerializableEnabled' configuration property)");

        GridCacheTxEx<K, V> tx = (GridCacheTxEx<K, V>)cctx.tm().userTx();

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
        return new GridCacheTxProxyImpl<>(tx, cctx);

    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStartAffinity(String cacheName, Object affinityKey, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, IgniteCheckedException {
        GridCacheAdapter<Object, Object> cache = cctx.kernalContext().cache().internalCache(cacheName);

        if (cache == null)
            throw new IllegalArgumentException("Failed to find cache with given name (cache is not configured): " +
                cacheName);

        return txStartGroupLock(cache.context(), affinityKey, concurrency, isolation, false, timeout, txSize, false);
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStartPartition(String cacheName, int partId, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, IgniteCheckedException {
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
    private GridCacheTx txStartGroupLock(GridCacheContext ctx, Object grpLockKey, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, boolean partLock, long timeout, int txSize, boolean sys)
        throws IllegalStateException, IgniteCheckedException {
        GridCacheTx tx = cctx.tm().userTx();

        if (tx != null)
            throw new IllegalStateException("Failed to start new transaction " +
                "(current thread already has a transaction): " + tx);

        GridCacheTxLocalAdapter<K, V> tx0 = cctx.tm().newTx(
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
        return new GridCacheTxProxyImpl<>(tx0, cctx);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheTx tx() {
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
