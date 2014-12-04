/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.transactions;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Grid transactions implementation.
 */
public class IgniteTransactionsImpl<K, V> implements IgniteTransactions {
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
        GridTransactionsConfiguration cfg = cctx.gridConfig().getTransactionsConfiguration();

        return txStart0(
            cfg.getDefaultTxConcurrency(),
            cfg.getDefaultTxIsolation(),
            cfg.getDefaultTxTimeout(),
            0);
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStart(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) {
        A.notNull(concurrency, "concurrency");
        A.notNull(isolation, "isolation");

        GridTransactionsConfiguration cfg = cctx.gridConfig().getTransactionsConfiguration();

        return txStart0(
            concurrency,
            isolation,
            cfg.getDefaultTxTimeout(),
            0
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
            txSize
        );
    }

    private GridCacheTx txStart0(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation,
        long timeout, int txSize) {
        GridTransactionsConfiguration cfg = cctx.gridConfig().getTransactionsConfiguration();

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
            concurrency,
            isolation,
            timeout,
            txSize,
            /** group lock keys */null,
            /** partition lock */false
        );

        assert tx != null;

        // Wrap into proxy.
        return new GridCacheTxProxyImpl<>(tx, cctx);

    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStartAffinity(Object affinityKey, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, GridException {
        // TODO: implement.
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStartPartition(int partId, GridCacheTxConcurrency concurrency,
        GridCacheTxIsolation isolation, long timeout, int txSize) throws IllegalStateException, GridException {
        // TODO: implement.
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridCacheTx tx() {
        // TODO: implement.
        return null;
    }
}
