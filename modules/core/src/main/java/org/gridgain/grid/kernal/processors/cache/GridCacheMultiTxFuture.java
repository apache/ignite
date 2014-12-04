/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.future.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Future which waits for completion of one or more transactions.
 */
public final class GridCacheMultiTxFuture<K, V> extends GridFutureAdapter<Boolean> {
    /** */
    private static final long serialVersionUID = 0L;

    /** Logger reference. */
    private static final AtomicReference<IgniteLogger> logRef = new AtomicReference<>();

    /** Transactions to wait for. */
    private final Set<GridCacheTxEx<K, V>> txs = new GridLeanSet<>();

    /** */
    private Set<GridCacheTxEx<K, V>> remainingTxs;

    /** Logger. */
    private IgniteLogger log;

    /**
     * @param cctx Cache context.
     */
    public GridCacheMultiTxFuture(GridCacheContext<K, V> cctx) {
        super(cctx.kernalContext());

        log = U.logger(ctx,  logRef, GridCacheMultiTxFuture.class);

        // Notify listeners in different threads.
        concurrentNotify(true);
    }

    /**
     * Empty constructor required for {@link Externalizable}.
     */
    public GridCacheMultiTxFuture() {
        // No-op.
    }

    /**
     * @return Transactions to wait for.
     */
    public Set<GridCacheTxEx<K, V>> txs() {
        return txs;
    }

    /**
     * @return Remaining transactions.
     */
    public Set<GridCacheTxEx<K, V>> remainingTxs() {
        return remainingTxs;
    }

    /**
     * @param tx Transaction to add.
     */
    public void addTx(GridCacheTxEx<K, V> tx) {
        txs.add(tx);
    }

    /**
     * Initializes this future.
     */
    public void init() {
        if (F.isEmpty(txs)) {
            remainingTxs = Collections.emptySet();

            onDone(true);
        }
        else {
            remainingTxs = new GridConcurrentHashSet<>(txs);

            for (final GridCacheTxEx<K, V> tx : txs) {
                if (!tx.done()) {
                    tx.finishFuture().listenAsync(new CI1<IgniteFuture<GridCacheTx>>() {
                        @Override public void apply(IgniteFuture<GridCacheTx> t) {
                            remainingTxs.remove(tx);

                            checkRemaining();
                        }
                    });
                }
                else
                    remainingTxs.remove(tx);
            }

            checkRemaining();
        }
    }

    /**
     * @return {@code True} if remaining set is empty.
     */
    private boolean checkRemaining() {
        if (remainingTxs.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("Finishing multi-tx future: " + this);

            onDone(true);

            return true;
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridCacheMultiTxFuture.class, this,
            "txs", F.viewReadOnly(txs, CU.<K, V>tx2xidVersion()),
            "remaining", F.viewReadOnly(remainingTxs, CU.<K, V>tx2xidVersion()),
            "super", super.toString()
        );
    }
}
