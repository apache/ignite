/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.transactions;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.transactions.*;
import org.jetbrains.annotations.*;

/**
 * Grid transactions implementation.
 */
public class GridTransactionsImpl implements GridTransactions {
    /** {@inheritDoc} */
    @Override public GridCacheTx txStart() throws IllegalStateException {
        // TODO: implement.
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStart(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation) {
        // TODO: implement.
        return null;
    }

    /** {@inheritDoc} */
    @Override public GridCacheTx txStart(GridCacheTxConcurrency concurrency, GridCacheTxIsolation isolation,
        long timeout, int txSize) {
        // TODO: implement.
        return null;
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
