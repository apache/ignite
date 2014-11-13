/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht.atomic;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.transactions.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Simple test for preloading in ATOMIC cache.
 */
public class GridCacheAtomicPreloadSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cacheCfg = new GridCacheConfiguration();

        cacheCfg.setCacheMode(GridCacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(GridCacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreloading() throws Exception {
        try {
            startGrids(3);

            GridTransactions txs = grid(0).transactions();

            assert txs != null;

            try (GridCacheTx tx = txs.txStart(GridCacheTxConcurrency.PESSIMISTIC, GridCacheTxIsolation.REPEATABLE_READ)) {
                GridCache<Object, Object> cache = grid(0).cache(null);

                // Lock.
                cache.get(0);

                cache.put(0, 1);

                tx.commit();
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
