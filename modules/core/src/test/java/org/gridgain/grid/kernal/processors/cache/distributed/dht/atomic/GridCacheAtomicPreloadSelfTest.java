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
        cacheCfg.setAtomicityMode(GridCacheAtomicityMode.ATOMIC);
        cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPreloading() throws Exception {
        try {
            startGrids(2);

            GridEx grid = grid(0);

            GridCache<Object, Object> cache = grid.cache(null);

            int keyCnt = 100;

            for (int i = 0; i < keyCnt; i++)
                cache.put(i, i);

            startGrid(2);

            awaitPartitionMapExchange();

            for (int i = 0; i < keyCnt; i++) {
                for (int g = 0; g < 3; g++) {
                    GridEx locGrid = grid(g);

                    GridNode node = locGrid.localNode();

                    Collection<GridNode> affNodes = cache.affinity().mapKeyToPrimaryAndBackups(i);

                    if (affNodes.contains(node))
                        assertEquals(i, locGrid.cache(null).peek(i));
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }
}
