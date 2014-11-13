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
import org.gridgain.grid.cache.affinity.*;
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
        cacheCfg.setDistributionMode(GridCacheDistributionMode.PARTITIONED_ONLY);
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

            awaitPartitionMapExchange();

            GridCache<Object, Object> cache = grid(0).cache(null);

            List<Integer> keys = generateKeys(grid(0).localNode(), cache);

            GridTransactions txs = grid(0).transactions();

            assert txs != null;

            for (int i = 0; i < keys.size(); i++) {
                Integer key = keys.get(i);

                info("Checking transaction for key [idx=" + i + ", key=" + key + ']');

                try (GridCacheTx tx = txs.txStart(GridCacheTxConcurrency.PESSIMISTIC,
                    GridCacheTxIsolation.REPEATABLE_READ)) {

                    // Lock.
                    cache.get(key);

                    cache.put(key, key + 1);

                    tx.commit();
                }
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Generates a set of keys: near, primary, backup.
     *
     * @param node Node for which keys are generated.
     * @param cache Cache to get affinity for.
     * @return Collection of keys.
     */
    private List<Integer> generateKeys(GridNode node, GridCache<Object, Object> cache) {
        List<Integer> keys = new ArrayList<>(3);

        GridCacheAffinity<Object> aff = cache.affinity();

        int base = 0;

        // Near key.
        while (aff.isPrimary(node, base) || aff.isBackup(node, base))
            base++;

        keys.add(base);

        // Primary key.
        while (!aff.isPrimary(node, base))
            base++;

        keys.add(base);

        // Backup key.
        while (!aff.isBackup(node, base))
            base++;

        keys.add(base);

        return keys;
    }
}
