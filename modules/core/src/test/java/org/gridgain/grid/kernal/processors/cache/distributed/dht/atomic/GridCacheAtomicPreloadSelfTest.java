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
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.transactions.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Simple test for preloading in ATOMIC cache.
 */
public class GridCacheAtomicPreloadSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean nearEnabled;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridCacheConfiguration cacheCfg = new GridCacheConfiguration();

        cacheCfg.setCacheMode(GridCacheMode.PARTITIONED);
        cacheCfg.setAtomicityMode(GridCacheAtomicityMode.TRANSACTIONAL);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        cacheCfg.setDistributionMode(nearEnabled ? NEAR_PARTITIONED : PARTITIONED_ONLY);
        cacheCfg.setBackups(1);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSimpleTxsNear() throws Exception {
        checkSimpleTxs(true, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSimpleTxsColocated() throws Exception {
        checkSimpleTxs(false, PESSIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSimpleTxsColocated() throws Exception {
        checkSimpleTxs(false, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    public void testOptimisticSimpleTxsNear() throws Exception {
        checkSimpleTxs(false, OPTIMISTIC);
    }

    /**
     * @throws Exception If failed.
     */
    private void checkSimpleTxs(boolean nearEnabled, GridCacheTxConcurrency concurrency) throws Exception {
        try {
            this.nearEnabled = nearEnabled;

            startGrids(3);

            awaitPartitionMapExchange();

            GridCache<Object, Object> cache = grid(0).cache(null);

            List<Integer> keys = generateKeys(grid(0).localNode(), cache);

            GridTransactions txs = grid(0).transactions();

            assert txs != null;

            for (int i = 0; i < keys.size(); i++) {
                Integer key = keys.get(i);

                info(">>>>>>>>>>>>>>>");
                info("Checking transaction for key [idx=" + i + ", key=" + key + ']');
                info(">>>>>>>>>>>>>>>");

                try (GridCacheTx tx = txs.txStart(concurrency, REPEATABLE_READ)) {
                    try {
                        // Lock if pessimistic, read if optimistic.
                        cache.get(key);

                        cache.put(key, key + 1);

                        tx.commit();
                    }
                    catch (Exception e) {
                        // Print exception in case if
                        e.printStackTrace();

                        throw e;
                    }
                }

//                Thread.sleep(500);

                info(">>>>>>>>>>>>>>>");
                info("Finished checking transaction for key [idx=" + i + ", key=" + key + ']');
                info(">>>>>>>>>>>>>>>");

                checkTransactions();
                checkValues(key, key + 1);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     *
     */
    private void checkTransactions() {
        for (int i = 0; i < 3; i++) {
            GridCacheTxManager<Object, Object> tm = ((GridKernal)grid(i)).context().cache().context().tm();

            assertEquals("Uncommitted transactions found on node [idx=" + i + ", mapSize=" + tm.idMapSize() + ']',
                0, tm.idMapSize());
        }
    }

    /**
     * @param key Key to check.
     * @param val Expected value.
     */
    private void checkValues(int key, int val) {
        for (int i = 0; i < 3; i++) {
            GridEx grid = grid(i);

            ClusterNode node = grid.localNode();

            GridCache<Object, Object> cache = grid.cache(null);

            boolean primary = cache.affinity().isPrimary(node, key);
            boolean backup = cache.affinity().isBackup(node, key);

            if (primary || backup)
                assertEquals("Invalid cache value [nodeId=" + node.id() + ", primary=" + primary +
                    ", backup=" + backup + ", key=" + key + ']', val, cache.peek(key));
        }
    }

    /**
     * Generates a set of keys: near, primary, backup.
     *
     * @param node Node for which keys are generated.
     * @param cache Cache to get affinity for.
     * @return Collection of keys.
     */
    private List<Integer> generateKeys(ClusterNode node, GridCache<Object, Object> cache) {
        List<Integer> keys = new ArrayList<>(3);

        GridCacheAffinity<Object> aff = cache.affinity();

        int base = 0;

//        // Near key.
        while (aff.isPrimary(node, base) || aff.isBackup(node, base))
            base++;

        keys.add(base);

//        Primary key.
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
