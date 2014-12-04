/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.dht;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Tests write-through.
 */
public abstract class GridCacheAbstractTransformWriteThroughSelfTest extends GridCommonAbstractTest {
    /** Grid count. */
    protected static final int GRID_CNT = 3;

    /** Update operation. */
    protected static final int OP_UPDATE = 0;

    /** Delete operation. */
    protected static final int OP_DELETE = 1;

    /** Near node constant. */
    protected static final int NEAR_NODE = 0;

    /** Primary node constant. */
    protected static final int PRIMARY_NODE = 0;

    /** Backup node constant. */
    protected static final int BACKUP_NODE = 0;

    /** Keys number. */
    public static final int KEYS_CNT = 30;

    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Value increment closure. */
    private static final IgniteClosure<Integer, Integer> INCR_CLOS = new IgniteClosure<Integer, Integer>() {
        @Override public Integer apply(Integer src) {
            return src == null ? 1 : src + 1;
        }
    };

    /** Value remove closure. */
    private static final IgniteClosure<Integer, Integer> RMV_CLOS = new IgniteClosure<Integer, Integer>() {
        @Override public Integer apply(Integer src) {
            return null;
        }
    };

    /** Test store. */
    private static List<GridCacheGenericTestStore<String, Integer>> stores =
        new ArrayList<>(GRID_CNT);

    /**
     * @return {@code True} if batch update is enabled.
     */
    protected abstract boolean batchUpdate();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        GridCacheGenericTestStore<String, Integer> store = new GridCacheGenericTestStore<>();

        stores.add(store);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(PARTITIONED);
        cacheCfg.setBackups(1);
        cacheCfg.setStore(store);
        cacheCfg.setAtomicityMode(TRANSACTIONAL);
        cacheCfg.setDistributionMode(NEAR_PARTITIONED);

        cfg.setCacheConfiguration(cacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        for (int i = 0; i < GRID_CNT; i++)
            startGrid(i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids(true);

        stores.clear();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (GridCacheGenericTestStore<String, Integer> store : stores)
            store.reset();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticNearUpdate() throws Exception {
        checkTransform(OPTIMISTIC, NEAR_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticPrimaryUpdate() throws Exception {
        checkTransform(OPTIMISTIC, PRIMARY_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticBackupUpdate() throws Exception {
        checkTransform(OPTIMISTIC, BACKUP_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticNearDelete() throws Exception {
        checkTransform(OPTIMISTIC, NEAR_NODE, OP_DELETE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticPrimaryDelete() throws Exception {
        checkTransform(OPTIMISTIC, PRIMARY_NODE, OP_DELETE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformOptimisticBackupDelete() throws Exception {
        checkTransform(OPTIMISTIC, BACKUP_NODE, OP_DELETE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticNearUpdate() throws Exception {
        checkTransform(PESSIMISTIC, NEAR_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticPrimaryUpdate() throws Exception {
        checkTransform(PESSIMISTIC, PRIMARY_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticBackupUpdate() throws Exception {
        checkTransform(PESSIMISTIC, BACKUP_NODE, OP_UPDATE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticNearDelete() throws Exception {
        checkTransform(PESSIMISTIC, NEAR_NODE, OP_DELETE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticPrimaryDelete() throws Exception {
        checkTransform(PESSIMISTIC, PRIMARY_NODE, OP_DELETE);
    }

    /**
     * @throws Exception If failed.
     */
    public void testTransformPessimisticBackupDelete() throws Exception {
        checkTransform(PESSIMISTIC, BACKUP_NODE, OP_DELETE);
    }

    /**
     * @param concurrency Concurrency.
     * @param nodeType Node type.
     * @param op Op.
     * @throws Exception If failed.
     */
    protected void checkTransform(GridCacheTxConcurrency concurrency, int nodeType, int op) throws Exception {
        GridCacheProjection<String, Integer> cache = cache(0);

        Collection<String> keys = keysForType(nodeType);

        for (String key : keys)
            cache.put(key, 1);

        GridCacheGenericTestStore<String, Integer> nearStore = stores.get(0);

        nearStore.reset();

        for (String key : keys)
            cache.clear(key);

        info(">>> Starting transform transaction");

        try (GridCacheTx tx = cache.txStart(concurrency, READ_COMMITTED)) {
            if (op == OP_UPDATE) {
                for (String key : keys)
                    cache.transform(key, INCR_CLOS);
            }
            else {
                for (String key : keys)
                    cache.transform(key, RMV_CLOS);
            }

            tx.commit();
        }

        if (batchUpdate()) {
            assertEquals(0, nearStore.getPutCount());
            assertEquals(0, nearStore.getRemoveCount());

            if (op == OP_UPDATE)
                assertEquals(1, nearStore.getPutAllCount());
            else
                assertEquals(1, nearStore.getRemoveAllCount());
        }
        else {
            assertEquals(0, nearStore.getPutAllCount());
            assertEquals(0, nearStore.getRemoveAllCount());

            if (op == OP_UPDATE)
                assertEquals(keys.size(), nearStore.getPutCount());
            else
                assertEquals(keys.size(), nearStore.getRemoveCount());
        }

        if (op == OP_UPDATE) {
            for (String key : keys)
                assertEquals((Integer)2, nearStore.getMap().get(key));
        }
        else {
            for (String key : keys)
                assertNull(nearStore.getMap().get(key));
        }
    }

    /**
     * @param nodeType Node type to generate keys for.
     * @return Collection of keys.
     */
    private Collection<String> keysForType(int nodeType) {
        Collection<String> keys = new ArrayList<>(KEYS_CNT);

        int numKey = 0;

        while (keys.size() < 30) {
            String key = String.valueOf(numKey);

            if (nodeType == NEAR_NODE) {
                if (!cache(0).affinity().isPrimaryOrBackup(grid(0).localNode(), key))
                    keys.add(key);
            }
            else if (nodeType == PRIMARY_NODE) {
                if (cache(0).affinity().isPrimary(grid(0).localNode(), key))
                    keys.add(key);
            }
            else if (nodeType == BACKUP_NODE) {
                if (cache(0).affinity().isBackup(grid(0).localNode(), key))
                    keys.add(key);
            }

            numKey++;
        }

        return keys;
    }
}
