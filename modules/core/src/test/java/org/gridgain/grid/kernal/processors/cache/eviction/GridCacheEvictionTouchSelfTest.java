/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.eviction;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.*;
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.cache.eviction.fifo.*;
import org.gridgain.grid.kernal.processors.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridCacheEvictionTouchSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private GridCacheEvictionPolicy<?, ?> plc;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        GridTransactionsConfiguration txCfg = c.getTransactionsConfiguration();

        txCfg.setDefaultTxConcurrency(PESSIMISTIC);
        txCfg.setDefaultTxIsolation(REPEATABLE_READ);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(REPLICATED);

        cc.setSwapEnabled(false);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setEvictionPolicy(plc);

        cc.setStore(new GridCacheGenericTestStore<Object, Object>() {
            @Override public Object load(GridCacheTx tx, Object key) {
                return key;
            }

            @Override public void loadAll(GridCacheTx tx, Collection<?> keys,
                IgniteBiInClosure<Object, Object> c) {
                for (Object key : keys)
                    c.apply(key, key);
            }
        });

        c.setCacheConfiguration(cc);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        plc = null;

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistency() throws Exception {
        plc = new GridCacheFifoEvictionPolicy<Object, Object>(500);

        try {
            Ignite ignite = startGrid(1);

            final GridCache<Integer, Integer> cache = ignite.cache(null);

            final Random rnd = new Random();

            try (GridCacheTx tx = cache.txStart()) {
                int iterCnt = 20;
                int keyCnt = 5000;

                for (int i = 0; i < iterCnt; i++) {
                    int j = rnd.nextInt(keyCnt);

                    // Put or remove?
                    if (rnd.nextBoolean())
                        cache.putx(j, j);
                    else
                        cache.remove(j);

                    if (i != 0 && i % 1000 == 0)
                        info("Stats [iterCnt=" + i + ", size=" + cache.size() + ']');
                }

                GridCacheFifoEvictionPolicy<Integer, Integer> plc0 = (GridCacheFifoEvictionPolicy<Integer, Integer>) plc;

                if (!plc0.queue().isEmpty()) {
                    for (GridCacheEntry<Integer, Integer> e : plc0.queue())
                        U.warn(log, "Policy queue item: " + e);

                    fail("Test failed, see logs for details.");
                }

                tx.commit();
            }
        }
        catch (Throwable t) {
            error("Test failed.", t);

            fail("Test failed, see logs for details.");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictSingle() throws Exception {
        plc = new GridCacheFifoEvictionPolicy<Object, Object>(500);

        try {
            Ignite ignite = startGrid(1);

            final GridCache<Integer, Integer> cache = ignite.cache(null);

            for (int i = 0; i < 100; i++)
                cache.put(i, i);

            assertEquals(100, ((GridCacheFifoEvictionPolicy)plc).queue().size());

            for (int i = 0; i < 100; i++)
                cache.evict(i);

            assertEquals(0, ((GridCacheFifoEvictionPolicy)plc).queue().size());
            assertEquals(0, cache.size());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testEvictAll() throws Exception {
        plc = new GridCacheFifoEvictionPolicy<Object, Object>(500);

        try {
            Ignite ignite = startGrid(1);

            final GridCache<Integer, Integer> cache = ignite.cache(null);

            Collection<Integer> keys = new ArrayList<>(100);

            for (int i = 0; i < 100; i++) {
                cache.put(i, i);

                keys.add(i);
            }

            assertEquals(100, ((GridCacheFifoEvictionPolicy)plc).queue().size());

            cache.evictAll(keys);

            assertEquals(0, ((GridCacheFifoEvictionPolicy)plc).queue().size());
            assertEquals(0, cache.size());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testGroupLock() throws Exception { // TODO GG-9141
        plc = new GridCacheFifoEvictionPolicy<>(100);

        try {
            Ignite g = startGrid(1);

            Integer affKey = 1;

            GridCache<GridCacheAffinityKey<Object>, Integer> cache = g.cache(null);

            GridCacheTx tx = cache.txStartAffinity(affKey, PESSIMISTIC, REPEATABLE_READ, 0, 5);

            try {
                for (int i = 0; i < 5; i++)
                    cache.put(new GridCacheAffinityKey<Object>(i, affKey), i);

                tx.commit();
            }
            finally {
                tx.close();
            }

            assertEquals(5, ((GridCacheFifoEvictionPolicy)plc).queue().size());

            tx = cache.txStartAffinity(affKey, PESSIMISTIC, REPEATABLE_READ, 0, 5);

            try {
                for (int i = 0; i < 5; i++)
                    cache.remove(new GridCacheAffinityKey<Object>(i, affKey));

                tx.commit();
            }
            finally {
                tx.close();
            }

            assertEquals(0, ((GridCacheFifoEvictionPolicy)plc).queue().size());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void _testPartitionGroupLock() throws Exception { // TODO GG-9141
        plc = new GridCacheFifoEvictionPolicy<>(100);

        try {
            Ignite g = startGrid(1);

            Integer affKey = 1;

            GridCache<Object, Integer> cache = g.cache(null);

            GridCacheTx tx = cache.txStartPartition(cache.affinity().partition(affKey), PESSIMISTIC, REPEATABLE_READ,
                0, 5);

            try {
                for (int i = 0; i < 5; i++)
                    cache.put(new GridCacheAffinityKey<Object>(i, affKey), i);

                tx.commit();
            }
            finally {
                tx.close();
            }

            assertEquals(5, ((GridCacheFifoEvictionPolicy)plc).queue().size());

            tx = cache.txStartPartition(cache.affinity().partition(affKey), PESSIMISTIC, REPEATABLE_READ, 0, 5);

            try {
                for (int i = 0; i < 5; i++)
                    cache.remove(new GridCacheAffinityKey<Object>(i, affKey));

                tx.commit();
            }
            finally {
                tx.close();
            }

            assertEquals(0, ((GridCacheFifoEvictionPolicy)plc).queue().size());
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testReload() throws Exception {
        plc = new GridCacheFifoEvictionPolicy<Object, Object>(100);

        try {
            Ignite ignite = startGrid(1);

            final GridCache<Integer, Integer> cache = ignite.cache(null);

            for (int i = 0; i < 10000; i++)
                cache.reload(i);

            assertEquals(100, cache.size());
            assertEquals(100, cache.size());
            assertEquals(100, ((GridCacheFifoEvictionPolicy)plc).queue().size());

            Collection<Integer> keys = new ArrayList<>(10000);

            for (int i = 0; i < 10000; i++)
                keys.add(i);

            cache.reloadAll(keys);

            assertEquals(100, cache.size());
            assertEquals(100, cache.size());
            assertEquals(100, ((GridCacheFifoEvictionPolicy)plc).queue().size());
        }
        finally {
            stopAllGrids();
        }
    }
}
