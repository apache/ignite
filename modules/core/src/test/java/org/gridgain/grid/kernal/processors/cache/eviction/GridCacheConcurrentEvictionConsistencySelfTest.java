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
import org.gridgain.grid.cache.eviction.*;
import org.gridgain.grid.cache.eviction.fifo.*;
import org.gridgain.grid.cache.eviction.lru.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridCacheConcurrentEvictionConsistencySelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Default iteration count. */
    private static final int ITERATION_CNT = 50000;

    /** Size of policy internal queue. */
    private static final int POLICY_QUEUE_SIZE = 50;

    /** Tested policy. */
    private GridCacheEvictionPolicy<?, ?> plc;

    /** Key count to put into the cache. */
    private int keyCnt;

    /** Number of threads. */
    private int threadCnt = 50;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.getTransactionsConfiguration().setDefaultTxConcurrency(PESSIMISTIC);
        c.getTransactionsConfiguration().setDefaultTxIsolation(READ_COMMITTED);

        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(LOCAL);

        cc.setSwapEnabled(false);

        cc.setWriteSynchronizationMode(FULL_SYNC);

        cc.setDistributionMode(PARTITIONED_ONLY);

        cc.setEvictionPolicy(plc);

        c.setCacheConfiguration(cc);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        c.setDiscoverySpi(disco);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 5 * 60 * 1000; // 5 min.
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyFifoLocalTwoKeys() throws Exception {
        plc = new GridCacheFifoEvictionPolicy<Object, Object>(1);

        keyCnt = 2;
        threadCnt = 10;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyLruLocalTwoKeys() throws Exception {
        plc = new GridCacheLruEvictionPolicy<Object, Object>(1);

        keyCnt = 2;
        threadCnt = 10;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyFifoLocalFewKeys() throws Exception {
        plc = new GridCacheFifoEvictionPolicy<Object, Object>(POLICY_QUEUE_SIZE);

        keyCnt = POLICY_QUEUE_SIZE + 5;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyLruLocalFewKeys() throws Exception {
        plc = new GridCacheLruEvictionPolicy<Object, Object>(POLICY_QUEUE_SIZE);

        keyCnt = POLICY_QUEUE_SIZE + 5;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyFifoLocal() throws Exception {
        plc = new GridCacheFifoEvictionPolicy<Object, Object>(POLICY_QUEUE_SIZE);

        keyCnt = POLICY_QUEUE_SIZE * 10;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    public void testPolicyConsistencyLruLocal() throws Exception {
        plc = new GridCacheLruEvictionPolicy<Object, Object>(POLICY_QUEUE_SIZE);

        keyCnt = POLICY_QUEUE_SIZE * 10;

        checkPolicyConsistency();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkPolicyConsistency() throws Exception {
        try {
            Ignite ignite = startGrid(1);

            final GridCache<Integer, Integer> cache = ignite.cache(null);

            long start = System.currentTimeMillis();

            IgniteFuture<?> fut = multithreadedAsync(
                new Callable<Object>() {
                    @Override
                    public Object call() throws Exception {
                        final Random rnd = new Random();

                        for (int i = 0; i < ITERATION_CNT; i++) {

                            int j = rnd.nextInt(keyCnt);

                            try (GridCacheTx tx = cache.txStart()) {
                                // Put or remove?
                                if (rnd.nextBoolean())
                                    cache.putx(j, j);
                                else
                                    cache.remove(j);

                                tx.commit();
                            }

                            if (i != 0 && i % 10000 == 0)
                                info("Stats [iterCnt=" + i + ", size=" + cache.size() + ']');
                        }

                        return null;
                    }
                },
                threadCnt
            );

            fut.get();

            Collection<GridCacheEntry<Integer, Integer>> queue = internalQueue(plc);

            info("Test results [threadCnt=" + threadCnt + ", iterCnt=" + ITERATION_CNT + ", cacheSize=" + cache.size() +
                ", internalQueueSize" + queue.size() + ", duration=" + (System.currentTimeMillis() - start) + ']');

            for (GridCacheEntry<Integer, Integer> e : queue) {
                Integer rmv = cache.remove(e.getKey());

                if (rmv == null)
                    fail("Eviction policy contains key that is not present in cache: " + e);
                else
                    info("Entry removed: " + rmv);
            }

            if (!cache.isEmpty()) {
                boolean zombies = false;

                for (GridCacheEntry<Integer, Integer> e : cache) {
                    U.warn(log, "Zombie entry: " + e);

                    zombies = true;
                }

                if (zombies)
                    fail("Cache contained zombie entries.");
            }
            else
                info("Cache is empty after test.");
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Gets internal policy queue.
     *
     * @param plc Policy to get queue from.
     * @return Internal entries collection.
     */
    private Collection<GridCacheEntry<Integer, Integer>> internalQueue(GridCacheEvictionPolicy<?, ?> plc) {
        if (plc instanceof GridCacheFifoEvictionPolicy) {
            GridCacheFifoEvictionPolicy<Integer, Integer> plc0 = (GridCacheFifoEvictionPolicy<Integer, Integer>)plc;

            return plc0.queue();
        }
        else if (plc instanceof GridCacheLruEvictionPolicy) {
            GridCacheLruEvictionPolicy<Integer, Integer> plc0 = (GridCacheLruEvictionPolicy<Integer, Integer>)plc;

            return plc0.queue();
        }

        assert false : "Unexpected policy type: " + plc.getClass().getName();

        return Collections.emptyList();
    }
}
