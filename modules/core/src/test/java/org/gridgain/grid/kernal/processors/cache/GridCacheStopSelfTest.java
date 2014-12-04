/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 * Tests correct cache stopping.
 */
public class GridCacheStopSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String EXPECTED_MSG = "Grid is in invalid state to perform this operation. " +
        "It either not started yet or has already being or have stopped";

    /** */
    private boolean atomic;

    /** */
    private boolean replicated;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disc = new GridTcpDiscoverySpi();

        disc.setIpFinder(new GridTcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(disc);

        GridCacheConfiguration ccfg  = new GridCacheConfiguration();

        ccfg.setCacheMode(replicated ? REPLICATED : PARTITIONED);

        ccfg.setAtomicityMode(atomic ? ATOMIC : TRANSACTIONAL);

        ccfg.setQueryIndexEnabled(true);
        ccfg.setSwapEnabled(true);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopExplicitTransactions() throws Exception {
        testStop(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopImplicitTransactions() throws Exception {
        testStop(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopExplicitTransactionsReplicated() throws Exception {
        replicated = true;

        testStop(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopImplicitTransactionsReplicated() throws Exception {
        replicated = true;

        testStop(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testStopAtomic() throws Exception {
        atomic = true;

        testStop(false);
    }

    /**
     * @param startTx If {@code true} starts transactions.
     * @throws Exception If failed.
     */
    private void testStop(final boolean startTx) throws Exception {
        for (int i = 0; i < 10; i++) {
            startGrid(0);

            final int PUT_THREADS = 50;

            final CountDownLatch stopLatch = new CountDownLatch(1);

            final CountDownLatch readyLatch = new CountDownLatch(PUT_THREADS);

            final GridCache<Integer, Integer> cache = grid(0).cache(null);

            assertNotNull(cache);

            assertEquals(atomic ? ATOMIC : TRANSACTIONAL, cache.configuration().getAtomicityMode());
            assertEquals(replicated ? REPLICATED : PARTITIONED, cache.configuration().getCacheMode());

            Collection<GridFuture<?>> putFuts = new ArrayList<>();

            for (int j = 0; j < PUT_THREADS; j++) {
                final int key = j;

                putFuts.add(GridTestUtils.runAsync(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        if (startTx) {
                            try (GridCacheTx tx = cache.txStart()) {
                                cache.put(key, key);

                                readyLatch.countDown();

                                stopLatch.await();

                                tx.commit();
                            }
                        }
                        else {
                            readyLatch.countDown();

                            stopLatch.await();

                            cache.put(key, key);
                        }

                        return null;
                    }
                }));
            }

            readyLatch.await();

            stopLatch.countDown();

            stopGrid(0);

            for (GridFuture<?> fut : putFuts) {
                try {
                    fut.get();
                }
                catch (GridException e) {
                    if (!e.getMessage().startsWith(EXPECTED_MSG))
                        e.printStackTrace();

                    assertTrue("Unexpected error message: " + e.getMessage(), e.getMessage().startsWith(EXPECTED_MSG));
                }
            }

            try {
                cache.put(1, 1);
            }
            catch (IllegalStateException e) {
                if (!e.getMessage().startsWith(EXPECTED_MSG))
                    e.printStackTrace();

                assertTrue("Unexpected error message: " + e.getMessage(), e.getMessage().startsWith(EXPECTED_MSG));
            }
        }
    }
}
