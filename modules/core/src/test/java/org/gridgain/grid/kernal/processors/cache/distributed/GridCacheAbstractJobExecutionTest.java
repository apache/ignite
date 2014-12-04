/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheFlag.*;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.*;
import static org.gridgain.grid.cache.GridCacheTxIsolation.*;

/**
 * Tests cache access from within jobs.
 */
public abstract class GridCacheAbstractJobExecutionTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** Job counter. */
    private static final AtomicInteger cntr = new AtomicInteger(0);

    /** */
    private static final int GRID_CNT = 4;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(disco);

        cfg.setMarshaller(new IgniteOptimizedMarshaller(false));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridCacheProjection<String, int[]> cache = grid(0).cache(null).flagsOn(SYNC_COMMIT).
            projection(String.class, int[].class);

        cache.removeAll();

        for (int i = 0; i < GRID_CNT; i++) {
            Ignite g = grid(i);

            GridCache<String, int[]> c = g.cache(null);

            assertEquals("Cache is not empty: " + c.entrySet(), 0, c.size());
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticRepeatableRead() throws Exception {
        checkTransactions(PESSIMISTIC, REPEATABLE_READ, 1000);
    }

    /**
     * @throws Exception If failed.
     */
    public void testPessimisticSerializable() throws Exception {
        checkTransactions(PESSIMISTIC, SERIALIZABLE, 1000);
    }

    /**
     * @param concur Concurrency.
     * @param isolation Isolation.
     * @param jobCnt Job count.
     * @throws Exception If fails.
     */
    private void checkTransactions(final GridCacheTxConcurrency concur, final GridCacheTxIsolation isolation,
        final int jobCnt) throws Exception {

        info("Grid 0: " + grid(0).localNode().id());
        info("Grid 1: " + grid(1).localNode().id());
        info("Grid 2: " + grid(2).localNode().id());
        info("Grid 3: " + grid(3).localNode().id());

        Ignite ignite = grid(0);

        Collection<IgniteFuture<?>> futs = new LinkedList<>();

        IgniteCompute comp = ignite.compute().enableAsync();

        for (int i = 0; i < jobCnt; i++) {
            comp.apply(new CX1<Integer, Void>() {
                @IgniteInstanceResource
                private Ignite ignite;

                @Override public Void applyx(final Integer i) throws GridException {
                    GridCache<String, int[]> cache = this.ignite.cache(null);

                    try (GridCacheTx tx = cache.txStart(concur, isolation)) {
                        int[] arr = cache.get("TestKey");

                        if (arr == null)
                            arr = new int[jobCnt];

                        arr[i] = 1;

                        cache.put("TestKey", arr);

                        int c = cntr.getAndIncrement();

                        if (c % 50 == 0)
                            X.println("Executing transaction [i=" + i + ", c=" + c + ']');

                        tx.commit();
                    }

                    return null;
                }
            }, i);

            futs.add(comp.future());
        }

        for (IgniteFuture<?> fut : futs)
            fut.get(); // Wait for completion.

        for (int i = 0; i < GRID_CNT; i++) {
            GridCacheProjection<String, int[]> c = grid(i).cache(null).projection(String.class, int[].class);

            // Do within transaction to make sure that lock is acquired
            // which means that all previous transactions have committed.

            try (GridCacheTx tx = c.txStart(concur, isolation)) {
                int[] arr = c.get("TestKey");

                assertNotNull(arr);
                assertEquals(jobCnt, arr.length);

                for (int j : arr)
                    assertEquals(1, j);

                tx.commit();
            }
        }
    }
}
