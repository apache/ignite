/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures.partitioned;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.SYNC;
import static org.gridgain.grid.cache.GridCacheTxConcurrency.PESSIMISTIC;
import static org.gridgain.grid.cache.GridCacheTxIsolation.REPEATABLE_READ;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 *
 */
public class GridCachePartitionedQueueCreateMultiNodeSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        c.setDiscoverySpi(spi);
        c.setIncludeEventTypes();
        c.setPeerClassLoadingEnabled(false);

        // Default cache configuration.
        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setPreloadMode(SYNC);
        cc.setBackups(0);

        c.setCacheConfiguration(cc);

        return c;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids(true);
    }

    /**
     * @throws Exception If failed.
     *
     * // TODO: GG-7394 - Enable when fixed.
     */
    public void _testQueueCreation() throws Exception {
        final AtomicInteger idx = new AtomicInteger();

        GridFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Grid grid = startGrid(idx.getAndIncrement());

                    UUID locNodeId = grid.localNode().id();

                    info("Started grid: " + locNodeId);

                    GridCache<String, ?> cache = grid.cache(null);

                    info("Creating queue: " + locNodeId);

                    GridCacheQueue<String> q = cache.dataStructures().queue("queue", 1, true, true);

                    assert q != null;

                    info("Putting first value: " + locNodeId);

                    q.offer("val", 1000, MILLISECONDS);

                    info("Putting second value: " + locNodeId);

                    boolean res2 = q.offer("val1", 1000, MILLISECONDS);

                    assert !res2;

                    info("Thread finished: " + locNodeId);

                    return null;
                }
            },
            10
        );

        fut.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testTx() throws Exception {
        int threadCnt = 10;

        final AtomicInteger idx = new AtomicInteger();
        final AtomicBoolean flag = new AtomicBoolean();

        final CountDownLatch latch = new CountDownLatch(threadCnt);

        GridFuture<?> fut = multithreadedAsync(
            new Callable<Object>() {
                @Override public Object call() throws Exception {
                    Grid grid = startGrid(idx.getAndIncrement());

                    boolean wait = false;

                    if (wait) {
                        latch.countDown();

                        latch.await();
                    }

                    // If output presents, test passes with greater probability.
                    // info("Start puts.");

                    GridCache<Integer, String> cache = grid.cache(null);

                    info("Partition: " + cache.affinity().partition(1));

                    GridCacheTx tx = cache.txStart(PESSIMISTIC, REPEATABLE_READ);

                    try {
                        // info("Getting value for key 1");

                        String s = cache.get(1);

                        // info("Got value: " + s);

                        if (s == null) {
                            assert flag.compareAndSet(false, true);

                            // info("Putting value.");

                            cache.putx(1, "val");

                            // info("Done putting value");

                            tx.commit();
                        }
                        else
                            assert "val".equals(s) : "String: " + s;
                    }
                    finally {
                        tx.close();
                    }

                    info("Thread finished for grid: " + grid.name());

                    return null;
                }
            },
            threadCnt
        );

        fut.get();
    }
}
