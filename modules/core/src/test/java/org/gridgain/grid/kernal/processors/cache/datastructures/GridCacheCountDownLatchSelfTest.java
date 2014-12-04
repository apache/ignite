/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import static java.util.concurrent.TimeUnit.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Cache count down latch self test.
 */
public class GridCacheCountDownLatchSelfTest extends GridCommonAbstractTest implements Externalizable {
    /** */
    private static final int NODES_CNT = 4;

    /** */
    private static final int THREADS_CNT = 5;

    /** */
    private static GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final Random RND = new Random();

    /**
     *
     */
    public GridCacheCountDownLatchSelfTest() {
        super(false /* start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost("127.0.0.1");

        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(spi);

        GridCacheConfiguration repCacheCfg = defaultCacheConfiguration();

        repCacheCfg.setName("replicated");
        repCacheCfg.setCacheMode(REPLICATED);
        repCacheCfg.setPreloadMode(SYNC);
        repCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        repCacheCfg.setEvictionPolicy(null);
        repCacheCfg.setAtomicityMode(TRANSACTIONAL);
        repCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        GridCacheConfiguration partCacheCfg = defaultCacheConfiguration();

        partCacheCfg.setName("partitioned");
        partCacheCfg.setCacheMode(PARTITIONED);
        partCacheCfg.setBackups(1);
        partCacheCfg.setPreloadMode(SYNC);
        partCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        partCacheCfg.setEvictionPolicy(null);
        partCacheCfg.setNearEvictionPolicy(null);
        partCacheCfg.setAtomicityMode(TRANSACTIONAL);
        partCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        GridCacheConfiguration locCacheCfg = defaultCacheConfiguration();

        locCacheCfg.setName("local");
        locCacheCfg.setCacheMode(LOCAL);
        locCacheCfg.setWriteSynchronizationMode(FULL_SYNC);
        locCacheCfg.setEvictionPolicy(null);
        locCacheCfg.setAtomicityMode(TRANSACTIONAL);
        locCacheCfg.setDistributionMode(NEAR_PARTITIONED);

        cfg.setCacheConfiguration(repCacheCfg, partCacheCfg, locCacheCfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        super.afterTestsStopped();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatchReplicated() throws Exception {
        checkLatch("replicated");
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatchPartitioned() throws Exception {
        checkLatch("partitioned");
    }

    /**
     * @throws Exception If failed.
     */
    public void testLatchLocal() throws Exception {
        // Test main functionality.
        GridCacheCountDownLatch latch = grid(0).cache("local").dataStructures().countDownLatch("latch", 2, false,
            true);

        assert latch.count() == 2;

        GridFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
            new Callable<Object>() {
                @Nullable @Override public Object call() throws Exception {
                    GridCacheCountDownLatch latch = grid(0).cache("local").dataStructures()
                        .countDownLatch("latch", 2, false, true);

                    assert latch != null && latch.count() == 2;

                    info("Thread is going to wait on latch: " + Thread.currentThread().getName());

                    assert latch.await(1, MINUTES);

                    info("Thread is again runnable: " + Thread.currentThread().getName());

                    return null;
                }
            },
            THREADS_CNT,
            "test-thread"
        );

        Thread.sleep(3000);

        assert latch.countDown()  == 1;

        assert latch.countDown() == 0;

        assert latch.await(1, SECONDS);

        // Ensure there are no hangs.
        fut.get();

        // Test operations on removed latch.
        grid(0).cache("local").dataStructures().removeCountDownLatch("latch");

        checkRemovedLatch(latch);
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception If failed.
     */
    private void checkLatch(final String cacheName) throws Exception {
        // Test API.
        checkAutoDelete(cacheName);

        checkAwait(cacheName);

        checkCountDown(cacheName);

        // Test main functionality.
        GridCacheCountDownLatch latch1 = grid(0).cache(cacheName).dataStructures().countDownLatch("latch", 2, false,
            true);

        assert latch1.count() == 2;

        GridCompute comp = grid(0).compute().enableAsync();

        comp.call(new IgniteCallable<Object>() {
            @GridInstanceResource
            private Ignite ignite;

            @GridLoggerResource
            private GridLogger log;

            @Nullable @Override public Object call() throws Exception {
                // Test latch in multiple threads on each node.
                GridFuture<?> fut = GridTestUtils.runMultiThreadedAsync(
                    new Callable<Object>() {
                        @Nullable @Override public Object call() throws Exception {
                            GridCacheCountDownLatch latch = ignite.cache(cacheName).dataStructures()
                                .countDownLatch("latch", 2, false, true);

                            assert latch != null && latch.count() == 2;

                            log.info("Thread is going to wait on latch: " + Thread.currentThread().getName());

                            assert latch.await(1, MINUTES);

                            log.info("Thread is again runnable: " + Thread.currentThread().getName());

                            return null;
                        }
                    },
                    5,
                    "test-thread"
                );

                fut.get();

                return null;
            }
        });

        GridFuture<Object> fut = comp.future();

        Thread.sleep(3000);

        assert latch1.countDown() == 1;

        assert latch1.countDown() == 0;

        // Ensure there are no hangs.
        fut.get();

        // Test operations on removed latch.
        grid(0).cache(cacheName).dataStructures().removeCountDownLatch("latch");

        checkRemovedLatch(latch1);
    }

    /**
     * @param latch Latch.
     *
     * @throws Exception If failed.
     */
    private void checkRemovedLatch(GridCacheCountDownLatch latch) throws Exception {
        assert latch.removed();

        assert latch.count() == 0;

        // Test await on removed future.
        latch.await();
        assert latch.await(10);
        assert latch.await(10, SECONDS);

        latch.await();

        // Test countdown.
        assert latch.countDown() == 0;
        assert latch.countDown(5) == 0;
        latch.countDownAll();
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception Exception.
     */
    private void checkAutoDelete(String cacheName) throws Exception {
        GridCacheCountDownLatch latch = createLatch(cacheName, "rmv", 5, true);

        latch.countDownAll();

        // Latch should be removed since autoDelete = true
        checkRemovedLatch(latch);

        GridCacheCountDownLatch latch1 = createLatch(cacheName, "rmv1", 5, false);

        latch1.countDownAll();

        // Latch should NOT be removed since autoDelete = false
        assert !latch1.removed();

        removeLatch(cacheName, "rmv1");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception Exception.
     */
    private void checkAwait(String cacheName) throws Exception {
        // Check only 'false' cases here. Successful await is tested over the grid.
        GridCacheCountDownLatch latch = createLatch(cacheName, "await", 5, true);

        assert !latch.await(10);
        assert !latch.await(10, MILLISECONDS);

        removeLatch(cacheName, "await");
    }

    /**
     * @param cacheName Cache name.
     * @throws Exception Exception.
     */
    private void checkCountDown(String cacheName) throws Exception {
        GridCacheCountDownLatch latch = createLatch(cacheName, "cnt", 10, true);

        assert latch.countDown() == 9;
        assert latch.countDown(2) == 7;

        latch.countDownAll();

        assert latch.count() == 0;

        checkRemovedLatch(latch);

        GridCacheCountDownLatch latch1 = createLatch(cacheName, "cnt1", 10, true);

        assert latch1.countDown() == 9;
        assert latch1.countDown(2) == 7;

        latch1.countDownAll();

        assert latch1.count() == 0;

        checkRemovedLatch(latch1);
    }

    /**
     * @param cacheName Cache name.
     * @param latchName Latch name.
     * @param cnt Count.
     * @param autoDel Auto delete flag.
     * @throws Exception If failed.
     * @return New latch.
     */
    private GridCacheCountDownLatch createLatch(String cacheName, String latchName, int cnt, boolean autoDel)
        throws Exception {
        GridCacheCountDownLatch latch = grid(RND.nextInt(NODES_CNT)).cache(cacheName).dataStructures()
            .countDownLatch(latchName, cnt, autoDel, true);

        // Test initialization.
        assert latchName.equals(latch.name());
        assert latch.count() == cnt;
        assert latch.initialCount() == cnt;
        assert latch.autoDelete() == autoDel;

        return latch;
    }

    /**
     * @param cacheName Cache name.
     * @param latchName Latch name.
     * @throws Exception If failed.
     */
    private void removeLatch(String cacheName, String latchName)
        throws Exception {
        GridCacheCountDownLatch latch = grid(RND.nextInt(NODES_CNT)).cache(cacheName).dataStructures()
            .countDownLatch(latchName, 10, false, true);

        assert latch != null;

        if (latch.count() > 0)
            latch.countDownAll();

        // Remove latch on random node.
        grid(RND.nextInt(NODES_CNT)).cache(cacheName).dataStructures().removeCountDownLatch(latchName);

        // Ensure latch is removed on all nodes.
        for (Ignite g : G.allGrids())
            assert ((GridKernal)g).internalCache(cacheName).context().dataStructures().
                countDownLatch(latchName, 10, true, false) == null;

        checkRemovedLatch(latch);
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }
}
