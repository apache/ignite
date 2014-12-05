/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.configuration.IgniteDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Multithreaded tests for replicated cache preloader.
 */
public class GridCacheSyncReplicatedPreloadSelfTest extends GridCommonAbstractTest {
    /** */
    private TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final boolean DISCO_DEBUG_MODE = false;


    /**
     * Constructs test.
     */
    public GridCacheSyncReplicatedPreloadSelfTest() {
        super(false /* don't start grid. */);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(ipFinder);
        disco.setDebugMode(DISCO_DEBUG_MODE);

        cfg.setDiscoverySpi(disco);

        GridCacheConfiguration cacheCfg = defaultCacheConfiguration();

        cacheCfg.setCacheMode(REPLICATED);
        cacheCfg.setDistributionMode(PARTITIONED_ONLY);
        cacheCfg.setWriteSynchronizationMode(FULL_SYNC);

        // This property is essential for this test.
        cacheCfg.setPreloadMode(SYNC);

        cacheCfg.setPreloadBatchSize(10000);

        cfg.setCacheConfiguration(cacheCfg);
        cfg.setDeploymentMode(CONTINUOUS);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testNodeRestart() throws Exception {
        int keyCnt = 1000;
        int retries = 20;

        Ignite g0 = startGrid(0);
        Ignite g1 = startGrid(1);

        for (int i = 0; i < keyCnt; i++)
            g0.cache(null).putx(i, i);

        assertEquals(keyCnt, g0.cache(null).size());
        assertEquals(keyCnt, g1.cache(null).size());

        for (int n = 0; n < retries; n++) {
            info("Starting additional grid node...");

            Ignite g2 = startGrid(2);

            assertEquals(keyCnt, g2.cache(null).size());

            info("Stopping additional grid node...");

            stopGrid(2);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings({"TooBroadScope"})
    public void testNodeRestartMultithreaded() throws Exception {
        final int keyCnt = 1000;
        final int retries = 300;
        int threadCnt = 5;

        Ignite g0 = startGrid(0);
        Ignite g1 = startGrid(1);

        for (int i = 0; i < keyCnt; i++)
            g0.cache(null).putx(i, i);

        assertEquals(keyCnt, g0.cache(null).size());
        assertEquals(keyCnt, g1.cache(null).size());

        final AtomicInteger cnt = new AtomicInteger();

        multithreaded(
            new Callable() {
                @Nullable @Override public Object call() throws Exception {
                    while (true) {
                        int c = cnt.incrementAndGet();

                        if (c > retries)
                            break;

                        int idx = c + 1;

                        info("Starting additional grid node with index: " + idx);

                        startGrid(idx);

                        info("Stopping additional grid node with index: " + idx);

                        stopGrid(idx);
                    }

                    return null;
                }
            },
            threadCnt);
    }
}
