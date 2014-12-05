/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.IgniteSystemProperties.*;
import static org.gridgain.grid.cache.GridCacheAtomicityMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;

/**
 *
 */
public class GridCacheMissingCommitVersionSelfTest extends GridCommonAbstractTest {
    /** */
    private volatile Integer failedKey;

    /** */
    private String maxCompletedTxCount;

    /**
     */
    public GridCacheMissingCommitVersionSelfTest() {
        super(true);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration() throws Exception {
        maxCompletedTxCount = System.getProperty(GG_MAX_COMPLETED_TX_COUNT);

        System.setProperty(GG_MAX_COMPLETED_TX_COUNT, String.valueOf(5));

        IgniteConfiguration cfg = super.getConfiguration();

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(new TcpDiscoveryVmIpFinder(true));

        cfg.setDiscoverySpi(discoSpi);

        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setCacheMode(PARTITIONED);
        ccfg.setAtomicityMode(TRANSACTIONAL);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        System.setProperty(GG_MAX_COMPLETED_TX_COUNT, maxCompletedTxCount != null ? maxCompletedTxCount : "");

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMissingCommitVersion() throws Exception {
        final GridCache<Integer, Integer> cache = cache();

        final int KEYS_PER_THREAD = 10_000;

        final AtomicInteger keyStart = new AtomicInteger();

        GridTestUtils.runMultiThreaded(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int start = keyStart.getAndAdd(KEYS_PER_THREAD);

                for (int i = 0; i < KEYS_PER_THREAD && failedKey == null; i++) {
                    int key = start + i;

                    try {
                        cache.put(key, 1);
                    }
                    catch (Exception e) {
                        log.info("Put failed: " + e);

                        failedKey = key;
                    }
                }


                return null;
            }
        }, 10, "put-thread");

        assertNotNull("Test failed to provoke 'missing commit version' error.", failedKey);

        log.info("Trying to update " + failedKey);

        IgniteFuture<?> fut = cache.putAsync(failedKey, 2);

        try {
            fut.get(5000);
        }
        catch (IgniteFutureTimeoutException ignore) {
            fail("Put failed to finish in 5s.");
        }
    }
}
