/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.distributed.replicated.preloader;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.apache.ignite.events.GridEventType.*;

/**
 * Tests that preload start/preload stop events are fired only once for replicated cache.
 */
public class GridCacheReplicatedPreloadStartStopEventsSelfTest extends GridCommonAbstractTest {
    /** */
    private GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        ((GridTcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        GridCacheConfiguration ccfg = new GridCacheConfiguration();

        ccfg.setCacheMode(REPLICATED);

        cfg.setCacheConfiguration(ccfg);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testStartStopEvents() throws Exception {
        Ignite ignite = startGrid(0);

        final AtomicInteger preloadStartCnt = new AtomicInteger();
        final AtomicInteger preloadStopCnt = new AtomicInteger();

        ignite.events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent e) {
                if (e.type() == EVT_CACHE_PRELOAD_STARTED)
                    preloadStartCnt.incrementAndGet();
                else if (e.type() == EVT_CACHE_PRELOAD_STOPPED)
                    preloadStopCnt.incrementAndGet();
                else
                    fail("Unexpected event type: " + e.type());

                return true;
            }
        }, EVT_CACHE_PRELOAD_STARTED, EVT_CACHE_PRELOAD_STOPPED);

        startGrid(1);

        startGrid(2);

        startGrid(3);

        assertTrue("Unexpected start count: " + preloadStartCnt.get(), preloadStartCnt.get() <= 1);
        assertTrue("Unexpected stop count: " + preloadStopCnt.get(), preloadStopCnt.get() <= 1);
    }
}
