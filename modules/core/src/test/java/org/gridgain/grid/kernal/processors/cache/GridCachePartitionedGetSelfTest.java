/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache;

import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.affinity.consistenthash.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.processors.cache.distributed.near.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheDistributionMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;
import static org.gridgain.grid.kernal.GridTopic.*;

/**
 *
 */
public class GridCachePartitionedGetSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final int GRID_CNT = 3;

    /** */
    private static final String KEY = "key";

    /** */
    private static final int VAL = 1;

    /** */
    private static final AtomicBoolean received = new AtomicBoolean();

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(discoverySpi());
        cfg.setCacheConfiguration(cacheConfiguration());

        return cfg;
    }

    /**
     * @return Discovery SPI;
     */
    private GridDiscoverySpi discoverySpi() {
        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /**
     * @return Cache configuration.
     */
    private GridCacheConfiguration cacheConfiguration() {
        GridCacheConfiguration cc = defaultCacheConfiguration();

        cc.setCacheMode(PARTITIONED);
        cc.setBackups(1);
        cc.setPreloadMode(SYNC);
        cc.setWriteSynchronizationMode(FULL_SYNC);
        cc.setSwapEnabled(true);
        cc.setEvictNearSynchronized(false);
        cc.setEvictSynchronized(false);
        cc.setDistributionMode(PARTITIONED_ONLY);

        return cc;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);

        prepare();
    }

    @Override protected void beforeTest() throws Exception {
        received.set(false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetFromPrimaryNode() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            GridCache<String, Integer> c = grid(i).cache(null);

            GridCacheEntry<String, Integer> e = c.entry(KEY);

            if (e.primary()) {
                info("Primary node: " + grid(i).localNode().id());

                c.get(KEY);

                break;
            }
        }

        assert !await();
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetFromBackupNode() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            GridCache<String, Integer> c = grid(i).cache(null);

            GridCacheEntry<String, Integer> e = c.entry(KEY);

            if (e.backup()) {
                info("Backup node: " + grid(i).localNode().id());

                Integer val = c.get(KEY);

                assert val != null && val == 1;

                assert !await();

                assert c.evict(KEY);

                assert c.peek(KEY) == null;

                val = c.get(KEY);

                assert val != null && val == 1;

                assert !await();

                break;
            }
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testGetFromNearNode() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            GridCache<String, Integer> c = grid(i).cache(null);

            GridCacheEntry<String, Integer> e = c.entry(KEY);

            if (!e.primary() && !e.backup()) {
                info("Near node: " + grid(i).localNode().id());

                Integer val = c.get(KEY);

                assert val != null && val == 1;

                break;
            }
        }

        assert await();
    }

    /**
     * @return {@code True} if awaited message.
     * @throws Exception If failed.
     */
    @SuppressWarnings({"BusyWait"})
    private boolean await() throws Exception {
        info("Checking flag: " + System.identityHashCode(received));

        for (int i = 0; i < 3; i++) {
            if (received.get())
                return true;

            info("Flag is false.");

            Thread.sleep(500);
        }

        return received.get();
    }

    /**
     * Puts value to primary node and registers listener
     * that sets {@link #received} flag to {@code true}
     * if {@link GridNearGetRequest} was received on primary node.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("deprecation")
    private void prepare() throws Exception {
        for (int i = 0; i < GRID_CNT; i++) {
            Grid g = grid(i);

            GridCacheEntry<String, Integer> e = g.<String, Integer>cache(null).entry(KEY);

            if (e.primary()) {
                info("Primary node: " + g.cluster().localNode().id());

                // Put value.
                g.cache(null).put(KEY, VAL);

                // Register listener.
                ((GridKernal)g).context().io().addMessageListener(
                    TOPIC_CACHE,
                    new GridMessageListener() {
                        @Override public void onMessage(UUID nodeId, Object msg) {
                            info("Received message from node [nodeId=" + nodeId + ", msg=" + msg + ']');

                            if (msg instanceof GridNearGetRequest) {
                                info("Setting flag: " + System.identityHashCode(received));

                                received.set(true);
                            }
                        }
                    }
                );

                break;
            }
        }
    }
}
