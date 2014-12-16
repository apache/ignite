/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.cache.datastructures;

import org.apache.commons.collections.*;
import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.cache.datastructures.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.kernal.processors.cache.datastructures.GridCacheQueueMultiNodeAbstractSelfTest.*;

/**
 * Consistency test for cache queue in multi node environment.
 */
public class GridCacheQueueMultiNodeConsistencySelfTest extends GridCommonAbstractTest {
    /** */
    protected static final int GRID_CNT = 3;

    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    protected static final int RETRIES = 20;

    /** */
    private static final int PRELOAD_DELAY = 200;

    /** Indicates whether force repartitioning is needed or not. */
    private boolean forceRepartition;

    /** Indicates whether random grid stopping is needed or not. */
    private boolean stopRandomGrid;

    /** */
    private GridCacheConfiguration cc = new GridCacheConfiguration();

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi spi = new TcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(spi);

        cc.setCacheMode(PARTITIONED);
        cc.setQueryIndexEnabled(true);
        cc.setSwapEnabled(false);

        c.setCacheConfiguration(cc);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfNoPreloadingAndBackupDisabled() throws Exception {
        cc.setBackups(0);
        cc.setPreloadPartitionedDelay(-1);
        cc.setPreloadMode(GridCachePreloadMode.NONE);

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfNoPreloadingAndBackupDisabledAndRepartitionForced() throws Exception {
        cc.setBackups(0);
        cc.setPreloadPartitionedDelay(-1);
        cc.setPreloadMode(GridCachePreloadMode.NONE);

        forceRepartition = true;

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfPreloadingIsSyncAndBackupDisabled() throws Exception {
        cc.setBackups(0);
        cc.setPreloadMode(GridCachePreloadMode.SYNC);

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfPreloadingIsAsyncAndBackupDisabled() throws Exception {
        cc.setBackups(0);
        cc.setPreloadMode(GridCachePreloadMode.ASYNC);

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfPreloadingIsSyncAndPartitionedDelayAndBackupDisabled() throws Exception {
        cc.setBackups(0);
        cc.setPreloadPartitionedDelay(PRELOAD_DELAY);
        cc.setPreloadMode(GridCachePreloadMode.SYNC);

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfPreloadingIsAsyncAndPartitionedDelayAndBackupDisabled() throws Exception {
        cc.setBackups(0);
        cc.setPreloadPartitionedDelay(PRELOAD_DELAY);
        cc.setPreloadMode(GridCachePreloadMode.ASYNC);

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfPreloadingIsSyncAndBackupEnabled() throws Exception {
        cc.setBackups(1);
        cc.setPreloadMode(GridCachePreloadMode.SYNC);

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfPreloadingIsAsyncAndBackupEnabled() throws Exception {
        cc.setBackups(1);
        cc.setPreloadMode(GridCachePreloadMode.ASYNC);

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfPreloadingIsSyncAndBackupEnabledAndOneNodeIsKilled() throws Exception {
        cc.setBackups(1);
        cc.setPreloadMode(GridCachePreloadMode.SYNC);

        stopRandomGrid = true;

        checkCacheQueue();
    }

    /**
     * @throws Exception If failed.
     */
    public void testIteratorIfPreloadingIsAsyncAndBackupEnabledAndOneNodeIsKilled() throws Exception {
        cc.setBackups(1);
        cc.setPreloadMode(GridCachePreloadMode.ASYNC);

        stopRandomGrid = true;

        checkCacheQueue();
    }

    /**
     * Starts {@code GRID_CNT} nodes, broadcasts {@code AddAllJob} to them then starts new grid and
     * reads cache queue content and finally asserts queue content is the same.
     *
     * @throws Exception If failed.
     */
    private void checkCacheQueue() throws Exception {
        startGrids(GRID_CNT);

        final String queueName = UUID.randomUUID().toString();

        GridCacheQueue<Integer> queue0 = grid(0).cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
            false, true);

        assertTrue(queue0.isEmpty());

        grid(0).compute().broadcast(new AddAllJob(queueName, RETRIES));

        assertEquals(GRID_CNT * RETRIES, queue0.size());

        if (stopRandomGrid)
            stopGrid(1 + new Random().nextInt(GRID_CNT));

        if (forceRepartition)
            for (int i = 0; i < GRID_CNT; i++)
                grid(i).cache(null).forceRepartition();

        Ignite newIgnite = startGrid(GRID_CNT + 1);

        // Intentionally commented code cause in this way inconsistent queue problem doesn't appear.
        // GridCacheQueue<Integer> newQueue = newGrid.cache().queue(queueName);
        // assertTrue(CollectionUtils.isEqualCollection(queue0, newQueue));

        Collection<Integer> locQueueContent = compute(newIgnite.cluster().forLocal()).call(new IgniteCallable<Collection<Integer>>() {
            @IgniteInstanceResource
            private Ignite grid;

            /** {@inheritDoc} */
            @Override public Collection<Integer> call() throws Exception {
                Collection<Integer> values = new ArrayList<>();

                grid.log().info("Running job [node=" + grid.cluster().localNode().id() + ", job=" + this + "]");

                GridCacheQueue<Integer> locQueue = grid.cache(null).dataStructures().queue(queueName, QUEUE_CAPACITY,
                    false, true);

                grid.log().info("Queue size " + locQueue.size());

                for (Integer element : locQueue)
                    values.add(element);

                return values;
            }
        });

        assertTrue(CollectionUtils.isEqualCollection(queue0, locQueueContent));

        grid(0).cache(null).dataStructures().removeQueue(queueName);
    }
}
