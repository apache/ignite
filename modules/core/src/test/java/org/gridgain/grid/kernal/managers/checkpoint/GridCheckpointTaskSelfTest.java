/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.checkpoint;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.checkpoint.*;
import org.gridgain.grid.spi.checkpoint.cache.*;
import org.gridgain.grid.spi.discovery.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Checkpoint tests.
 */
public class GridCheckpointTaskSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Checkpoints cache name. */
    private static final String CACHE_NAME = "checkpoints.cache";

    /** Checkpoint key. */
    private static final String CP_KEY = "test.checkpoint.key." + System.currentTimeMillis();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setCacheConfiguration(cacheConfiguration());
        cfg.setCheckpointSpi(checkpointSpi());
        cfg.setDiscoverySpi(discoverySpi());

        return cfg;
    }

    /**
     * @return Cache configuration.
     */
    private GridCacheConfiguration cacheConfiguration() {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setName(CACHE_NAME);
        cfg.setCacheMode(REPLICATED);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return Checkpoint SPI.
     */
    private GridCheckpointSpi checkpointSpi() {
        GridCacheCheckpointSpi spi = new GridCacheCheckpointSpi();

        spi.setCacheName(CACHE_NAME);

        return spi;
    }

    /**
     * @return Discovery SPI.
     */
    private GridDiscoverySpi discoverySpi() {
        GridTcpDiscoverySpi spi = new GridTcpDiscoverySpi();

        spi.setIpFinder(IP_FINDER);

        return spi;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid(1);
        startGrid(2);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        assert grid(1).cache(CACHE_NAME).isEmpty() : grid(1).cache(CACHE_NAME).entrySet();
        assert grid(2).cache(CACHE_NAME).isEmpty() : grid(2).cache(CACHE_NAME).entrySet();

        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    public void testFailover() throws Exception {
        grid(1).compute().execute(FailoverTestTask.class, null);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReduce() throws Exception {
        grid(1).compute().execute(ReduceTestTask.class, null);
    }

    /**
     * Failover test task.
     */
    @GridComputeTaskSessionFullSupport
    private static class FailoverTestTask extends ComputeTaskAdapter<Void, Void> {
        /** Grid. */
        @GridInstanceResource
        private Ignite ignite;

        /** Task session. */
        @GridTaskSessionResource
        private ComputeTaskSession ses;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Void arg) throws GridException {
            assert ignite.cluster().nodes().size() == 2;

            ClusterNode rmt = F.first(ignite.cluster().forRemotes().nodes());

            ses.saveCheckpoint(CP_KEY, true);

            return F.asMap(
                new ComputeJobAdapter() {
                    @GridLocalNodeIdResource
                    private UUID nodeId;

                    @GridTaskSessionResource
                    private ComputeTaskSession ses;

                    @Override public Object execute() throws GridException {
                        X.println("Executing FailoverTestTask job on node " + nodeId);

                        Boolean cpVal = ses.loadCheckpoint(CP_KEY);

                        assert cpVal != null;

                        if (cpVal) {
                            ses.saveCheckpoint(CP_KEY, false);

                            throw new ComputeExecutionRejectedException("Failing over the job.");
                        }

                        return null;
                    }
                },
                rmt
            );
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     * Failover test task.
     */
    @GridComputeTaskSessionFullSupport
    private static class ReduceTestTask extends ComputeTaskAdapter<Void, Void> {
        /** Grid. */
        @GridInstanceResource
        private Ignite ignite;

        /** Task session. */
        @GridTaskSessionResource
        private ComputeTaskSession ses;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Void arg) throws GridException {
            assert ignite.cluster().nodes().size() == 2;

            ClusterNode rmt = F.first(ignite.cluster().forRemotes().nodes());

            return F.asMap(
                new ComputeJobAdapter() {
                    @GridLocalNodeIdResource
                    private UUID nodeId;

                    @GridTaskSessionResource
                    private ComputeTaskSession ses;

                    @Override public Object execute() throws GridException {
                        X.println("Executing ReduceTestTask job on node " + nodeId);

                        ses.saveCheckpoint(CP_KEY, true);

                        return null;
                    }
                },
                rmt
            );
        }

        /** {@inheritDoc} */
        @Override public Void reduce(List<ComputeJobResult> results) throws GridException {
            assert ses.loadCheckpoint(CP_KEY) != null;

            return null;
        }
    }
}
