/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.p2p;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.testframework.junits.common.*;

import static org.apache.ignite.configuration.IgniteDeploymentMode.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCachePreloadMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Tests for continuous deployment with cache and changing topology.
 */
public class GridP2PContinuousDeploymentSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** Number of grids cache. */
    private static final int GRID_CNT = 2;

    /** Name for grid without cache. */
    private static final String GRID_NAME = "grid-no-cache";

    /** First test task name. */
    private static final String TEST_TASK_1 = "org.gridgain.grid.tests.p2p.GridP2PContinuousDeploymentTask1";

    /** Second test task name. */
    private static final String TEST_TASK_2 = "org.gridgain.grid.tests.p2p.GridP2PContinuousDeploymentTask2";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDeploymentMode(CONTINUOUS);

        if (GRID_NAME.equals(gridName))
            cfg.setCacheConfiguration();
        else
            cfg.setCacheConfiguration(cacheConfiguration());

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        return cfg;
    }

    /**
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    protected GridCacheConfiguration cacheConfiguration() throws Exception {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(PARTITIONED);
        cfg.setBackups(1);
        cfg.setWriteSynchronizationMode(FULL_SYNC);
        cfg.setPreloadMode(SYNC);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testDeployment() throws Exception {
        Ignite ignite = startGrid(GRID_NAME);

        Class cls = getExternalClassLoader().loadClass(TEST_TASK_1);

        compute(ignite.cluster().forRemotes()).execute(cls, null);

        stopGrid(GRID_NAME);

        ignite = startGrid(GRID_NAME);

        cls = getExternalClassLoader().loadClass(TEST_TASK_2);

        compute(ignite.cluster().forRemotes()).execute(cls, null);

        stopGrid(GRID_NAME);
    }
}
