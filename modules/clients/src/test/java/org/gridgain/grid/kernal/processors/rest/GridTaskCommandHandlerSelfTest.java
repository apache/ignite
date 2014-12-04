/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.rest;

import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.gridgain.client.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.kernal.processors.rest.handlers.task.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.gridgain.client.GridClientProtocol.*;
import static org.gridgain.grid.cache.GridCacheMode.*;
import static org.gridgain.grid.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test for {@code GridTaskCommandHandler}
 */
public class GridTaskCommandHandlerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final GridTcpDiscoveryIpFinder IP_FINDER = new GridTcpDiscoveryVmIpFinder(true);

    /** */
    private static final String CACHE_NAME = "cache";

    /** */
    public static final String HOST = "127.0.0.1";

    /** */
    public static final int BINARY_PORT = 11212;

    /** */
    private static final int MAX_TASK_RESULTS = 10;

    /** */
    private GridClient client;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(GridSystemProperties.GG_REST_MAX_TASK_RESULTS, String.valueOf(MAX_TASK_RESULTS));

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        System.clearProperty(GridSystemProperties.GG_REST_MAX_TASK_RESULTS);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        client = GridClientFactory.start(clientConfiguration());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        GridClientFactory.stop(client.id());
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setLocalHost(HOST);

        assert cfg.getClientConnectionConfiguration() == null;

        GridClientConnectionConfiguration clientCfg = new GridClientConnectionConfiguration();

        clientCfg.setRestTcpPort(BINARY_PORT);

        cfg.setClientConnectionConfiguration(clientCfg);

        GridTcpDiscoverySpi disco = new GridTcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(disco);

        cfg.setCacheConfiguration(cacheConfiguration(null), cacheConfiguration("replicated"),
            cacheConfiguration("partitioned"), cacheConfiguration(CACHE_NAME));

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private GridCacheConfiguration cacheConfiguration(@Nullable String cacheName) throws Exception {
        GridCacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(cacheName == null || CACHE_NAME.equals(cacheName) ? LOCAL : "replicated".equals(cacheName) ?
            REPLICATED : PARTITIONED);
        cfg.setName(cacheName);
        cfg.setWriteSynchronizationMode(FULL_SYNC);

        return cfg;
    }

    /**
     * @return Client configuration.
     */
    private GridClientConfiguration clientConfiguration() {
        GridClientConfiguration cfg = new GridClientConfiguration();

        GridClientDataConfiguration nullCache = new GridClientDataConfiguration();

        GridClientDataConfiguration cache = new GridClientDataConfiguration();

        cache.setName(CACHE_NAME);

        cfg.setDataConfigurations(Arrays.asList(nullCache, cache));

        cfg.setProtocol(TCP);
        cfg.setServers(Arrays.asList("localhost:" + BINARY_PORT));

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testManyTasksRun() throws Exception {
        GridClientCompute compute = client.compute();

        for (int i = 0; i < 1000; i++)
            assertEquals("executing".length(), compute.execute(TestTask.class.getName(), "executing"));

        GridClientFactory.stop(client.id(), true);

        GridKernal g = (GridKernal)grid(0);

        Map<GridRestCommand, GridRestCommandHandler> handlers = U.field(g.context().rest(), "handlers");

        GridTaskCommandHandler taskHnd = (GridTaskCommandHandler)F.find(handlers.values(), null,
            new P1<GridRestCommandHandler>() {
                @Override public boolean apply(GridRestCommandHandler e) {
                    return e instanceof GridTaskCommandHandler;
                }
            });

        assertNotNull("GridTaskCommandHandler was not found", taskHnd);

        ConcurrentLinkedHashMap taskDesc = U.field(taskHnd, "taskDescs");

        assertTrue("Task result map size exceeded max value [mapSize=" + taskDesc.sizex() + ", " +
            "maxSize=" + MAX_TASK_RESULTS + ']', taskDesc.sizex() <= MAX_TASK_RESULTS);
    }

    /**
     * Test task.
     */
    private static class TestTask extends GridComputeTaskSplitAdapter<String, Integer> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, final String arg) throws GridException {
            return Collections.singletonList(new GridComputeJobAdapter() {
                @Override public Object execute() {
                    try {
                        Thread.sleep(10);
                    }
                    catch (InterruptedException ignored) {
                        Thread.currentThread().interrupt();
                    }

                    return arg.length();
                }
            });
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
            int sum = 0;

            for (GridComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }
}
