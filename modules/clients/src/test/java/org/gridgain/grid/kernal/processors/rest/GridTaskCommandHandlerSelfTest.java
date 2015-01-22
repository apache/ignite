/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.grid.kernal.processors.rest;

import org.apache.ignite.*;
import org.apache.ignite.cache.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.client.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.rest.handlers.*;
import org.gridgain.grid.kernal.processors.rest.handlers.task.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.testframework.junits.common.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.util.*;

import static org.apache.ignite.client.GridClientProtocol.*;
import static org.apache.ignite.cache.GridCacheMode.*;
import static org.apache.ignite.cache.GridCacheWriteSynchronizationMode.*;

/**
 * Test for {@code GridTaskCommandHandler}
 */
public class GridTaskCommandHandlerSelfTest extends GridCommonAbstractTest {
    /** */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

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
        System.setProperty(IgniteSystemProperties.GG_REST_MAX_TASK_RESULTS, String.valueOf(MAX_TASK_RESULTS));

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        System.clearProperty(IgniteSystemProperties.GG_REST_MAX_TASK_RESULTS);
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

        ClientConnectionConfiguration clientCfg = new ClientConnectionConfiguration();

        clientCfg.setRestTcpPort(BINARY_PORT);

        cfg.setClientConnectionConfiguration(clientCfg);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

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
    private CacheConfiguration cacheConfiguration(@Nullable String cacheName) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

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
    private static class TestTask extends ComputeTaskSplitAdapter<String, Integer> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, final String arg) throws IgniteCheckedException {
            return Collections.singletonList(new ComputeJobAdapter() {
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
        @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            int sum = 0;

            for (ComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }
}
