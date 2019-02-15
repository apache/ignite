/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.rest;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.client.GridClient;
import org.apache.ignite.internal.client.GridClientCompute;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDataConfiguration;
import org.apache.ignite.internal.client.GridClientFactory;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandler;
import org.apache.ignite.internal.processors.rest.handlers.task.GridTaskCommandHandler;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jsr166.ConcurrentLinkedHashMap;
import org.junit.Test;

import static org.apache.ignite.cache.CacheMode.LOCAL;
import static org.apache.ignite.cache.CacheMode.PARTITIONED;
import static org.apache.ignite.cache.CacheMode.REPLICATED;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;
import static org.apache.ignite.internal.client.GridClientProtocol.TCP;

/**
 * Test for {@code GridTaskCommandHandler}
 */
public class TaskCommandHandlerSelfTest extends GridCommonAbstractTest {
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
        System.setProperty(IgniteSystemProperties.IGNITE_REST_MAX_TASK_RESULTS, String.valueOf(MAX_TASK_RESULTS));

        startGrid(0);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        System.clearProperty(IgniteSystemProperties.IGNITE_REST_MAX_TASK_RESULTS);
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
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setLocalHost(HOST);

        assert cfg.getConnectorConfiguration() == null;

        ConnectorConfiguration clientCfg = new ConnectorConfiguration();

        clientCfg.setPort(BINARY_PORT);

        cfg.setConnectorConfiguration(clientCfg);

        cfg.setCacheConfiguration(cacheConfiguration(DEFAULT_CACHE_NAME), cacheConfiguration("replicated"),
            cacheConfiguration("partitioned"), cacheConfiguration(CACHE_NAME));

        return cfg;
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     * @throws Exception In case of error.
     */
    private CacheConfiguration cacheConfiguration(@NotNull String cacheName) throws Exception {
        CacheConfiguration cfg = defaultCacheConfiguration();

        cfg.setCacheMode(DEFAULT_CACHE_NAME.equals(cacheName) || CACHE_NAME.equals(cacheName) ? LOCAL : "replicated".equals(cacheName) ?
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
    @Test
    public void testManyTasksRun() throws Exception {
        GridClientCompute compute = client.compute();

        for (int i = 0; i < 1000; i++)
            assertEquals(new Integer("executing".length()), compute.execute(TestTask.class.getName(), "executing"));

        GridClientFactory.stop(client.id(), true);

        IgniteKernal g = (IgniteKernal)grid(0);

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
        @Override protected Collection<? extends ComputeJob> split(int gridSize, final String arg) {
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
        @Override public Integer reduce(List<ComputeJobResult> results) {
            int sum = 0;

            for (ComputeJobResult res : results)
                sum += res.<Integer>getData();

            return sum;
        }
    }
}
