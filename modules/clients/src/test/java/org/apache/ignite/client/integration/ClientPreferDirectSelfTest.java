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

package org.apache.ignite.client.integration;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.client.*;
import org.apache.ignite.client.balancer.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.junits.common.*;

import java.util.*;

import static org.apache.ignite.client.integration.ClientAbstractMultiNodeSelfTest.*;

/**
 *
 */
public class ClientPreferDirectSelfTest extends GridCommonAbstractTest {
    /** VM ip finder for TCP discovery. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** */
    private static final int NODES_CNT = 6;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        info("Stopping grids.");

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();

        disco.setIpFinder(IP_FINDER);

        c.setDiscoverySpi(disco);

        c.setLocalHost(HOST);

        assert c.getClientConnectionConfiguration() == null;

        ClientConnectionConfiguration clientCfg = new ClientConnectionConfiguration();

        clientCfg.setRestTcpPort(REST_TCP_PORT_BASE);

        c.setClientConnectionConfiguration(clientCfg);

        return c;
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomBalancer() throws Exception {
        GridClientRandomBalancer b = new GridClientRandomBalancer();

        b.setPreferDirectNodes(true);

        executeTest(b);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRoundRobinBalancer() throws Exception {
        GridClientRoundRobinBalancer b = new GridClientRoundRobinBalancer();

        b.setPreferDirectNodes(true);

        executeTest(b);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    private void executeTest(GridClientLoadBalancer b) throws Exception {
        try (GridClient c = client(b)) {
            Set<String> executions = new HashSet<>();

            for (int i = 0; i < NODES_CNT * 10; i++)
                executions.add(
                    c.compute().<String>execute(TestTask.class.getName(), null));

            assertEquals(NODES_CNT / 2, executions.size());

            for (int i = 0; i < NODES_CNT / 2; i++)
                executions.contains(grid(i).localNode().id().toString());
        }
    }

    /**
     * @param b Balancer.
     * @return Client.
     * @throws Exception If failed.
     */
    private GridClient client(GridClientLoadBalancer b) throws Exception {
        GridClientConfiguration cfg = new GridClientConfiguration();

        cfg.setBalancer(b);

        cfg.setTopologyRefreshFrequency(TOP_REFRESH_FREQ);

        Collection<String> rtrs = new ArrayList<>(3);

        for (int i = 0; i < NODES_CNT / 2; i++)
            rtrs.add(HOST + ':' + (REST_TCP_PORT_BASE + i));

        cfg.setRouters(rtrs);

        return GridClientFactory.start(cfg);
    }

    /**
     * Test task. Returns Id of the node that has split the task,
     */
    private static class TestTask extends ComputeTaskSplitAdapter<Object, String> {
        @IgniteInstanceResource
        private Ignite ignite;

        /** Count of tasks this job was split to. */
        private int gridSize;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg)
            throws IgniteCheckedException {
            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            this.gridSize = gridSize;

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    @Override public Object execute() {
                        try {
                            Thread.sleep(100);
                        }
                        catch (InterruptedException ignored) {
                            Thread.currentThread().interrupt();
                        }

                        return "OK";
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public String reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            int sum = 0;

            for (ComputeJobResult res : results) {
                assertNotNull(res.getData());

                sum += 1;
            }

            assert gridSize == sum;

            return ignite.cluster().localNode().id().toString();
        }
    }
}
