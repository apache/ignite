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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.spi.discovery.DiscoverySpiMBean;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpi;
import org.apache.ignite.spi.discovery.zk.ZookeeperDiscoverySpiMBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.zookeeper.ZkTestClientCnxnSocketNIO;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 *
 */
@RunWith(Parameterized.class)
public class ZookeeperDiscoveryRandomStopOrFailConcurrentTest extends ZookeeperDiscoverySpiTestBase {
    /** */
    private static final int NUM_CLIENTS = 10;

    /** */
    private static final int NUM_SERVERS = 10;

    /** */
    private static final int ZK_SESSION_TIMEOUT = 5_000;

    /** */
    @Parameterized.Parameters(name = "stop mode = {0}, with crd = {1}")
    public static Collection<Object[]> parameters() {
        List<Object[]> params = new ArrayList<>();

        for (StopMode stopMode: StopMode.values()) {
            params.add(new Object[] {stopMode, true});
            params.add(new Object[] {stopMode, false});
        }

        return params;
    }

    /** */
    @Parameterized.Parameter(0)
    public StopMode stopMode;

    /** */
    @Parameterized.Parameter(1)
    public boolean killCrd;

    /** */
    private final AtomicLong nodesLeft = new AtomicLong(0);

    /** */
    private final AtomicLong nodesFailed = new AtomicLong(0);

    /** */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setClusterStateOnStart(ClusterState.INACTIVE);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        sesTimeout = ZK_SESSION_TIMEOUT;

        testSockNio = true;

        clientReconnectDisabled = true;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (Ignite g: G.allGrids()) {
            ZkTestClientCnxnSocketNIO cnxn = ZkTestClientCnxnSocketNIO.forNode(g);

            if (cnxn != null)
                cnxn.allowConnect();
        }

        super.afterTest();
    }

    /** {@inheritDoc} */
    @Override protected void waitForTopology(int expSize) throws Exception {
        assertTrue(GridTestUtils.waitForCondition(() -> grid(0).cluster().nodes().size() == expSize, 30_000));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopOrFailConcurrently() throws Exception {
        IgniteEx client = startServersAndClients(NUM_SERVERS, NUM_CLIENTS);

        int crd = getCoordinatorIndex();

        List<Integer> srvToStop = IntStream.range(1, NUM_SERVERS + 1)
            .filter(j -> j != crd)
            .boxed()
            .collect(Collectors.collectingAndThen(Collectors.toList(), list -> {
                Collections.shuffle(list);

                return list.subList(0, NUM_SERVERS / 2);
            }));

        if (killCrd)
            srvToStop.set(0, crd);

        List<Integer> cliToStop = IntStream.range(NUM_SERVERS + 1, NUM_CLIENTS + NUM_SERVERS)
            .boxed()
            .collect(Collectors.collectingAndThen(Collectors.toList(), list -> {
                Collections.shuffle(list);

                return list.subList(0, NUM_CLIENTS / 2);
            }));

        srvToStop.addAll(cliToStop);

        stopOrKillMultithreaded(srvToStop);

        waitForTopology(NUM_CLIENTS + NUM_SERVERS - srvToStop.size());

        checkStopFlagsDeleted(10_000);

        DiscoverySpiMBean mBean = getMbean(client);

        GridTestUtils.waitForCondition(() -> nodesLeft.get() == mBean.getNodesLeft(), 10_000);
        GridTestUtils.waitForCondition(() -> nodesFailed.get() == mBean.getNodesFailed(), 10_000);
    }

    /** */
    private void checkStopFlagsDeleted(long timeout) throws Exception {
        ZookeeperClient zkClient = new ZookeeperClient(getTestResources().getLogger(),
            zkCluster.getConnectString(),
            30_000,
            null);

        ZkIgnitePaths paths = new ZkIgnitePaths(ZookeeperDiscoverySpiTestHelper.IGNITE_ZK_ROOT);

        GridTestUtils.waitForCondition(() -> {
            try {
                return zkClient.getChildren(paths.stoppedNodesFlagsDir).isEmpty();
            }
            catch (Exception e) {
                if (e instanceof InterruptedException)
                    Thread.currentThread().interrupt();

                throw new RuntimeException("Failed to wait for stopped nodes flags", e);
            }
        }, timeout);
    }

    /** */
    private void stopOrKillMultithreaded(final List<Integer> stopIndices) throws Exception {
        log.info("Stopping or killing nodes by idx: " + stopIndices.toString());

        final StopMode mode = stopMode;

        GridTestUtils.runMultiThreaded((idx) -> {
            try {
                Random rnd = ThreadLocalRandom.current();

                int nodeIdx = stopIndices.get(idx);

                if (mode == StopMode.FAIL_ONLY || (mode == StopMode.RANDOM && rnd.nextBoolean())) {
                    ZkTestClientCnxnSocketNIO c0 = ZkTestClientCnxnSocketNIO.forNode(grid(nodeIdx));

                    c0.closeSocket(true);

                    nodesFailed.incrementAndGet();
                }
                else {
                    stopGrid(nodeIdx);

                    nodesLeft.incrementAndGet();
                }
            }
            catch (Exception e) {
                e.printStackTrace();

                fail(e.getMessage());
            }
        }, stopIndices.size(), "stop-node");
    }

    /** */
    private int getCoordinatorIndex() {
        UUID crdId = getMbean(grid(0)).getCoordinator();

        Optional<Integer> crdIdx = grid(0).cluster().nodes().stream().filter(n -> n.id().equals(crdId))
            .map(n -> getTestIgniteInstanceIndex((String)n.consistentId())).findAny();

        assertTrue(crdIdx.isPresent());

        return crdIdx.get();
    }

    /** */
    private DiscoverySpiMBean getMbean(IgniteEx grid) {
        ZookeeperDiscoverySpiMBean bean = getMxBean(grid.context().igniteInstanceName(), "SPIs",
            ZookeeperDiscoverySpi.class, ZookeeperDiscoverySpiMBean.class);

        assertNotNull(bean);

        return bean;
    }

    /** */
    private IgniteEx startServersAndClients(int numServers, int numClients) throws Exception {
        startGridsMultiThreaded(1, numServers);
        startClientGridsMultiThreaded(numServers + 1, numClients - 1);

        IgniteEx res = startClientGrid(0);

        waitForTopology(numClients + numServers);

        // Set initial value of counters from MBean.
        nodesLeft.addAndGet(getMbean(res).getNodesLeft());
        nodesFailed.addAndGet(getMbean(res).getNodesFailed());

        return res;
    }

    enum StopMode {
        STOP_ONLY,
        FAIL_ONLY,
        RANDOM
    }
}
