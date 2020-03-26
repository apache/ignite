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

package org.apache.ignite.client;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.configuration.ClientConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Checks compute grid funtionality of thin client.
 */
public class ComputeTaskTest extends GridCommonAbstractTest {
    /** Grids count. */
    private static final int GRIDS_CNT = 4;

    /** Client connector addresses. */
    private String[] clientConnAddresses;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setClientConnectorConfiguration(
            new ClientConnectorConfiguration().setThinClientConfiguration(
                new ThinClientConfiguration().setComputeEnabled(true)));
    }

    /**
     *
     */
    private IgniteClient startClient(String ... addrs) {
        return Ignition.startClient(new ClientConfiguration().setAddresses(addrs));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(GRIDS_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        clientConnAddresses = new String[GRIDS_CNT];

        for (int i = 0; i < GRIDS_CNT; i++)
            clientConnAddresses[i] = "127.0.0.1:" + (ClientConnectorConfiguration.DFLT_PORT + i);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /**
     *
     */
    @Test
    public void testCompute() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            T2<UUID, List<UUID>> val = client.compute().execute(TestTask.class.getName(), null);

            assertEquals(grid(0).localNode().id(), val.get1());
            assertEquals(new HashSet<>(F.nodeIds(grid(0).cluster().nodes())), val.get2());
        }
    }

    /**
     *
     */
    @Test
    public void testComputeTaskCancelation() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses)) {
            ClientFuture<T2<UUID, List<UUID>>> fut = client.compute().executeAsync(TestTask.class.getName(), 1_000L);

            fut.cancel();

            GridTestUtils.assertThrowsAnyCause(null, fut::get, ClientException.class, "cancelled");
        }
    }

    /**
     *
     */
    @Test
    public void testComputeClusterGroup() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            ClientClusterGroup grp = client.cluster().forNodeIds(nodeIds(1, 2));

            T2<UUID, List<UUID>> val = client.compute(grp).execute(TestTask.class.getName(), null);

            assertEquals(grid(0).localNode().id(), val.get1());
            assertEquals(nodeIds(1, 2), val.get2());
        }
    }

    /**
     *
     */
    @Test(expected = ClientException.class)
    public void testComputeUnknownTask() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses)) {
            client.compute().execute("NoSuchTask", null);
        }
    }

    /**
     * Returns set of node IDs by given grid indexes.
     *
     * @param gridIdxs Grid indexes.
     */
    private Set<UUID> nodeIds(int ... gridIdxs) {
        Set<UUID> nodeIds = new HashSet<>();

        for (int i : gridIdxs)
            nodeIds.add(grid(i).localNode().id());

        return nodeIds;
    }

    /**
     * Test compute job which return node id where it was executed.
     */
    private static class TestJob implements ComputeJob {
        /** Ignite. */
        @IgniteInstanceResource
        Ignite ignite;

        /** Sleep time. */
        private final Long sleepTime;

        /**
         * @param sleepTime Sleep time.
         */
        private TestJob(Long sleepTime) {
            this.sleepTime = sleepTime;
        }

        /** {@inheritDoc} */
        @Override public void cancel() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            if (sleepTime != null)
                doSleep(sleepTime);

            return ignite.cluster().localNode().id();
        }
    }

    /**
     * Test compute task which returns node id for routing node and list of node ids for each node was affected.
     */
    @ComputeTaskName("TestTask")
    private static class TestTask implements ComputeTask<Long, T2<UUID, Set<UUID>>> {
        /** Ignite. */
        @IgniteInstanceResource
        Ignite ignite;

        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Long arg) throws IgniteException {
            return subgrid.stream().collect(Collectors.toMap(node -> new TestJob(arg), node -> node));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Nullable @Override public T2<UUID, Set<UUID>> reduce(List<ComputeJobResult> results) throws IgniteException {
            return new T2<>(ignite.cluster().localNode().id(),
                results.stream().map(res -> (UUID)res.getData()).collect(Collectors.toSet()));
        }
    }
}
