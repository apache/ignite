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

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
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

    /** Test task name. */
    private static final String TEST_TASK_NAME = "TestTask";

    /** Client connector addresses. */
    private final String[] clientConnAddresses = new String[GRIDS_CNT];

    /** Node ID for each grid. */
    private final UUID[] nodeIds = new UUID[GRIDS_CNT];

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setClientConnectorConfiguration(
            new ClientConnectorConfiguration().setThinClientConfiguration(
                new ThinClientConfiguration().setComputeEnabled(getTestIgniteInstanceIndex(igniteInstanceName) == 0)));
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

        for (int i = 0; i < GRIDS_CNT; i++) {
            clientConnAddresses[i] = "127.0.0.1:" + (ClientConnectorConfiguration.DFLT_PORT + i);
            nodeIds[i] = grid(i).localNode().id();
        }
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
    public void testExecuteTaskByClassName() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            T2<UUID, List<UUID>> val = client.compute().execute(TestTask.class.getName(), null);

            assertEquals(nodeIds[0], val.get1());
            assertEquals(new HashSet<>(F.nodeIds(grid(0).cluster().nodes())), val.get2());
        }
    }

    /**
     *
     */
    @Test(expected = ClientException.class)
    public void testComputeDisabled() throws Exception {
        // Only grid(0) is allowed to execute thin client compute tasks.
        try (IgniteClient client = startClient(clientConnAddresses[1])) {
            client.compute().execute(TestTask.class.getName(), null);
        }
    }

    /**
     *
     */
    @Test
    public void testExecuteTaskByName() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            // We should deploy class manually to make it visible to server node by task name.
            grid(0).compute().localDeployTask(TestTask.class, TestTask.class.getClassLoader());

            T2<UUID, List<UUID>> val = client.compute().execute(TEST_TASK_NAME, null);

            assertEquals(nodeIds[0], val.get1());
            assertEquals(new HashSet<>(F.nodeIds(grid(0).cluster().nodes())), val.get2());
        }
    }

    /**
     *
     */
    @Test
    public void testExecuteTaskAsync() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            ClientFuture<T2<UUID, List<UUID>>> fut = client.compute().executeAsync(TestLatchTask.class.getName(), null);

            GridTestUtils.assertThrowsAnyCause(
                null,
                () -> fut.get(10L, TimeUnit.MILLISECONDS),
                TimeoutException.class,
                null
            );

            assertFalse(fut.isDone());

            TestLatchTask.latch.countDown();

            T2<UUID, List<UUID>> val = fut.get();

            assertTrue(fut.isDone());
            assertEquals(nodeIds[0], val.get1());
            assertEquals(new HashSet<>(F.nodeIds(grid(0).cluster().nodes())), val.get2());
        }
    }

    /**
     *
     */
    @Test(expected = CancellationException.class)
    public void testTaskCancellation() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            ClientFuture<T2<UUID, List<UUID>>> fut = client.compute().executeAsync(TestTask.class.getName(), 1_000L);

            assertFalse(fut.isCancelled());
            assertFalse(fut.isDone());

            fut.cancel();

            assertTrue(fut.isCancelled());
            assertTrue(fut.isDone());

            fut.get();
        }
    }

    /**
     *
     */
    @Test(expected = ClientException.class)
    public void testTaskWithTimeout() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            client.compute().withTimeout(100L).execute(TestTask.class.getName(), 1_000L);
        }
    }

    /**
     *
     */
    @Test
    public void testTaskWithNoResultCache() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            ClientCompute computeWithCache = client.compute();
            ClientCompute computeWithNoCache = client.compute().withNoResultCache();

            assertTrue(computeWithCache.execute(TestResultCacheTask.class.getName(), null));
            assertFalse(computeWithNoCache.execute(TestResultCacheTask.class.getName(), null));
        }
    }

    /**
     *
     */
    @Test
    public void testTaskWithNoFailover() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            ClientCompute computeWithFailover = client.compute();
            ClientCompute computeWithNoFailover = client.compute().withNoFailover();

            assertTrue(computeWithFailover.execute(TestFailoverTask.class.getName(), null));
            assertFalse(computeWithNoFailover.execute(TestFailoverTask.class.getName(), null));
        }
    }

    /**
     *
     */
    @Test
    public void testExecuteTaskOnClusterGroup() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            ClientClusterGroup grp = client.cluster().forNodeIds(nodeIds(1, 2));

            T2<UUID, List<UUID>> val = client.compute(grp).execute(TestTask.class.getName(), null);

            assertEquals(nodeIds[0], val.get1());
            assertEquals(nodeIds(1, 2), val.get2());
        }
    }


    /**
     *
     */
    @Test(expected = ClientException.class)
    public void testExecuteTaskOnEmptyClusterGroup() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            ClientClusterGroup grp = client.cluster().forNodeIds(Collections.emptyList());

            client.compute(grp).execute(TestTask.class.getName(), null);
        }
    }

    /**
     *
     */
    @Test
    public void testComputeWithMixedModificators() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            ClientClusterGroup grp = client.cluster().forNodeId(nodeIds[1], nodeIds[2]);

            ClientCompute compute = client.compute(grp).withNoFailover().withNoResultCache().withTimeout(200L);

            T2<UUID, List<UUID>> val = client.compute(grp).execute(TestTask.class.getName(), null);

            assertEquals(nodeIds(1, 2), val.get2());

            assertFalse(compute.execute(TestFailoverTask.class.getName(), null));

            assertFalse(compute.execute(TestResultCacheTask.class.getName(), null));

            GridTestUtils.assertThrowsAnyCause(
                null,
                () -> compute.execute(TestTask.class.getName(), 1_000L),
                ClientException.class,
                null
            );
        }
    }

    /**
     *
     */
    @Test(expected = ClientException.class)
    public void testExecuteUnknownTask() throws Exception {
        try (IgniteClient client = startClient(clientConnAddresses[0])) {
            client.compute().execute("NoSuchTask", null);
        }
    }

    // TODO Node failures (execute on one node, reconnect, execute on another, reconnect, assert tasks cancelled
    // resources on client and on server released.

    /**
     * Returns set of node IDs by given grid indexes.
     *
     * @param gridIdxs Grid indexes.
     */
    private Set<UUID> nodeIds(int ... gridIdxs) {
        Set<UUID> res = new HashSet<>();

        for (int i : gridIdxs)
            res.add(nodeIds[i]);

        return res;
    }

    /**
     * Compute job which returns node id where it was executed.
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
     * Compute task which returns node id for routing node and list of node ids for each node was affected.
     */
    @ComputeTaskName(TEST_TASK_NAME)
    private static class TestTask extends ComputeTaskAdapter<Long, T2<UUID, Set<UUID>>> {
        /** Ignite. */
        @IgniteInstanceResource
        Ignite ignite;

        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Long arg) throws IgniteException {
            return subgrid.stream().collect(Collectors.toMap(node -> new TestJob(arg), node -> node));
        }

        /** {@inheritDoc} */
        @Nullable @Override public T2<UUID, Set<UUID>> reduce(List<ComputeJobResult> results) throws IgniteException {
            return new T2<>(ignite.cluster().localNode().id(),
                results.stream().map(res -> (UUID)res.getData()).collect(Collectors.toSet()));
        }
    }

    /**
     * Compute task with latch on routing node.
     */
    @ComputeTaskName("TestLatchTask")
    private static class TestLatchTask extends TestTask {
        /** Latch. */
        private static volatile CountDownLatch latch;

        /** {@inheritDoc} */
        @Override public @Nullable T2<UUID, Set<UUID>> reduce(List<ComputeJobResult> results) throws IgniteException {
            latch = new CountDownLatch(1);

            try {
                latch.await();
            }
            catch (InterruptedException ignore) {
                // No-op.
            }

            return super.reduce(results);
        }
    }

    /**
     * Task to test failover.
     */
    private static class TestFailoverTask implements ComputeTask<Long, Boolean> {
        /** */
        private final AtomicBoolean firstJobProcessed = new AtomicBoolean();

        /** */
        private volatile boolean failedOver;

        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Long arg) throws IgniteException {
            return F.asMap(new TestJob(null), subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            if (firstJobProcessed.compareAndSet(false, true))
                return ComputeJobResultPolicy.FAILOVER;
            else {
                failedOver = true;

                return ComputeJobResultPolicy.WAIT;
            }
        }

        /** {@inheritDoc} */
        @Nullable @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteException {
            return failedOver;
        }
    }

    /**
     * Task to test "no result cache" flag.
     */
    private static class TestResultCacheTask implements ComputeTask<Long, Boolean> {
        /** Is result cached. */
        private volatile boolean cached;

        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable Long arg) throws IgniteException {
            return F.asMap(new TestJob(null), subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            if (!F.isEmpty(rcvd))
                cached = true;

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Boolean reduce(List<ComputeJobResult> results) throws IgniteException {
            return cached;
        }
    }
}
