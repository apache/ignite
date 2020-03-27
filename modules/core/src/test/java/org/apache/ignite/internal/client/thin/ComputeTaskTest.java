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

package org.apache.ignite.internal.client.thin;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.Ignition;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientCompute;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.ClientFuture;
import org.apache.ignite.client.IgniteClient;
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
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
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

    /** Default timeout value. */
    private static final long TIMEOUT = 1_000L;

    /** Test task name. */
    private static final String TEST_TASK_NAME = "TestTask";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setClientConnectorConfiguration(
            new ClientConnectorConfiguration().setThinClientConfiguration(
                new ThinClientConfiguration().setComputeEnabled(getTestIgniteInstanceIndex(igniteInstanceName) <= 1)));
    }

    /**
     *
     */
    private IgniteClient startClient(int ... gridIdxs) {
        String[] addrs = new String[gridIdxs.length];

        for (int i = 0; i < gridIdxs.length; i++)
            addrs[i] = "127.0.0.1:" + (ClientConnectorConfiguration.DFLT_PORT + gridIdxs[i]);

        return Ignition.startClient(new ClientConfiguration().setAddresses(addrs));
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(GRIDS_CNT);
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
        try (IgniteClient client = startClient(0)) {
            T2<UUID, Set<UUID>> val = client.compute().execute(TestTask.class.getName(), null);

            assertEquals(nodeId(0), val.get1());
            assertEquals(new HashSet<>(F.nodeIds(grid(0).cluster().nodes())), val.get2());
        }
    }

    /**
     *
     */
    @Test(expected = ClientException.class)
    public void testComputeDisabled() throws Exception {
        // Only grid(0) and grid(1) is allowed to execute thin client compute tasks.
        try (IgniteClient client = startClient(2)) {
            client.compute().execute(TestTask.class.getName(), null);
        }
    }

    /**
     *
     */
    @Test
    public void testExecuteTaskByName() throws Exception {
        try (IgniteClient client = startClient(0)) {
            // We should deploy class manually to make it visible to server node by task name.
            grid(0).compute().localDeployTask(TestTask.class, TestTask.class.getClassLoader());

            T2<UUID, Set<UUID>> val = client.compute().execute(TEST_TASK_NAME, null);

            assertEquals(nodeId(0), val.get1());
            assertEquals(new HashSet<>(F.nodeIds(grid(0).cluster().nodes())), val.get2());
        }
    }

    /**
     *
     */
    @Test
    public void testExecuteTaskAsync() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientFuture<T2<UUID, Set<UUID>>> fut = client.compute().executeAsync(TestLatchTask.class.getName(), null);

            GridTestUtils.assertThrowsAnyCause(
                null,
                () -> fut.get(10L, TimeUnit.MILLISECONDS),
                TimeoutException.class,
                null
            );

            assertFalse(fut.isDone());

            TestLatchTask.latch.countDown();

            T2<UUID, Set<UUID>> val = fut.get();

            assertTrue(fut.isDone());
            assertEquals(nodeId(0), val.get1());
            assertEquals(new HashSet<>(F.nodeIds(grid(0).cluster().nodes())), val.get2());
        }
    }

    /**
     *
     */
    @Test(expected = CancellationException.class)
    public void testTaskCancellation() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientFuture<T2<UUID, List<UUID>>> fut = client.compute().executeAsync(TestTask.class.getName(), TIMEOUT);

            assertFalse(fut.isCancelled());
            assertFalse(fut.isDone());

            fut.cancel();

            assertTrue(((ClientComputeImpl)client.compute()).activeTaskFutures().isEmpty());

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
        try (IgniteClient client = startClient(0)) {
            client.compute().withTimeout(TIMEOUT / 5).execute(TestTask.class.getName(), TIMEOUT);
        }
    }

    /**
     *
     */
    @Test
    public void testTaskWithNoResultCache() throws Exception {
        try (IgniteClient client = startClient(0)) {
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
        try (IgniteClient client = startClient(0)) {
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
        try (IgniteClient client = startClient(0)) {
            ClientClusterGroup grp = client.cluster().forNodeIds(nodeIds(1, 2));

            T2<UUID, List<UUID>> val = client.compute(grp).execute(TestTask.class.getName(), null);

            assertEquals(nodeId(0), val.get1());
            assertEquals(nodeIds(1, 2), val.get2());
        }
    }


    /**
     *
     */
    @Test(expected = ClientException.class)
    public void testExecuteTaskOnEmptyClusterGroup() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientClusterGroup grp = client.cluster().forNodeIds(Collections.emptyList());

            client.compute(grp).execute(TestTask.class.getName(), null);
        }
    }

    /**
     *
     */
    @Test
    public void testComputeWithMixedModificators() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientClusterGroup grp = client.cluster().forNodeId(nodeId(1), nodeId(2));

            ClientCompute compute = client.compute(grp).withNoFailover().withNoResultCache().withTimeout(TIMEOUT / 5);

            T2<UUID, List<UUID>> val = client.compute(grp).execute(TestTask.class.getName(), null);

            assertEquals(nodeIds(1, 2), val.get2());

            assertFalse(compute.execute(TestFailoverTask.class.getName(), null));

            assertFalse(compute.execute(TestResultCacheTask.class.getName(), null));

            GridTestUtils.assertThrowsAnyCause(
                null,
                () -> compute.execute(TestTask.class.getName(), TIMEOUT),
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
        try (IgniteClient client = startClient(0)) {
            client.compute().execute("NoSuchTask", null);
        }
    }

    /**
     *
     */
    @Test
    public void testExecuteTaskConnectionLost() throws Exception {
        try (IgniteClient client = startClient(0, 1)) {
            ClientComputeImpl compute = (ClientComputeImpl)client.compute();

            ClientFuture<Object> fut1  = compute.executeAsync(TestTask.class.getName(), TIMEOUT);

            dropAllThinClientConnections();

            ClientFuture<Object> fut2  = compute.executeAsync(TestTask.class.getName(), TIMEOUT);

            dropAllThinClientConnections();

            ClientFuture<Object> fut3  = compute.executeAsync(TestLatchTask.class.getName(), null);

            assertEquals(1, compute.activeTaskFutures().size());

            compute.execute(TestTask.class.getName(), null);

            assertEquals(1, compute.activeTaskFutures().size());

            assertTrue(fut1.isDone());

            GridTestUtils.assertThrowsAnyCause(null, fut1::get, ClientException.class, "closed");

            assertTrue(fut2.isDone());

            GridTestUtils.assertThrowsAnyCause(null, fut2::get, ClientException.class, "closed");

            assertFalse(fut3.isDone());

            TestLatchTask.latch.countDown();

            fut3.get(TIMEOUT, TimeUnit.MILLISECONDS);

            assertTrue(compute.activeTaskFutures().isEmpty());
        }
    }

    /**
     *
     */
    @Test
    public void testExecuteTwoTasksMisorderedResults() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCompute compute1 = client.compute(client.cluster().forNodeId(nodeId(1)));
            ClientCompute compute2 = client.compute(client.cluster().forNodeId(nodeId(2)));

            ClientFuture<T2<UUID, Set<UUID>>> fut1 = compute1.executeAsync(TestLatchTask.class.getName(), null);

            CountDownLatch latch1 = TestLatchTask.latch;

            ClientFuture<T2<UUID, Set<UUID>>> fut2 = compute2.executeAsync(TestLatchTask.class.getName(), null);

            CountDownLatch latch2 = TestLatchTask.latch;

            latch2.countDown();

            assertEquals(nodeIds(2), fut2.get().get2());

            assertFalse(fut1.isDone());

            latch1.countDown();

            assertEquals(nodeIds(1), fut1.get().get2());
        }
    }

    /**
     *
     */
    @Test
    public void testExecuteTaskTwoClientsToOneNode() throws Exception {
        try (IgniteClient client1 = startClient(0); IgniteClient client2 = startClient(0)) {
            ClientCompute compute1 = client1.compute(client1.cluster().forNodeId(nodeId(1)));
            ClientCompute compute2 = client2.compute(client2.cluster().forNodeId(nodeId(2)));

            ClientFuture<T2<UUID, Set<UUID>>> fut1 = compute1.executeAsync(TestLatchTask.class.getName(), null);

            CountDownLatch latch1 = TestLatchTask.latch;

            ClientFuture<T2<UUID, Set<UUID>>> fut2 = compute2.executeAsync(TestLatchTask.class.getName(), null);

            CountDownLatch latch2 = TestLatchTask.latch;

            latch2.countDown();

            assertEquals(nodeIds(2), fut2.get().get2());

            assertFalse(fut1.isDone());

            latch1.countDown();

            assertEquals(nodeIds(1), fut1.get().get2());
        }
    }

    /**
     *
     */
    @Test
    public void testExecuteTaskConcurrentLoad() throws Exception {
        try (IgniteClient client = startClient(0)) {
            int threadsCnt = 5;
            int iterations = 20;

            ClientCache<Integer, Integer> cache = client.getOrCreateCache(DEFAULT_CACHE_NAME);

            AtomicInteger threadIdxs = new AtomicInteger();

            CyclicBarrier barrier = new CyclicBarrier(threadsCnt);

            GridTestUtils.runMultiThreaded(
                () -> {
                    int threadIdx = threadIdxs.incrementAndGet();
                    
                    Random rnd = new Random();

                    try {
                        barrier.await();

                        for (int i = 0; i < iterations; i++) {
                            int nodeIdx = rnd.nextInt(GRIDS_CNT);
                            
                            cache.put(threadIdx, i);
                            
                            ClientCompute compute = client.compute(client.cluster().forNodeId(nodeId(nodeIdx)));
                            
                            ClientFuture<T2<UUID, Set<UUID>>> fut = compute.executeAsync(TestTask.class.getName(), null);
                            
                            assertEquals((Integer)i, cache.get(threadIdx));

                            assertEquals(nodeIds(nodeIdx), fut.get().get2());
                        }
                    }
                    catch (InterruptedException | BrokenBarrierException ignore) {
                        // No-op.
                    }
                    
                }, threadsCnt, "run-task-async");
        }
    }

    /**
     * Returns set of node IDs by given grid indexes.
     *
     * @param gridIdxs Grid indexes.
     */
    private Set<UUID> nodeIds(int ... gridIdxs) {
        Set<UUID> res = new HashSet<>();

        for (int i : gridIdxs)
            res.add(nodeId(i));

        return res;
    }

    /**
     *
     */
    private void dropAllThinClientConnections() {
        for (Ignite ignite : G.allGrids()) {
            ClientProcessorMXBean mxBean = getMxBean(ignite.name(), "Clients",
                ClientListenerProcessor.class, ClientProcessorMXBean.class);

            mxBean.dropAllConnections();
        }
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
        /** Global latch. */
        private static volatile CountDownLatch latch;

        /** Local latch. */
        private final CountDownLatch locLatch = new CountDownLatch(1);

        /**
         * Default constructor.
         */
        public TestLatchTask() {
            latch = locLatch;
        }

        /** {@inheritDoc} */
        @Override public @Nullable T2<UUID, Set<UUID>> reduce(List<ComputeJobResult> results) throws IgniteException {
            try {
                locLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignore) {
                // No-op.
            }

            latch = null;

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
