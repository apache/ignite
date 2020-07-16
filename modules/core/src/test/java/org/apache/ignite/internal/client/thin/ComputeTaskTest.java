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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.client.ClientCache;
import org.apache.ignite.client.ClientClusterGroup;
import org.apache.ignite.client.ClientCompute;
import org.apache.ignite.client.ClientException;
import org.apache.ignite.client.IgniteClient;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskName;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.ThinClientConfiguration;
import org.apache.ignite.internal.processors.odbc.ClientListenerProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.mxbean.ClientProcessorMXBean;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 * Checks compute grid funtionality of thin client.
 */
public class ComputeTaskTest extends AbstractThinClientTest {
    /** Grids count. */
    private static final int GRIDS_CNT = 4;

    /** Active tasks limit. */
    private static final int ACTIVE_TASKS_LIMIT = 50;

    /** Default timeout value. */
    private static final long TIMEOUT = 1_000L;

    /** Test task name. */
    public static final String TEST_TASK_NAME = "TestTask";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setClientConnectorConfiguration(
            new ClientConnectorConfiguration().setThinClientConfiguration(
                new ThinClientConfiguration().setMaxActiveComputeTasksPerConnection(
                    getTestIgniteInstanceIndex(igniteInstanceName) <= 1 ? ACTIVE_TASKS_LIMIT : 0)))
            .setClientMode(getTestIgniteInstanceIndex(igniteInstanceName) == 3);
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
            assertEquals(new HashSet<>(F.nodeIds(grid(0).cluster().forServers().nodes())), val.get2());
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
            assertEquals(new HashSet<>(F.nodeIds(grid(0).cluster().forServers().nodes())), val.get2());
        }
    }

    /**
     *
     */
    @Test
    public void testExecuteTaskAsync() throws Exception {
        try (IgniteClient client = startClient(0)) {
            TestLatchTask.latch = new CountDownLatch(1);

            Future<T2<UUID, Set<UUID>>> fut = client.compute().executeAsync(TestLatchTask.class.getName(), null);

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
            assertEquals(new HashSet<>(F.nodeIds(grid(0).cluster().forServers().nodes())), val.get2());
        }
    }

    /**
     *
     */
    @Test(expected = CancellationException.class)
    public void testTaskCancellation() throws Exception {
        try (IgniteClient client = startClient(0)) {
            Future<T2<UUID, List<UUID>>> fut = client.compute().executeAsync(TestTask.class.getName(), TIMEOUT);

            assertFalse(fut.isCancelled());
            assertFalse(fut.isDone());

            fut.cancel(true);

            assertTrue(GridTestUtils.waitForCondition(
                () -> ((ClientComputeImpl)client.compute()).activeTaskFutures().isEmpty(), TIMEOUT));

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

            // Compute on client node defined explicitly.
            grp = client.cluster().forNodeIds(nodeIds(3));

            val = client.compute(grp).execute(TestTask.class.getName(), null);

            assertEquals(nodeId(0), val.get1());
            assertEquals(nodeIds(3), val.get2());

            // Compute on all nodes (clients + servers).
            grp = client.cluster();

            val = client.compute(grp).execute(TestTask.class.getName(), null);

            assertEquals(nodeId(0), val.get1());
            assertEquals(new HashSet<>(F.nodeIds(grid(0).cluster().nodes())), val.get2());
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

            Future<Object> fut1 = compute.executeAsync(TestTask.class.getName(), TIMEOUT);

            dropAllThinClientConnections();

            Future<Object> fut2 = compute.executeAsync(TestTask.class.getName(), TIMEOUT);

            dropAllThinClientConnections();

            TestLatchTask.latch = new CountDownLatch(1);

            Future<Object> fut3 = compute.executeAsync(TestLatchTask.class.getName(), null);

            assertEquals(1, compute.activeTaskFutures().size());

            compute.execute(TestTask.class.getName(), null);

            assertTrue(GridTestUtils.waitForCondition(() -> compute.activeTaskFutures().size() == 1, TIMEOUT));

            assertTrue(fut1.isDone());

            GridTestUtils.assertThrowsAnyCause(null, fut1::get, ClientException.class, "closed");

            assertTrue(fut2.isDone());

            GridTestUtils.assertThrowsAnyCause(null, fut2::get, ClientException.class, "closed");

            assertFalse(fut3.isDone());

            TestLatchTask.latch.countDown();

            fut3.get(TIMEOUT, TimeUnit.MILLISECONDS);

            assertTrue(GridTestUtils.waitForCondition(() -> compute.activeTaskFutures().isEmpty(), TIMEOUT));
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

            CountDownLatch latch1 = TestLatchTask.latch = new CountDownLatch(1);

            Future<T2<UUID, Set<UUID>>> fut1 = compute1.executeAsync(TestLatchTask.class.getName(), null);

            CountDownLatch latch2 = TestLatchTask.latch = new CountDownLatch(1);

            Future<T2<UUID, Set<UUID>>> fut2 = compute2.executeAsync(TestLatchTask.class.getName(), null);

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

            CountDownLatch latch1 = TestLatchTask.latch = new CountDownLatch(1);

            Future<T2<UUID, Set<UUID>>> fut1 = compute1.executeAsync(TestLatchTask.class.getName(), null);

            CountDownLatch latch2 = TestLatchTask.latch = new CountDownLatch(1);

            Future<T2<UUID, Set<UUID>>> fut2 = compute2.executeAsync(TestLatchTask.class.getName(), null);

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
    public void testActiveTasksLimit() throws Exception {
        try (IgniteClient client = startClient(0)) {
            ClientCompute compute = client.compute(client.cluster().forNodeId(nodeId(1)));

            CountDownLatch latch = TestLatchTask.latch = new CountDownLatch(1);

            List<Future<T2<UUID, Set<UUID>>>> futs = new ArrayList<>(ACTIVE_TASKS_LIMIT);

            for (int i = 0; i < ACTIVE_TASKS_LIMIT; i++)
                futs.add(compute.executeAsync(TestLatchTask.class.getName(), null));

            // Check that we can't start more tasks.
            GridTestUtils.assertThrowsAnyCause(
                null,
                () -> compute.executeAsync(TestLatchTask.class.getName(), null),
                ClientException.class,
                "limit"
            );

            // Check that cancelled tasks restore limit.
            for (int i = 0; i < ACTIVE_TASKS_LIMIT / 2; i++)
                futs.get(i).cancel(true);

            latch.countDown();

            // Check that successfully complited tasks restore limit.
            for (int i = ACTIVE_TASKS_LIMIT / 2; i < ACTIVE_TASKS_LIMIT; i++)
                assertEquals(nodeIds(1), futs.get(i).get(TIMEOUT, TimeUnit.MILLISECONDS).get2());

            // Check that complited with error tasks restore limit.
            GridTestUtils.assertThrowsAnyCause(
                null,
                () -> compute.execute("NoSuchTask", null),
                ClientException.class,
                null
            );

            // Check that we can start up to ACTIVE_TASKS_LIMIT new active tasks again.
            latch = TestLatchTask.latch = new CountDownLatch(1);

            futs = new ArrayList<>(ACTIVE_TASKS_LIMIT);

            for (int i = 0; i < ACTIVE_TASKS_LIMIT; i++)
                futs.add(compute.executeAsync(TestLatchTask.class.getName(), null));

            latch.countDown();

            for (Future<T2<UUID, Set<UUID>>> fut : futs)
                assertEquals(nodeIds(1), fut.get(TIMEOUT, TimeUnit.MILLISECONDS).get2());
        }
    }

    /**
     *
     */
    @Test
    public void testExecuteTaskConcurrentLoad() throws Exception {
        try (IgniteClient client = startClient(0)) {
            int threadsCnt = 20;
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

                            Future<T2<UUID, Set<UUID>>> fut = compute.executeAsync(TestTask.class.getName(), null);

                            boolean cancelled = (i % 3 == 0) && fut.cancel(true);

                            assertEquals((Integer)i, cache.get(threadIdx));

                            if (cancelled)
                                assertTrue(fut.isCancelled());
                            else
                                assertEquals(nodeIds(nodeIdx), fut.get().get2());
                        }
                    }
                    catch (ExecutionException e) {
                        log.error("Task failed: ", e);

                        fail("Task failed");
                    }
                    catch (InterruptedException | BrokenBarrierException ignore) {
                        // No-op.
                    }

                }, threadsCnt, "run-task-async");

            assertTrue(GridTestUtils.waitForCondition(
                () -> ((ClientComputeImpl)client.compute()).activeTaskFutures().isEmpty(), TIMEOUT));
        }
    }

    /**
     * Returns set of node IDs by given grid indexes.
     *
     * @param gridIdxs Grid indexes.
     */
    private Set<UUID> nodeIds(int... gridIdxs) {
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
     * Compute task with latch on routing node.
     */
    @ComputeTaskName("TestLatchTask")
    private static class TestLatchTask extends TestTask {
        /** Global latch. */
        private static volatile CountDownLatch latch;

        /** Local latch. */
        private final CountDownLatch locLatch;

        /**
         * Default constructor.
         */
        public TestLatchTask() {
            locLatch = latch;
        }

        /** {@inheritDoc} */
        @Override public @Nullable T2<UUID, Set<UUID>> reduce(List<ComputeJobResult> results) throws IgniteException {
            try {
                if (locLatch != null)
                    locLatch.await(TIMEOUT, TimeUnit.MILLISECONDS);
            }
            catch (InterruptedException ignore) {
                // No-op.
            }

            return super.reduce(results);
        }
    }
}
