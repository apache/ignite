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

package org.apache.ignite.internal.metric;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.systemview.view.ComputeJobView;
import org.apache.ignite.spi.systemview.view.ComputeTaskView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOBS_VIEW;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;
import static org.apache.ignite.testframework.GridTestUtils.runAsync;

/** Tests for {@link SystemView} for compute tasks. */
public class SystemViewComputeTaskTest extends SystemViewAbstractTest {
    /** */
    private static CountDownLatch jobStartedLatch;

    /** */
    private static CountDownLatch releaseJobLatch;

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#broadcastAsync(IgniteRunnable)} call. */
    @Test
    public void testComputeBroadcast() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(6);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().systemView().view(TASKS_VIEW);

            for (int i = 0; i < 5; i++) {
                g1.compute().broadcastAsync(() -> {
                    try {
                        barrier.await();
                        barrier.await();
                    }
                    catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            barrier.await();

            assertEquals(5, tasks.size());

            ComputeTaskView t = tasks.iterator().next();

            assertFalse(t.internal());
            assertNull(t.affinityCacheName());
            assertEquals(-1, t.affinityPartitionId());
            assertTrue(t.taskClassName().startsWith(getClass().getName()));
            assertTrue(t.taskName().startsWith(getClass().getName()));
            assertEquals(g1.localNode().id(), t.taskNodeId());
            assertEquals("0", t.userVersion());

            barrier.await();
        }
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#runAsync(IgniteRunnable)} call. */
    @Test
    public void testComputeRunnable() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().systemView().view(TASKS_VIEW);

            g1.compute().runAsync(() -> {
                try {
                    barrier.await();
                    barrier.await();
                }
                catch (InterruptedException | BrokenBarrierException e) {
                    throw new RuntimeException(e);
                }
            });

            barrier.await();

            assertEquals(1, tasks.size());

            ComputeTaskView t = tasks.iterator().next();

            assertFalse(t.internal());
            assertNull(t.affinityCacheName());
            assertEquals(-1, t.affinityPartitionId());
            assertTrue(t.taskClassName().startsWith(getClass().getName()));
            assertTrue(t.taskName().startsWith(getClass().getName()));
            assertEquals(g1.localNode().id(), t.taskNodeId());
            assertEquals("0", t.userVersion());

            barrier.await();
        }
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#apply(IgniteClosure, Object)} call. */
    @Test
    public void testComputeApply() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().systemView().view(TASKS_VIEW);

            GridTestUtils.runAsync(() -> {
                g1.compute().apply(x -> {
                    try {
                        barrier.await();
                        barrier.await();
                    }
                    catch (InterruptedException | BrokenBarrierException e) {
                        throw new RuntimeException(e);
                    }

                    return 0;
                }, 1);
            });

            barrier.await();

            assertEquals(1, tasks.size());

            ComputeTaskView t = tasks.iterator().next();

            assertFalse(t.internal());
            assertNull(t.affinityCacheName());
            assertEquals(-1, t.affinityPartitionId());
            assertTrue(t.taskClassName().startsWith(getClass().getName()));
            assertTrue(t.taskName().startsWith(getClass().getName()));
            assertEquals(g1.localNode().id(), t.taskNodeId());
            assertEquals("0", t.userVersion());

            barrier.await();
        }
    }

    /**
     * Tests work of {@link SystemView} for compute grid
     * {@link IgniteCompute#affinityCallAsync(String, Object, IgniteCallable)} call.
     */
    @Test
    public void testComputeAffinityCall() throws Exception {
        CyclicBarrier barrier = new CyclicBarrier(2);

        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ComputeTaskView> tasks = g1.context().systemView().view(TASKS_VIEW);

            IgniteCache<Integer, Integer> cache = g1.createCache("test-cache");

            cache.put(1, 1);

            g1.compute().affinityCallAsync("test-cache", 1, () -> {
                try {
                    barrier.await();
                    barrier.await();
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                return 0;
            });

            barrier.await();

            assertEquals(1, tasks.size());

            ComputeTaskView t = tasks.iterator().next();

            assertFalse(t.internal());
            assertEquals("test-cache", t.affinityCacheName());
            assertEquals(1, t.affinityPartitionId());
            assertTrue(t.taskClassName().startsWith(getClass().getName()));
            assertTrue(t.taskName().startsWith(getClass().getName()));
            assertEquals(g1.localNode().id(), t.taskNodeId());
            assertEquals("0", t.userVersion());

            barrier.await();
        }
    }

    /** */
    @Test
    public void testComputeTask() throws Exception {
        doTestComputeTask(false);
    }

    /** */
    @Test
    public void testInternalComputeTask() throws Exception {
        doTestComputeTask(true);
    }

    /** */
    private void doTestComputeTask(boolean internal) throws Exception {
        int gridCnt = 3;

        IgniteEx g1 = startGrids(gridCnt);

        try {
            IgniteCache<Integer, Integer> cache = g1.createCache("test-cache");

            cache.put(1, 1);

            for (int i = 0; i < gridCnt; i++) {
                IgniteEx grid = grid(i);

                SystemView<ComputeTaskView> tasks = grid.context().systemView().view(TASKS_VIEW);

                jobStartedLatch = new CountDownLatch(3);
                releaseJobLatch = new CountDownLatch(1);

                IgniteInternalFuture<Object> fut
                    = runAsync(() -> grid.compute().execute(internal ? new InternalTask() : new UserTask(), 1));

                assertTrue(jobStartedLatch.await(30_000, TimeUnit.MILLISECONDS));

                try {
                    assertEquals(1, tasks.size());

                    ComputeTaskView t = tasks.iterator().next();

                    assertEquals("Expecting to see " + (internal ? "internal" : "user") + " task", internal, t.internal());
                    assertNull(t.affinityCacheName());
                    assertEquals(-1, t.affinityPartitionId());
                    assertTrue(t.taskClassName().startsWith(getClass().getName()));
                    assertTrue(t.taskName().startsWith(getClass().getName()));
                    assertEquals(grid.localNode().id(), t.taskNodeId());
                    assertEquals("0", t.userVersion());

                    checkJobs(gridCnt, internal, t.sessionId());
                }
                finally {
                    releaseJobLatch.countDown();
                }

                fut.get(getTestTimeout(), TimeUnit.MILLISECONDS);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private void checkJobs(int gridCnt, boolean internal, IgniteUuid sesId) {
        for (int i = 0; i < gridCnt; i++) {
            SystemView<ComputeJobView> jobs = grid(i).context().systemView().view(JOBS_VIEW);

            assertTrue("Expecting to see " + (internal ? "internal" : "user") + " job", jobs.size() > 0);

            ComputeJobView job = jobs.iterator().next();

            assertEquals(sesId, job.sessionId());
            assertEquals("Expecting to see " + (internal ? "internal" : "user") + " job", internal, job.isInternal());
        }

        releaseJobLatch.countDown();
    }

    /** */
    private static class UserTask implements ComputeTask<Object, Object> {
        /** {@inheritDoc} */
        @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(
            List<ClusterNode> subgrid,
            @Nullable Object arg
        ) throws IgniteException {
            return subgrid.stream().collect(Collectors.toMap(k -> new UserJob(), Function.identity()));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws IgniteException {
            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
            return 1;
        }

        /** */
        private static class UserJob implements ComputeJob {
            /** {@inheritDoc} */
            @Override public void cancel() {
                // No-op.
            }

            /** {@inheritDoc} */
            @Override public Object execute() throws IgniteException {
                jobStartedLatch.countDown();

                try {
                    assertTrue(releaseJobLatch.await(30_000, TimeUnit.MILLISECONDS));
                }
                catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }

                return 1;
            }
        }
    }

    /** */
    @GridInternal
    public static class InternalTask extends UserTask {
        // No-op.
    }
}
