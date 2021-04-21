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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeoutException;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.IgniteSpiAdapter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiMultipleInstancesSupport;
import org.apache.ignite.spi.collision.CollisionContext;
import org.apache.ignite.spi.collision.CollisionExternalListener;
import org.apache.ignite.spi.collision.CollisionJobContext;
import org.apache.ignite.spi.collision.CollisionSpi;
import org.apache.ignite.spi.systemview.view.ComputeJobView;
import org.apache.ignite.spi.systemview.view.ComputeJobView.ComputeJobState;
import org.apache.ignite.spi.systemview.view.ComputeTaskView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOBS_VIEW;
import static org.apache.ignite.internal.processors.task.GridTaskProcessor.TASKS_VIEW;
import static org.apache.ignite.spi.systemview.view.ComputeJobView.ComputeJobState.ACTIVE;
import static org.apache.ignite.spi.systemview.view.ComputeJobView.ComputeJobState.CANCELED;
import static org.apache.ignite.spi.systemview.view.ComputeJobView.ComputeJobState.PASSIVE;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests for compute task {@link SystemView}. */
public class SystemViewComputeJobTest extends GridCommonAbstractTest {
    /** */
    public static final long TIMEOUT = 10_000L;

    /** */
    private static CyclicBarrier barrier;

    /** */
    private static IgniteEx server;

    /** */
    private static IgniteEx client;

    /** */
    private static IgniteCache<Integer, Integer> cache;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setCollisionSpi(new CancelCollisionSpi());

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        server = startGrid(0);
        client = startClientGrid(1);

        cache = server.createCache("test-cache");

        cache.put(1, 1);
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#broadcastAsync(IgniteRunnable)} call. */
    @Test
    public void testComputeBroadcast() throws Exception {
        barrier = new CyclicBarrier(6);

        SystemView<ComputeJobView> jobs = server.context().systemView().view(JOBS_VIEW);

        for (int i = 0; i < 5; i++) {
            client.compute().broadcastAsync(() -> {
                try {
                    barrier.await(TIMEOUT, MILLISECONDS);
                    barrier.await(TIMEOUT, MILLISECONDS);
                }
                catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        barrier.await(TIMEOUT, MILLISECONDS);

        assertEquals(5, jobs.size());

        for (ComputeJobView job : jobs)
            checkJobView(job);

        barrier.await(TIMEOUT, MILLISECONDS);

        boolean res = waitForCondition(() -> jobs.size() == 0, TIMEOUT);
        assertTrue(res);
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#runAsync(IgniteRunnable)} call. */
    @Test
    public void testComputeRunnable() throws Exception {
        barrier = new CyclicBarrier(2);

        SystemView<ComputeJobView> jobs = server.context().systemView().view(JOBS_VIEW);

        client.compute().runAsync(() -> {
            try {
                barrier.await(TIMEOUT, MILLISECONDS);
                barrier.await(TIMEOUT, MILLISECONDS);
            }
            catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                throw new RuntimeException(e);
            }
        });

        barrier.await(TIMEOUT, MILLISECONDS);

        assertEquals(1, jobs.size());

        for (ComputeJobView job : jobs)
            checkJobView(job);

        barrier.await(TIMEOUT, MILLISECONDS);

        boolean res = waitForCondition(() -> jobs.size() == 0, TIMEOUT);
        assertTrue(res);
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#apply(IgniteClosure, Object)} call. */
    @Test
    public void testComputeApply() throws Exception {
        barrier = new CyclicBarrier(2);

        SystemView<ComputeJobView> jobs = server.context().systemView().view(JOBS_VIEW);

        GridTestUtils.runAsync(() -> {
            client.compute().apply(x -> {
                try {
                    barrier.await(TIMEOUT, MILLISECONDS);
                    barrier.await(TIMEOUT, MILLISECONDS);
                }
                catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new RuntimeException(e);
                }

                return 0;
            }, 1);
        });

        barrier.await(TIMEOUT, MILLISECONDS);

        assertEquals(1, jobs.size());

        ComputeJobView t = jobs.iterator().next();

        checkJobView(t);

        barrier.await(TIMEOUT, MILLISECONDS);

        boolean res = waitForCondition(() -> jobs.size() == 0, TIMEOUT);
        assertTrue(res);
    }

    /**
     * Tests work of {@link SystemView} for compute grid
     * {@link IgniteCompute#affinityCallAsync(String, Object, IgniteCallable)} call.
     */
    @Test
    public void testComputeAffinityCall() throws Exception {
        barrier = new CyclicBarrier(2);

        SystemView<ComputeJobView> jobs = server.context().systemView().view(JOBS_VIEW);

        client.compute().affinityCallAsync("test-cache", 1, () -> {
            try {
                barrier.await(TIMEOUT, MILLISECONDS);
                barrier.await(TIMEOUT, MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return 0;
        });

        barrier.await(TIMEOUT, MILLISECONDS);

        assertEquals(1, jobs.size());

        ComputeJobView t = jobs.iterator().next();

        assertFalse(t.isInternal());
        assertEquals(String.valueOf(CU.cacheId("test-cache")), t.affinityCacheIds());
        assertEquals(1, t.affinityPartitionId());
        assertTrue(t.taskClassName().startsWith(getClass().getName()));
        assertTrue(t.taskName().startsWith(getClass().getName()));
        assertEquals(client.localNode().id(), t.originNodeId());

        barrier.await(TIMEOUT, MILLISECONDS);

        boolean res = waitForCondition(() -> jobs.size() == 0, TIMEOUT);
        assertTrue(res);
    }

    /** */
    @Test
    public void testComputeTask() throws Exception {
        barrier = new CyclicBarrier(2);

        SystemView<ComputeJobView> jobs = server.context().systemView().view(JOBS_VIEW);

        client.compute().executeAsync(new ComputeTask<Object, Object>() {
            @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
                @Nullable Object arg) throws IgniteException {
                return Collections.singletonMap(new ComputeJob() {
                    @Override public void cancel() {
                        // No-op.
                    }

                    @Override public Object execute() throws IgniteException {
                        try {
                            barrier.await(TIMEOUT, MILLISECONDS);
                            barrier.await(TIMEOUT, MILLISECONDS);
                        }
                        catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                            throw new RuntimeException(e);
                        }

                        return 1;
                    }
                }, subgrid.get(0));
            }

            @Override public ComputeJobResultPolicy result(ComputeJobResult res,
                List<ComputeJobResult> rcvd) throws IgniteException {

                return null;
            }

            @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
                return 1;
            }
        }, 1);

        barrier.await(TIMEOUT, MILLISECONDS);

        assertEquals(1, jobs.size());

        ComputeJobView t = jobs.iterator().next();

        checkJobView(t);

        barrier.await(TIMEOUT, MILLISECONDS);

        boolean res = waitForCondition(() -> jobs.size() == 0, TIMEOUT);
        assertTrue(res);
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#runAsync(IgniteRunnable)} call. */
    @Test
    public void testComputeRunnableJobAndTask() throws Exception {
        try (IgniteEx server2 = startGrid(2)) {
            barrier = new CyclicBarrier(3);

            SystemView<ComputeJobView> jobs1 = server.context().systemView().view(JOBS_VIEW);
            SystemView<ComputeJobView> jobs2 = server2.context().systemView().view(JOBS_VIEW);
            SystemView<ComputeTaskView> tasks = client.context().systemView().view(TASKS_VIEW);

            client.compute().broadcastAsync(() -> {
                try {
                    barrier.await(TIMEOUT, MILLISECONDS);
                    barrier.await(TIMEOUT, MILLISECONDS);
                }
                catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            });

            barrier.await(TIMEOUT, MILLISECONDS);

            assertEquals(1, tasks.size());
            assertEquals(1, jobs1.size());
            assertEquals(1, jobs2.size());

            ComputeTaskView task = tasks.iterator().next();

            checkTaskAndJob(task, jobs1.iterator().next());
            checkTaskAndJob(task, jobs2.iterator().next());

            barrier.await(TIMEOUT, MILLISECONDS);

            boolean res = waitForCondition(() -> jobs1.size() == 0, TIMEOUT);
            assertTrue(res);

            res = waitForCondition(() -> jobs2.size() == 0, TIMEOUT);
            assertTrue(res);

            res = waitForCondition(() -> tasks.size() == 0, TIMEOUT);
            assertTrue(res);
        }
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#runAsync(IgniteRunnable)} call. */
    @Test
    public void testComputeAffinityCallJobAndTask() throws Exception {
        barrier = new CyclicBarrier(2);

        SystemView<ComputeJobView> jobs = server.context().systemView().view(JOBS_VIEW);
        SystemView<ComputeTaskView> tasks = client.context().systemView().view(TASKS_VIEW);

        client.compute().affinityCallAsync("test-cache", 1, () -> {
            try {
                barrier.await(TIMEOUT, MILLISECONDS);
                barrier.await(TIMEOUT, MILLISECONDS);
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            return 0;
        });

        barrier.await(TIMEOUT, MILLISECONDS);

        assertEquals(1, tasks.size());
        assertEquals(1, jobs.size());

        checkTaskAndJob(tasks.iterator().next(), jobs.iterator().next());

        barrier.await(TIMEOUT, MILLISECONDS);

        boolean res = waitForCondition(() -> jobs.size() == 0, TIMEOUT);
        assertTrue(res);

        res = waitForCondition(() -> tasks.size() == 0, TIMEOUT);
        assertTrue(res);
    }

    /** */
    @Test
    public void testCancelComputeTask() throws Exception {
        barrier = new CyclicBarrier(2);

        SystemView<ComputeJobView> jobs = server.context().systemView().view(JOBS_VIEW);

        client.compute().withName("cancel-task").executeAsync(new ComputeTask<Object, Object>() {
            @Override public @NotNull Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
                @Nullable Object arg) throws IgniteException {
                return Collections.singletonMap(new ComputeJob() {
                    @Override public void cancel() {
                        // No-op.
                    }

                    @Override public Object execute() throws IgniteException {
                        try {
                            Thread.sleep(60_000);
                        }
                        catch (InterruptedException e) {
                            throw new IgniteException(e);
                        }

                        return null;
                    }
                }, subgrid.get(0));
            }

            @Override public ComputeJobResultPolicy result(ComputeJobResult res,
                List<ComputeJobResult> rcvd) throws IgniteException {

                return null;
            }

            @Nullable @Override public Object reduce(List<ComputeJobResult> results) throws IgniteException {
                return 1;
            }
        }, 1);

        barrier.await(TIMEOUT, MILLISECONDS);

        assertEquals(1, jobs.size());

        checkJobView(jobs.iterator().next(), "cancel-task", PASSIVE);

        barrier.await(TIMEOUT, MILLISECONDS);
        barrier.await(TIMEOUT, MILLISECONDS);

        assertEquals(1, jobs.size());

        checkJobView(jobs.iterator().next(), "cancel-task", CANCELED);

        barrier.await(TIMEOUT, MILLISECONDS);

        boolean res = waitForCondition(() -> jobs.size() == 0, TIMEOUT);
        assertTrue(res);
    }

    /**
     * Check fields for local {@link ComputeTaskView} and remote {@link ComputeJobView} info of the same computation.
     */
    private void checkTaskAndJob(ComputeTaskView task, ComputeJobView job) {
        assertNotSame(task.id(), job.id());
        assertEquals(task.sessionId(), job.sessionId());
        assertEquals(task.taskNodeId(), job.originNodeId());
        assertEquals(task.taskName(), job.taskName());
        assertEquals(task.taskClassName(), job.taskClassName());

        if (task.affinityCacheName() != null)
            assertEquals((Integer)CU.cacheId(task.affinityCacheName()), Integer.valueOf(job.affinityCacheIds()));
        else
            assertNull(job.affinityCacheIds());

        assertEquals(task.affinityPartitionId(), job.affinityPartitionId());
    }

    /** Check job fields. */
    private void checkJobView(ComputeJobView job) {
        checkJobView(job, getClass().getName(), ACTIVE);
    }

    /** Check job fields. */
    private void checkJobView(ComputeJobView job, String taskPrefix, ComputeJobState state) {
        assertFalse(job.isInternal());
        assertNull(job.affinityCacheIds());
        assertEquals(-1, job.affinityPartitionId());
        assertTrue(job.taskClassName().startsWith(getClass().getName()));
        assertTrue(job.taskName().startsWith(taskPrefix));
        assertEquals(client.localNode().id(), job.originNodeId());
        assertEquals(state, job.state());
        assertEquals(0, job.finishTime());

        if (state == ACTIVE) {
            assertTrue(job.startTime() > 0);
            assertTrue(job.isStarted());
        }
    }

    /** */
    @IgniteSpiMultipleInstancesSupport(true)
    private static class CancelCollisionSpi extends IgniteSpiAdapter implements CollisionSpi {
        /** {@inheritDoc} */
        @Override public void onCollision(CollisionContext ctx) {
            for (CollisionJobContext job : ctx.waitingJobs()) {
                // Waiting for test checks job in the `PASSIVE` state then activating it.
                waitForCancelJobChecks(job);

                job.activate();
            }

            for (CollisionJobContext job : ctx.activeJobs()) {
                // Cancelling job and then waiting for test checks job in the `CANCELLED` state.
                if (job.getTaskSession().getTaskName().equalsIgnoreCase("cancel-task"))
                    job.cancel();

                waitForCancelJobChecks(job);
            }
        }

        /** */
        private void waitForCancelJobChecks(CollisionJobContext job) {
            if (job.getTaskSession().getTaskName().equalsIgnoreCase("cancel-task")) {
                try {
                    barrier.await(TIMEOUT, MILLISECONDS);
                    barrier.await(TIMEOUT, MILLISECONDS);
                }
                catch (InterruptedException | BrokenBarrierException | TimeoutException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override public void setExternalCollisionListener(@Nullable CollisionExternalListener lsnr) {
            // No-op.
        }

        @Override public void spiStart(@Nullable String igniteInstanceName) throws IgniteSpiException {
            // No-op.
        }

        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }
    }
}
