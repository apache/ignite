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
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.spi.systemview.view.ComputeJobView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.ignite.internal.processors.job.GridJobProcessor.JOB_VIEW;

/** Tests for compute task {@link SystemView}. */
public class SystemViewComputeJobTest extends GridCommonAbstractTest {
    public static final long TIMEOUT = 5_000L;

    private static CyclicBarrier barrier;

    private static IgniteEx server;

    private static IgniteEx client;

    private static IgniteCache<Integer, Integer> cache;

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

        SystemView<ComputeJobView> tasks = server.context().systemView().view(JOB_VIEW);

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

        assertEquals(5, tasks.size());

        ComputeJobView t = tasks.iterator().next();

        checkTask(t);

        barrier.await(TIMEOUT, MILLISECONDS);
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#runAsync(IgniteRunnable)} call. */
    @Test
    public void testComputeRunnable() throws Exception {
        barrier = new CyclicBarrier(2);

        SystemView<ComputeJobView> tasks = server.context().systemView().view(JOB_VIEW);

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

        assertEquals(1, tasks.size());

        ComputeJobView t = tasks.iterator().next();

        checkTask(t);

        barrier.await(TIMEOUT, MILLISECONDS);
    }

    /** Tests work of {@link SystemView} for compute grid {@link IgniteCompute#apply(IgniteClosure, Object)} call. */
    @Test
    public void testComputeApply() throws Exception {
        barrier = new CyclicBarrier(2);

        SystemView<ComputeJobView> tasks = server.context().systemView().view(JOB_VIEW);

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

        assertEquals(1, tasks.size());

        ComputeJobView t = tasks.iterator().next();

        checkTask(t);

        barrier.await(TIMEOUT, MILLISECONDS);
    }

    /**
     * Tests work of {@link SystemView} for compute grid
     * {@link IgniteCompute#affinityCallAsync(String, Object, IgniteCallable)} call.
     */
    @Test
    public void testComputeAffinityCall() throws Exception {
        barrier = new CyclicBarrier(2);

        SystemView<ComputeJobView> jobs = server.context().systemView().view(JOB_VIEW);

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
    }

    /** */
    @Test
    public void testComputeTask() throws Exception {
        barrier = new CyclicBarrier(2);

        SystemView<ComputeJobView> tasks = server.context().systemView().view(JOB_VIEW);

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

        assertEquals(1, tasks.size());

        ComputeJobView t = tasks.iterator().next();

        checkTask(t);

        barrier.await(TIMEOUT, MILLISECONDS);
    }

    /** Check tasks fields. */
    private void checkTask(ComputeJobView t) {
        assertFalse(t.isInternal());
        assertNull(t.affinityCacheIds());
        assertEquals(-1, t.affinityPartitionId());
        assertTrue(t.taskClassName().startsWith(getClass().getName()));
        assertTrue(t.taskName().startsWith(getClass().getName()));
        assertEquals(client.localNode().id(), t.originNodeId());
    }
}
