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

package org.apache.ignite.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.GridTestUtils.SF;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

/**
 *
 */
public class TaskNodeRestartTest extends GridCommonAbstractTest {
    /** */
    private static final int NODES = 3;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpCommunicationSpi)cfg.getCommunicationSpi()).setSharedMemoryPort(-1);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGridsMultiThreaded(NODES);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTaskNodeRestart() throws Exception {
        final AtomicBoolean finished = new AtomicBoolean();

        final AtomicInteger stopIdx = new AtomicInteger();

        IgniteInternalFuture<?> restartFut = GridTestUtils.runMultiThreadedAsync(new Callable<Object>() {
            @Override public Object call() throws Exception {
                int idx = stopIdx.getAndIncrement();

                int node = NODES + idx;

                while (!finished.get()) {
                    log.info("Start node: " + node);

                    startGrid(node);

                    U.sleep(300);

                    log.info("Stop node: " + node);

                    stopGrid(node);
                }

                return null;
            }
        }, 2, "stop-thread");

        IgniteInternalFuture<?> fut = null;

        try {
            final long stopTime = System.currentTimeMillis() + SF.applyLB(30_000, 10_000);

            final AtomicInteger idx = new AtomicInteger();

            fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
                @Override public Void call() throws Exception {
                    int node = idx.getAndIncrement() % NODES;

                    Ignite ignite = ignite(node);

                    log.info("Start thread: " + ignite.name());

                    IgniteCompute compute = ignite.compute();

                    while (U.currentTimeMillis() < stopTime) {
                        try {
                            compute.broadcast(new TestCallable());

                            compute.call(new TestCallable());

                            compute.execute(new TestTask1(), null);

                            compute.execute(new TestTask2(), null);
                        }
                        catch (IgniteException e) {
                            log.info("Error: " + e);
                        }
                    }

                    return null;
                }
            }, 20, "test-thread");

            fut.get(90_000);

            finished.set(true);

            restartFut.get();
        }
        finally {
            finished.set(true);

            if (fut != null)
                fut.cancel();

            restartFut.get(5000);
        }
    }

    /**
     *
     */
    private static class TestTask1 extends ComputeTaskAdapter<Void, Void> {
        /** {@inheritDoc} */
        @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Void arg)
            throws IgniteException {
            Map<TestJob, ClusterNode> jobs = new HashMap<>();

            for (ClusterNode node : subgrid)
                jobs.put(new TestJob(), node);

            return jobs;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) throws IgniteException {
            return null;
        }
    }

    /**
     *
     */
    private static class TestTask2 implements ComputeTask<Void, Void> {
        /** {@inheritDoc} */
        @NotNull @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Void arg)
            throws IgniteException {
            Map<TestJob, ClusterNode> jobs = new HashMap<>();

            for (ClusterNode node : subgrid)
                jobs.put(new TestJob(), node);

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) {
            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     *
     */
    private static class TestJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            return null;
        }
    }

    /**
     *
     */
    private static class TestCallable implements IgniteCallable<Void> {
        /** {@inheritDoc} */
        @Nullable @Override public Void call() throws Exception {
            return null;
        }
    }
}
