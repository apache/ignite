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

package org.apache.ignite.internal.processors.closure;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.optimized.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.discovery.tcp.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.*;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.*;
import org.apache.ignite.testframework.*;
import org.apache.ignite.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Tests for {@link GridClosureProcessor}.
 */
@GridCommonTest(group = "Closure Processor")
public class GridClosureProcessorSelfTest extends GridCommonAbstractTest {
    /** Number of grids started for tests. Should not be less than 2. */
    private static final int NODES_CNT = 2;

    /** Job sleep duration in order to initiate timeout exception. */
    private static final long JOB_SLEEP = 200;

    /** Timeout used in timed tests. */
    private static final long JOB_TIMEOUT = 100;

    /** IP finder. */
    private final TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new OptimizedMarshaller(false));

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(ipFinder);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setCacheConfiguration();

        return cfg;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"ConstantConditions"})
    @Override protected void beforeTestsStarted() throws Exception {
        assert NODES_CNT >= 2;

        startGrids(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        execCntr.set(0);
    }

    /** Execution counter for runnable and callable jobs. */
    private static AtomicInteger execCntr = new AtomicInteger(0);

    /**
     * Test runnable job.
     */
    private static class TestRunnable implements IgniteRunnable {
        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** @{inheritDoc} */
        @Override public void run() {
            log.info("Runnable job executed on node: " + ignite.cluster().localNode().id());

            assert ignite != null;

            execCntr.incrementAndGet();
        }
    }

    /**
     * Base class for test callables.
     */
    private abstract static class AbstractTestCallable implements IgniteCallable<Integer> {
        /** */
        @IgniteInstanceResource
        protected Ignite ignite;

        /** */
        @LoggerResource
        protected IgniteLogger log;
    }

    /**
     * Test callable job.
     */
    private static class TestCallable extends AbstractTestCallable {
        /** {@inheritDoc} */
        @Override public Integer call() {
            log.info("Callable job executed on node: " + ignite.cluster().localNode().id());

            assert ignite != null;

            return execCntr.incrementAndGet();
        }
    }

    /**
     * Test callable job which throws class not found exception.
     */
    private static class TestCallableError extends AbstractTestCallable implements Externalizable {
        /**
         *
         */
        public TestCallableError() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public Integer call() {
            log.info("Callable job executed on node: " + ignite.cluster().localNode().id());

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            throw new ClassNotFoundException();
        }
    }

    /**
     * Test callable job which sleeps for some time. Is used in timeout tests.
     */
    private static class TestCallableTimeout extends AbstractTestCallable {
        /** {@inheritDoc} */
        @Override public Integer call() throws Exception {
            Thread.sleep(JOB_SLEEP);

            return null;
        }
    }

    /**
     * @param idx Node index.
     * @param job Runnable job.
     * @param p Optional node predicate.
     * @return Future object.
     */
    private ComputeTaskFuture<?> runAsync(int idx, IgniteRunnable job, @Nullable IgnitePredicate<ClusterNode> p) {
        assert idx >= 0 && idx < NODES_CNT;
        assert job != null;

        execCntr.set(0);

        IgniteCompute comp = p != null ? compute(grid(idx).cluster().forPredicate(p)) : grid(idx).compute();

        comp = comp.withAsync();

        comp.run(job);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @param job Runnable job.
     * @param p Optional node predicate.
     * @return Future object.
     */
    private ComputeTaskFuture<?> broadcast(int idx, IgniteRunnable job, @Nullable IgnitePredicate<ClusterNode> p) {
        assert idx >= 0 && idx < NODES_CNT;
        assert job != null;

        execCntr.set(0);

        ClusterGroup prj = grid(idx).cluster();

        if (p != null)
            prj = prj.forPredicate(p);

        IgniteCompute comp = compute(prj).withAsync();

        comp.broadcast(job);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @param jobs Runnable jobs.
     * @param p Optional node predicate.
     * @return Future object.
     */
    private ComputeTaskFuture<?> runAsync(int idx,
        Collection<TestRunnable> jobs,
        @Nullable IgnitePredicate<ClusterNode> p)
    {
        assert idx >= 0 && idx < NODES_CNT;
        assert !F.isEmpty(jobs);

        execCntr.set(0);

        IgniteCompute comp = p != null ? compute(grid(idx).cluster().forPredicate(p)) : grid(idx).compute();

        comp = comp.withAsync();

        comp.run(jobs);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @param job Callable job.
     * @param p Optional node predicate.
     * @return Future object.
     */
    private ComputeTaskFuture<Integer> callAsync(int idx,
        IgniteCallable<Integer> job, @Nullable
        IgnitePredicate<ClusterNode> p) {
        assert idx >= 0 && idx < NODES_CNT;
        assert job != null;

        execCntr.set(0);

        IgniteCompute comp = p != null ? compute(grid(idx).cluster().forPredicate(p)) : grid(idx).compute();

        comp = comp.withAsync();

        comp.call(job);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @param job Callable job.
     * @param p Optional node predicate.
     * @return Future object.
     */
    private ComputeTaskFuture<Collection<Integer>> broadcast(int idx, IgniteCallable<Integer> job,
        @Nullable IgnitePredicate<ClusterNode> p) {
        assert idx >= 0 && idx < NODES_CNT;
        assert job != null;

        execCntr.set(0);

        IgniteCompute comp = p != null ? compute(grid(idx).cluster().forPredicate(p)) : grid(idx).compute();

        comp = comp.withAsync();

        comp.broadcast(job);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @param jobs Callable job.
     * @param p Optional node predicate.
     * @return Future object.
     */
    private ComputeTaskFuture<Collection<Integer>> callAsync(int idx, Collection<TestCallable> jobs,
        @Nullable IgnitePredicate<ClusterNode> p) {
        assert idx >= 0 && idx < NODES_CNT;
        assert !F.isEmpty(jobs);

        execCntr.set(0);

        IgniteCompute comp = p != null ? compute(grid(idx).cluster().forPredicate(p)) : grid(idx).compute();

        comp = comp.withAsync();

        comp.call(jobs);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @return Predicate.
     */
    private IgnitePredicate<ClusterNode> singleNodePredicate(final int idx) {
        assert idx >= 0 && idx < NODES_CNT;

        return new IgnitePredicate<ClusterNode>() {
            @Override public boolean apply(ClusterNode e) { return grid(idx).localNode().id().equals(e.id()); }
        };
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunAsyncSingle() throws Exception {
        IgniteRunnable job = new TestRunnable();

        ComputeTaskFuture<?> fut = broadcast(0, job, null);

        assert fut.get() == null;

        assertEquals(NODES_CNT, execCntr.getAndSet(0));

        fut = broadcast(0, job, singleNodePredicate(0));

        assert fut.get() == null;

        assertEquals(1, execCntr.get());

        fut = runAsync(0, job, null);

        assert fut.get() == null : "Execution result must be null.";

        assert execCntr.get() == 1 :
            "Execution counter must be equal to 1, actual: " + execCntr.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunAsyncMultiple() throws Exception {
        Collection<TestRunnable> jobs = F.asList(new TestRunnable(), new TestRunnable());

        ComputeTaskFuture<?> fut = runAsync(0, jobs, null);

        assert fut.get() == null : "Execution result must be null.";

        assert execCntr.get() == jobs.size() :
            "Execution counter must be equal to " + jobs.size() + ", actual: " + execCntr.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallAsyncSingle() throws Exception {
        IgniteCallable<Integer> job = new TestCallable();

        ComputeTaskFuture<Collection<Integer>> fut1 = broadcast(0, job, null);

        assert fut1.get() != null;

        assertEquals(NODES_CNT, execCntr.getAndSet(0));

        fut1 = broadcast(0, job, singleNodePredicate(0));

        // We left one node so we can get definite result.
        assertEquals(Integer.valueOf(1), F.first(fut1.get()));

        assertEquals(1, execCntr.get());

        ComputeTaskFuture<Integer> fut2 = callAsync(0, job, null);

        assert fut2.get() == 1 :
            "Execution result must be equal to 1, actual: " + fut2.get();

        assert execCntr.get() == 1 :
            "Execution counter must be equal to 1, actual: " + execCntr.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallAsyncErrorNoFailover() throws Exception {
        IgniteCompute comp = compute(grid(0).cluster().forPredicate(F.notEqualTo(grid(0).localNode()))).withAsync();

        comp.withNoFailover().call(new TestCallableError());

        ComputeTaskFuture<Integer> fut = comp.future();

        try {
            fut.get();

            assert false : "Exception should have been thrown.";
        }
        catch (IgniteException e) {
            info("Caught expected exception: " + e);
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithName() throws Exception {
        grid(0).compute().withName("TestTaskName").call(new TestCallable());
    }

    /**
     * @throws Exception If failed.
     */
    public void testWithTimeout() throws Exception {
        Collection<TestCallableTimeout> jobs = F.asList(new TestCallableTimeout());

        boolean timedOut = false;

        try {
            // Ensure that we will get timeout exception.
            grid(0).compute().withTimeout(JOB_TIMEOUT).call(jobs);
        }
        catch (ComputeTaskTimeoutException ignore) {
            timedOut = true;
        }

        assert timedOut : "Task has not timed out.";

        timedOut = false;

        try {
            // Previous task invocation cleared the timeout.
            grid(0).compute().call(jobs);
        }
        catch (ComputeTaskTimeoutException ignore) {
            timedOut = true;
        }

        assert !timedOut : "Subsequently called task has timed out.";
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallAsyncMultiple() throws Exception {
        Collection<TestCallable> jobs = F.asList(new TestCallable(), new TestCallable());

        ComputeTaskFuture<Collection<Integer>> fut = callAsync(0, jobs, null);

        Collection<Integer> results = fut.get();

        assert !results.isEmpty() : "Collection of results is empty.";

        assert results.size() == jobs.size() :
            "Collection of results must be of size: " + jobs.size() + ".";

        for (int i = 1; i <= jobs.size(); i++)
            assert results.contains(i) : "Collection of results does not contain value: " + i;
    }

    /**
     * @throws Exception If failed.
     */
    public void testReduceAsync() throws Exception {
        Collection<TestCallable> jobs = F.asList(new TestCallable(), new TestCallable());

        IgniteCompute comp = grid(0).compute().withAsync();

        comp.call(jobs, F.sumIntReducer());

        ComputeTaskFuture<Integer> fut = comp.future();

        // Sum of arithmetic progression.
        int exp = (1 + jobs.size()) * jobs.size() / 2;

        assert fut.get() == exp :
            "Execution result must be equal to " + exp + ", actual: " + fut.get();

        assert execCntr.get() == jobs.size() :
            "Execution counter must be equal to " + jobs.size() + ", actual: " + execCntr.get();

        execCntr.set(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testReducerError() throws Exception {
        final Ignite g = grid(0);

        final Collection<IgniteCallable<Integer>> jobs = new ArrayList<>();

        for (int i = 0; i < g.cluster().nodes().size(); i++) {
            jobs.add(new IgniteCallable<Integer>() {
                @Override public Integer call() throws Exception {
                    throw new RuntimeException("Test exception.");
                }
            });
        }

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                g.compute().call(jobs, new IgniteReducer<Integer, Object>() {
                    @Override public boolean collect(@Nullable Integer e) {
                        fail("Expects failed jobs never call 'collect' method.");

                        return true;
                    }

                    @Override public Object reduce() {
                        return null;
                    }
                });

                return null;
            }
        }, IgniteException.class, null);
    }
}
