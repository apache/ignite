/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.closure;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.marshaller.optimized.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;
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
    private final GridTcpDiscoveryIpFinder ipFinder = new GridTcpDiscoveryVmIpFinder(true);

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMarshaller(new GridOptimizedMarshaller(false));

        GridTcpDiscoverySpi discoSpi = new GridTcpDiscoverySpi();

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
    private static class TestRunnable implements GridRunnable {
        /** */
        @GridInstanceResource
        private Grid grid;

        /** */
        @GridLoggerResource
        private GridLogger log;

        /** @{inheritDoc} */
        @Override public void run() {
            log.info("Runnable job executed on node: " + grid.localNode().id());

            assert grid != null;

            execCntr.incrementAndGet();
        }
    }

    /**
     * Base class for test callables.
     */
    private abstract static class AbstractTestCallable implements GridCallable<Integer> {
        /** */
        @GridInstanceResource
        protected Grid grid;

        /** */
        @GridLoggerResource
        protected GridLogger log;
    }

    /**
     * Test callable job.
     */
    private static class TestCallable extends AbstractTestCallable {
        /** {@inheritDoc} */
        @Override public Integer call() {
            log.info("Callable job executed on node: " + grid.localNode().id());

            assert grid != null;

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
            log.info("Callable job executed on node: " + grid.localNode().id());

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
     * @throws GridException If failed.
     */
    private GridFuture<?> runAsync(int idx, Runnable job, @Nullable GridPredicate<GridNode> p)
        throws GridException {
        assert idx >= 0 && idx < NODES_CNT;
        assert job != null;

        execCntr.set(0);

        GridCompute comp = p != null ? grid(idx).forPredicate(p).compute() : grid(idx).compute();

        comp = comp.enableAsync();

        comp.run(job);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @param job Runnable job.
     * @param p Optional node predicate.
     * @return Future object.
     * @throws GridException If failed.
     */
    private GridFuture<?> broadcast(int idx, Runnable job, @Nullable GridPredicate<GridNode> p)
        throws GridException {
        assert idx >= 0 && idx < NODES_CNT;
        assert job != null;

        execCntr.set(0);

        GridProjection prj = grid(idx);

        if (p != null)
            prj = prj.forPredicate(p);

        GridCompute comp = prj.compute().enableAsync();

        comp.broadcast(job);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @param jobs Runnable jobs.
     * @param p Optional node predicate.
     * @return Future object.
     * @throws GridException If failed.
     */
    private GridFuture<?> runAsync(int idx, Collection<TestRunnable> jobs, @Nullable GridPredicate<GridNode> p)
        throws GridException {
        assert idx >= 0 && idx < NODES_CNT;
        assert !F.isEmpty(jobs);

        execCntr.set(0);

        GridCompute comp = p != null ? grid(idx).forPredicate(p).compute() : grid(idx).compute();

        comp = comp.enableAsync();

        comp.run(jobs);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @param job Callable job.
     * @param p Optional node predicate.
     * @return Future object.
     * @throws GridException If failed.
     */
    private GridFuture<Integer> callAsync(int idx, Callable<Integer> job, @Nullable GridPredicate<GridNode> p)
        throws GridException {
        assert idx >= 0 && idx < NODES_CNT;
        assert job != null;

        execCntr.set(0);

        GridCompute comp = p != null ? grid(idx).forPredicate(p).compute() : grid(idx).compute();

        comp = comp.enableAsync();

        comp.call(job);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @param job Callable job.
     * @param p Optional node predicate.
     * @return Future object.
     * @throws GridException If failed.
     */
    private GridFuture<Collection<Integer>> broadcast(int idx, Callable<Integer> job,
        @Nullable GridPredicate<GridNode> p) throws GridException {
        assert idx >= 0 && idx < NODES_CNT;
        assert job != null;

        execCntr.set(0);

        GridCompute comp = p != null ? grid(idx).forPredicate(p).compute() : grid(idx).compute();

        comp = comp.enableAsync();

        comp.broadcast(job);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @param jobs Callable job.
     * @param p Optional node predicate.
     * @return Future object.
     * @throws GridException If failed.
     */
    private GridFuture<Collection<Integer>> callAsync(int idx, Collection<TestCallable> jobs,
        @Nullable GridPredicate<GridNode> p) throws GridException {
        assert idx >= 0 && idx < NODES_CNT;
        assert !F.isEmpty(jobs);

        execCntr.set(0);

        GridCompute comp = p != null ? grid(idx).forPredicate(p).compute() : grid(idx).compute();

        comp = comp.enableAsync();

        comp.call(jobs);

        return comp.future();
    }

    /**
     * @param idx Node index.
     * @return Predicate.
     */
    private GridPredicate<GridNode> singleNodePredicate(final int idx) {
        assert idx >= 0 && idx < NODES_CNT;

        return new GridPredicate<GridNode>() {
            @Override public boolean apply(GridNode e) { return grid(idx).localNode().id().equals(e.id()); }
        };
    }

    /**
     * @throws Exception If failed.
     */
    public void testRunAsyncSingle() throws Exception {
        Runnable job = new TestRunnable();

        GridFuture<?> fut = broadcast(0, job, null);

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

        GridFuture<?> fut = runAsync(0, jobs, null);

        assert fut.get() == null : "Execution result must be null.";

        assert execCntr.get() == jobs.size() :
            "Execution counter must be equal to " + jobs.size() + ", actual: " + execCntr.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallAsyncSingle() throws Exception {
        Callable<Integer> job = new TestCallable();

        GridFuture<Collection<Integer>> fut1 = broadcast(0, job, null);

        assert fut1.get() != null;

        assertEquals(NODES_CNT, execCntr.getAndSet(0));

        fut1 = broadcast(0, job, singleNodePredicate(0));

        // We left one node so we can get definite result.
        assertEquals(Integer.valueOf(1), F.first(fut1.get()));

        assertEquals(1, execCntr.get());

        GridFuture<Integer> fut2 = callAsync(0, job, null);

        assert fut2.get() == 1 :
            "Execution result must be equal to 1, actual: " + fut2.get();

        assert execCntr.get() == 1 :
            "Execution counter must be equal to 1, actual: " + execCntr.get();
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallAsyncErrorNoFailover() throws Exception {
        GridCompute comp = grid(0).forPredicate(F.notEqualTo(grid(0).localNode())).compute().enableAsync();

        comp.withNoFailover().call(new TestCallableError());

        GridFuture<Integer> fut = comp.future();

        try {
            fut.get();

            assert false : "Exception should have been thrown.";
        }
        catch (GridException e) {
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
        catch (GridComputeTaskTimeoutException ignore) {
            timedOut = true;
        }

        assert timedOut : "Task has not timed out.";

        timedOut = false;

        try {
            // Previous task invocation cleared the timeout.
            grid(0).compute().call(jobs);
        }
        catch (GridComputeTaskTimeoutException ignore) {
            timedOut = true;
        }

        assert !timedOut : "Subsequently called task has timed out.";
    }

    /**
     * @throws Exception If failed.
     */
    public void testCallAsyncMultiple() throws Exception {
        Collection<TestCallable> jobs = F.asList(new TestCallable(), new TestCallable());

        GridFuture<Collection<Integer>> fut = callAsync(0, jobs, null);

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

        GridCompute comp = grid(0).compute().enableAsync();

        comp.call(jobs, F.sumIntReducer());

        GridFuture<Integer> fut = comp.future();

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
        final Grid g = grid(0);

        final Collection<Callable<Integer>> jobs = new ArrayList<>();

        for (int i = 0; i < g.nodes().size(); i++) {
            jobs.add(new GridCallable<Integer>() {
                @Override public Integer call() throws Exception {
                    throw new RuntimeException("Test exception.");
                }
            });
        }

        GridTestUtils.assertThrows(log, new Callable<Void>() {
            @Override public Void call() throws Exception {
                g.compute().call(jobs, new GridReducer<Integer, Object>() {
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
        }, GridException.class, null);
    }
}
