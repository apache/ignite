/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 * Abstract test for {@link GridProjection}
 */
@SuppressWarnings("deprecation")
public abstract class GridProjectionAbstractTest extends GridCommonAbstractTest implements Externalizable {
    /** Waiting timeout. */
    private static final int WAIT_TIMEOUT = 30000;

    /** Utility static variable. */
    private static final AtomicInteger cnt = new AtomicInteger(0);

    /** Mutex. */
    private static final Object mux = new Object();

    /** Projection. */
    private GridProjection prj;

    /** Runnable job. */
    private Runnable runJob = new TestRunnable();

    /** Callable job. */
    private Callable<String> calJob = new TestCallable<>();

    /** Closure job. */
    private GridClosure<String, String> clrJob = new GridClosure<String, String>() {
        @Override public String apply(String s) {
            return s;
        }

        @Override public String toString() {
            return "clrJob";
        }
    };

    /** Reducer. */
    private GridReducer<String, Object> rdc = new GridReducer<String, Object>() {
        @Override public boolean collect(String e) {
            return true;
        }

        @Nullable @Override public Object reduce() {
            return null;
        }

        @Override public String toString() {
            return "rdc";
        }
    };

    /** */
    protected GridProjectionAbstractTest() {
        // No-op.
    }

    /**
     * @param startGrid Start grid flag.
     */
    protected GridProjectionAbstractTest(boolean startGrid) {
        super(startGrid);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        prj = projection();

        cnt.set(0);
    }

    /**
     * @return Projection.
     */
    protected abstract GridProjection projection();

    /**
     * @return Local node ID.
     */
    @Nullable protected abstract UUID localNodeId();

    /**
     * @return Remote nodes IDs.
     */
    protected Collection<UUID> remoteNodeIds() {
        return F.nodeIds(projection().forRemotes().nodes());
    }

    /**
     * @return Projection size.
     */
    private int projectionSize() {
        int size = localNodeId() != null ? 1 : 0;

        size += remoteNodeIds().size();

        assert size > 0;

        return size;
    }

    /**
     * @return Collection of projection node IDs.
     */
    private Collection<UUID> projectionNodeIds() {
        Collection<UUID> ids = new LinkedList<>();

        UUID id = localNodeId();

        if (id != null)
            ids.add(id);

        ids.addAll(remoteNodeIds());

        assert !ids.isEmpty();

        return ids;
    }

    /**
     * Test for projection on not existing node IDs.
     */
    public void testInvalidProjection() {
        Collection<UUID> ids = new HashSet<>();

        ids.add(UUID.randomUUID());
        ids.add(UUID.randomUUID());

        GridProjection invalidPrj = prj.forNodeIds(ids);

        assertEquals(0, invalidPrj.nodes().size());
    }

    /**
     * @throws Exception If test failed.
     */
    public void testProjection() throws Exception {
        assert prj != null;

        assert prj.grid() != null;
        assert prj.predicate() != null;

        int size = projectionSize();

        assert prj.nodes().size() == size;

        Collection<UUID> nodeIds = projectionNodeIds();

        for (GridNode node : prj.nodes())
            assert nodeIds.contains(node.id());
    }

    /**
     * @throws Exception If test failed.
     */
    public void testRemoteNodes() throws Exception {
        Collection<UUID> remoteNodeIds = remoteNodeIds();

        UUID locNodeId = localNodeId();

        int size = remoteNodeIds.size();

        String name = "oneMoreGrid";

        try {
            Grid g = startGrid(name);

            UUID excludedId = g.localNode().id();

            assertEquals(size, prj.forRemotes().nodes().size());

            for (GridNode node : prj.forRemotes().nodes()) {
                UUID id = node.id();

                assert !id.equals(locNodeId) && remoteNodeIds.contains(id) && !excludedId.equals(id);
            }
        }
        finally {
            stopGrid(name);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testRemoteProjection() throws Exception {
        Collection<UUID> remoteNodeIds = remoteNodeIds();

        GridProjection remotePrj = projection().forRemotes();

        Collection<UUID> prjNodeIds = F.nodeIds(remotePrj.nodes());

        assert prjNodeIds.size() == remoteNodeIds.size();

        assert prjNodeIds.containsAll(remoteNodeIds());

        assert !prjNodeIds.contains(localNodeId());

        String name = "oneMoreGrid";

        try {
            Grid g = startGrid(name);

            UUID excludedId = g.localNode().id();

            assert !F.nodeIds(remotePrj.nodes()).contains(excludedId);
        }
        finally {
            stopGrid(name);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testExecution() throws Exception {
        String name = "oneMoreGrid";

        Collection<GridBiTuple<Grid, GridPredicate<GridEvent>>> lsnrs = new LinkedList<>();

        try {
            final AtomicInteger cnt = new AtomicInteger();

            Grid g = startGrid(name);

            GridPredicate<GridEvent> lsnr;

            if (!Grid.class.isAssignableFrom(projection().getClass())) {
                g.events().localListen(lsnr = new GridPredicate<GridEvent>() {
                    @Override public boolean apply(GridEvent evt) {
                        assert evt.type() == EVT_JOB_STARTED;

                        assert false;

                        return true;
                    }
                }, EVT_JOB_STARTED);

                lsnrs.add(F.t(g, lsnr));
            }

            for (GridNode node : prj.nodes()) {
                g = G.grid(node.id());

                g.events().localListen(lsnr = new GridPredicate<GridEvent>() {
                    @Override public boolean apply(GridEvent evt) {
                        assert evt.type() == EVT_JOB_STARTED;

                        synchronized (mux) {
                            cnt.incrementAndGet();

                            mux.notifyAll();
                        }

                        return true;
                    }
                }, EVT_JOB_STARTED);

                lsnrs.add(F.t(g, lsnr));
            }

            run1(cnt);
            run2(cnt);

            call1(cnt);
            call2(cnt);
            call3(cnt);
            call4(cnt);
            call5(cnt);

            forkjoin1(cnt);
            forkjoin2(cnt);

            exec1(cnt);
            exec2(cnt);

            executorService(cnt);
        }
        finally {
            for (GridBiTuple<Grid, GridPredicate<GridEvent>> t : lsnrs)
                t.get1().events().stopLocalListen(t.get2(), EVT_JOB_STARTED);

            stopGrid(name);
        }
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void run1(AtomicInteger cnt) throws Exception {
        GridCompute comp = prj.compute().enableAsync();

        comp.broadcast(runJob);

        GridFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        prj.compute().broadcast(runJob);

        waitForValue(cnt, projectionSize());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void run2(AtomicInteger cnt) throws Exception {
        Collection<Runnable> jobs = F.asList(runJob);

        GridCompute comp = prj.compute().enableAsync();

        comp.run(jobs);

        GridFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        prj.compute().run(jobs);

        waitForValue(cnt, jobs.size());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void call1(AtomicInteger cnt) throws Exception {
        GridCompute comp = prj.compute().enableAsync();

        comp.broadcast(calJob);

        GridFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        prj.compute().broadcast(calJob);

        waitForValue(cnt, projectionSize());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void call2(AtomicInteger cnt) throws Exception {
        GridCompute comp = prj.compute().enableAsync();

        Collection<Callable<String>> jobs = F.asList(calJob);

        comp.call(jobs);

        GridFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        prj.compute().call(jobs);

        waitForValue(cnt, jobs.size());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void call3(AtomicInteger cnt) throws Exception {
        GridCompute comp = prj.compute().enableAsync();

        comp.apply(clrJob, (String) null);

        GridFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        prj.compute().apply(clrJob, (String) null);

        waitForValue(cnt, 1);
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void call4(AtomicInteger cnt) throws Exception {
        Collection<String> args = F.asList("a", "b", "c");

        GridCompute comp = prj.compute().enableAsync();

        comp.apply(clrJob, args);

        GridFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        prj.compute().apply(clrJob, args);

        waitForValue(cnt, args.size());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void call5(AtomicInteger cnt) throws Exception {
        GridCompute comp = prj.compute().enableAsync();

        comp.broadcast(new TestClosure(), "arg");

        GridFuture<Collection<String>> fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        Collection<String> res = prj.compute().broadcast(new TestClosure(), "arg");

        assertEquals(projectionSize(), res.size());

        waitForValue(cnt, projectionSize());

        for (String resStr : res)
            assertEquals("arg", resStr);
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void forkjoin1(AtomicInteger cnt) throws Exception {
        Collection<String> args = F.asList("a", "b", "c");

        GridCompute comp = prj.compute().enableAsync();

        comp.apply(clrJob, args, rdc);

        GridFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        prj.compute().apply(clrJob, args, rdc);

        waitForValue(cnt, args.size());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void forkjoin2(AtomicInteger cnt) throws Exception {
        Collection<Callable<String>> jobs = F.asList(calJob);

        GridCompute comp = prj.compute().enableAsync();

        comp.call(jobs, rdc);

        GridFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        prj.compute().call(jobs, rdc);

        waitForValue(cnt, jobs.size());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void exec1(AtomicInteger cnt) throws Exception {
        cnt.set(0);

        prj.compute().execute(TestTask.class.getName(), null);

        waitForValue(cnt, projectionSize());

        cnt.set(0);

        prj.compute().execute(new TestTask(), null);

        waitForValue(cnt, projectionSize());

        cnt.set(0);

        prj.compute().execute(TestTask.class, null);

        waitForValue(cnt, projectionSize());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void exec2(AtomicInteger cnt) throws Exception {
        cnt.set(0);

        prj.compute().withTimeout(WAIT_TIMEOUT).execute(TestTask.class.getName(), null);

        waitForValue(cnt, projectionSize());

        cnt.set(0);

        prj.compute().withTimeout(WAIT_TIMEOUT).execute(new TestTask(), null);

        waitForValue(cnt, projectionSize());

        cnt.set(0);

        prj.compute().withTimeout(WAIT_TIMEOUT).execute(TestTask.class, null);

        waitForValue(cnt, projectionSize());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void executorService(AtomicInteger cnt) throws Exception {
        cnt.set(0);

        ExecutorService execSrvc = prj.executorService();

        Future<String> fut = execSrvc.submit(new TestCallable<String>() {
            @Override public String call() throws Exception {
                return "submit1";
            }
        });

        waitForValue(cnt, 1);

        assertEquals("submit1", fut.get());

        cnt.set(0);

        fut = execSrvc.submit(new TestRunnable(), "submit2");

        waitForValue(cnt, 1);

        assertEquals("submit2", fut.get());

        cnt.set(0);

        Future<?> runFut = execSrvc.submit(new TestRunnable());

        waitForValue(cnt, 1);

        runFut.get();
    }

    /**
     * @param fut Execution future.
     * @throws InterruptedException Thrown if wait was interrupted.
     */
    @SuppressWarnings({"UnconditionalWait"})
    private void waitForExecution(GridFuture fut) throws InterruptedException {
        long sleep = 250;

        long threshold = System.currentTimeMillis() + WAIT_TIMEOUT;

        do synchronized (mux) {
            mux.wait(sleep);
        }
        while (fut != null && !fut.isDone() && !fut.isCancelled() && threshold > System.currentTimeMillis());

        assert fut == null || fut.isDone();
    }

    /**
     * @param cnt Counter to check.
     * @param val Value to check.
     * @throws InterruptedException Thrown if wait was interrupted.
     */
    private void waitForValue(AtomicInteger cnt, int val) throws InterruptedException {
        assert cnt != null;
        assert val > 0;

        long threshold = System.currentTimeMillis() + WAIT_TIMEOUT;

        long time;

        while (threshold > (time = System.currentTimeMillis()))
            synchronized (mux) {
                if (cnt.get() == val)
                    break;

                mux.wait(threshold - time);
            }

        assert cnt.get() == val;
    }

    /**
     *  Test closure.
     */
    private static class TestClosure implements GridClosure<String, String> {
        /** {@inheritDoc} */
        @Override public String apply(String s) {
            return s;
        }
    }

    /**
     * Test runnable.
     */
    private static class TestRunnable implements Runnable, Serializable {
        /** {@inheritDoc} */
        @Override public void run() {
            // No-op.
        }
    }

    /**
     * Test callable.
     */
    private static class TestCallable<T> implements Callable<T>, Serializable {
        /** {@inheritDoc} */
        @Nullable @Override public T call() throws Exception {
            return null;
        }
    }

    /**
     * Test task.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestTask extends GridComputeTaskSplitAdapter<String, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, String arg) throws GridException {
            Collection<GridComputeJob> jobs = new HashSet<>();

            for (int i = 0; i < gridSize; i++)
                jobs.add(new TestJob());

            return jobs;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }

    /**
     * Test job.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestJob extends GridComputeJobAdapter {
        /** {@inheritDoc} */
        @Nullable @Override public Object execute() throws GridException {
            return null;
        }
    }

    /** {@inheritDoc} */
    @Override public void writeExternal(ObjectOutput out) throws IOException {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // No-op.
    }
}
