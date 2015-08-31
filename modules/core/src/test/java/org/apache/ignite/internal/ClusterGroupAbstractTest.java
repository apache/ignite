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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteReducer;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_JOB_STARTED;

/**
 * Abstract test for {@link org.apache.ignite.cluster.ClusterGroup}
 */
@SuppressWarnings("deprecation")
public abstract class ClusterGroupAbstractTest extends GridCommonAbstractTest implements Externalizable {
    /** VM ip finder for TCP discovery. */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** Waiting timeout. */
    private static final int WAIT_TIMEOUT = 30000;

    /** Utility static variable. */
    private static final AtomicInteger cnt = new AtomicInteger(0);

    /** Mutex. */
    private static final Object mux = new Object();

    /** Projection. */
    private ClusterGroup prj;

    /** Runnable job. */
    private IgniteRunnable runJob = new TestRunnable();

    /** Callable job. */
    private IgniteCallable<String> calJob = new TestCallable<>();

    /** Closure job. */
    private IgniteClosure<String, String> clrJob = new IgniteClosure<String, String>() {
        @Override public String apply(String s) {
            return s;
        }

        @Override public String toString() {
            return "clrJob";
        }
    };

    /** Reducer. */
    private IgniteReducer<String, Object> rdc = new IgniteReducer<String, Object>() {
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
    protected ClusterGroupAbstractTest() {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setDiscoverySpi(new TcpDiscoverySpi().setForceServerMode(true).setIpFinder(ipFinder));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        prj = projection();

        cnt.set(0);
    }

    /**
     * @return Projection.
     */
    protected abstract ClusterGroup projection();

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

        ClusterGroup invalidPrj = prj.forNodeIds(ids);

        assertEquals(0, invalidPrj.nodes().size());
    }

    /**
     * @throws Exception If test failed.
     */
    public void testProjection() throws Exception {
        assert prj != null;

        assert prj.ignite() != null;
        assert prj.predicate() != null;

        int size = projectionSize();

        assert prj.nodes().size() == size;

        Collection<UUID> nodeIds = projectionNodeIds();

        for (ClusterNode node : prj.nodes())
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
            Ignite g = startGrid(name);

            UUID excludedId = g.cluster().localNode().id();

            assertEquals(size, prj.forRemotes().nodes().size());

            for (ClusterNode node : prj.forRemotes().nodes()) {
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

        ClusterGroup remotePrj = projection().forRemotes();

        Collection<UUID> prjNodeIds = F.nodeIds(remotePrj.nodes());

        assert prjNodeIds.size() == remoteNodeIds.size();

        assert prjNodeIds.containsAll(remoteNodeIds());

        assert !prjNodeIds.contains(localNodeId());

        String name = "oneMoreGrid";

        try {
            Ignite g = startGrid(name);

            UUID excludedId = g.cluster().localNode().id();

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

        Collection<IgniteBiTuple<Ignite, IgnitePredicate<Event>>> lsnrs = new LinkedList<>();

        try {
            final AtomicInteger cnt = new AtomicInteger();

            Ignite g = startGrid(name);

            IgnitePredicate<Event> lsnr;

            if (!IgniteCluster.class.isAssignableFrom(projection().getClass())) {
                g.events().localListen(lsnr = new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
                        assert evt.type() == EVT_JOB_STARTED;

                        assert false;

                        return true;
                    }
                }, EVT_JOB_STARTED);

                lsnrs.add(F.t(g, lsnr));
            }

            for (ClusterNode node : prj.nodes()) {
                g = G.ignite(node.id());

                g.events().localListen(lsnr = new IgnitePredicate<Event>() {
                    @Override public boolean apply(Event evt) {
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

            checkActiveFutures();
        }
        finally {
            for (IgniteBiTuple<Ignite, IgnitePredicate<Event>> t : lsnrs)
                t.get1().events().stopLocalListen(t.get2(), EVT_JOB_STARTED);

            stopGrid(name);
        }
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void run1(AtomicInteger cnt) throws Exception {
        IgniteCompute comp = compute(prj).withAsync();

        comp.broadcast(runJob);

        ComputeTaskFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        compute(prj).broadcast(runJob);

        waitForValue(cnt, projectionSize());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void run2(AtomicInteger cnt) throws Exception {
        Collection<IgniteRunnable> jobs = F.asList(runJob);

        IgniteCompute comp = compute(prj).withAsync();

        comp.run(jobs);

        ComputeTaskFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        compute(prj).run(jobs);

        waitForValue(cnt, jobs.size());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void call1(AtomicInteger cnt) throws Exception {
        IgniteCompute comp = compute(prj).withAsync();

        comp.broadcast(calJob);

        ComputeTaskFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        compute(prj).broadcast(calJob);

        waitForValue(cnt, projectionSize());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void call2(AtomicInteger cnt) throws Exception {
        IgniteCompute comp = compute(prj).withAsync();

        Collection<IgniteCallable<String>> jobs = F.asList(calJob);

        comp.call(jobs);

        ComputeTaskFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        compute(prj).call(jobs);

        waitForValue(cnt, jobs.size());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void call3(AtomicInteger cnt) throws Exception {
        IgniteCompute comp = compute(prj).withAsync();

        comp.apply(clrJob, (String) null);

        ComputeTaskFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        compute(prj).apply(clrJob, (String) null);

        waitForValue(cnt, 1);
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void call4(AtomicInteger cnt) throws Exception {
        Collection<String> args = F.asList("a", "b", "c");

        IgniteCompute comp = compute(prj).withAsync();

        comp.apply(clrJob, args);

        ComputeTaskFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        compute(prj).apply(clrJob, args);

        waitForValue(cnt, args.size());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void call5(AtomicInteger cnt) throws Exception {
        IgniteCompute comp = compute(prj).withAsync();

        comp.broadcast(new TestClosure(), "arg");

        ComputeTaskFuture<Collection<String>> fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        Collection<String> res = compute(prj).broadcast(new TestClosure(), "arg");

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

        IgniteCompute comp = compute(prj).withAsync();

        comp.apply(clrJob, args, rdc);

        ComputeTaskFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        compute(prj).apply(clrJob, args, rdc);

        waitForValue(cnt, args.size());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void forkjoin2(AtomicInteger cnt) throws Exception {
        Collection<IgniteCallable<String>> jobs = F.asList(calJob);

        IgniteCompute comp = compute(prj).withAsync();

        comp.call(jobs, rdc);

        ComputeTaskFuture fut = comp.future();

        waitForExecution(fut);

        cnt.set(0);

        compute(prj).call(jobs, rdc);

        waitForValue(cnt, jobs.size());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void exec1(AtomicInteger cnt) throws Exception {
        cnt.set(0);

        compute(prj).execute(TestTask.class.getName(), null);

        waitForValue(cnt, projectionSize());

        cnt.set(0);

        compute(prj).execute(new TestTask(), null);

        waitForValue(cnt, projectionSize());

        cnt.set(0);

        compute(prj).execute(TestTask.class, null);

        waitForValue(cnt, projectionSize());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void exec2(AtomicInteger cnt) throws Exception {
        cnt.set(0);

        compute(prj).withTimeout(WAIT_TIMEOUT).execute(TestTask.class.getName(), null);

        waitForValue(cnt, projectionSize());

        cnt.set(0);

        compute(prj).withTimeout(WAIT_TIMEOUT).execute(new TestTask(), null);

        waitForValue(cnt, projectionSize());

        cnt.set(0);

        compute(prj).withTimeout(WAIT_TIMEOUT).execute(TestTask.class, null);

        waitForValue(cnt, projectionSize());
    }

    /**
     * @param cnt Counter.
     * @throws Exception If failed.
     */
    private void executorService(AtomicInteger cnt) throws Exception {
        cnt.set(0);

        ExecutorService execSrvc = prj.ignite().executorService(prj);

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
    private void waitForExecution(IgniteFuture fut) throws InterruptedException {
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
     * @throws Exception If test failed.
     */
    private void checkActiveFutures() throws Exception {
        IgniteCompute comp = compute(prj).withAsync();

        assertEquals(0, comp.activeTaskFutures().size());

        cnt.set(0);

        Collection<ComputeTaskFuture<Object>> futsList = new ArrayList<>();

        for (int i = 0; i < 10; i++) {
            comp.call(new TestWaitCallable<>());

            ComputeTaskFuture<Object> fut = comp.future();

            assertFalse(fut.isDone());

            Map<IgniteUuid, ComputeTaskFuture<Object>> futs = comp.activeTaskFutures();

            assertEquals(i + 1, futs.size());

            assertTrue(futs.containsKey(fut.getTaskSession().getId()));

            futsList.add(fut);
        }

        synchronized (mux) {
            cnt.incrementAndGet();

            mux.notifyAll();
        }

        for (ComputeTaskFuture<Object> fut : futsList)
            fut.get();

        assertEquals(0, comp.activeTaskFutures().size());
    }

    /**
     *  Test closure.
     */
    private static class TestClosure implements IgniteClosure<String, String> {
        /** {@inheritDoc} */
        @Override public String apply(String s) {
            return s;
        }
    }

    /**
     * Test runnable.
     */
    private static class TestRunnable implements IgniteRunnable {
        /** {@inheritDoc} */
        @Override public void run() {
            // No-op.
        }
    }

    /**
     * Test callable.
     */
    private static class TestCallable<T> implements IgniteCallable<T> {
        /** {@inheritDoc} */
        @Nullable @Override public T call() throws Exception {
            return null;
        }
    }

    /**
     * Test callable.
     */
    private static class TestWaitCallable<T> implements IgniteCallable<T> {
        /** {@inheritDoc} */
        @Nullable @Override public T call() throws Exception {
            synchronized (mux) {
                while (cnt.get() == 0)
                    mux.wait();
            }

            return null;
        }
    }

    /**
     * Test task.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestTask extends ComputeTaskSplitAdapter<String, Void> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) {
            Collection<ComputeJob> jobs = new HashSet<>();

            for (int i = 0; i < gridSize; i++)
                jobs.add(new TestJob());

            return jobs;
        }

        /** {@inheritDoc} */
        @Nullable @Override public Void reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Test job.
     */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestJob extends ComputeJobAdapter {
        /** {@inheritDoc} */
        @Nullable @Override public Object execute() {
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