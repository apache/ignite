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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJob;
import org.apache.ignite.compute.ComputeJobAdapter;
import org.apache.ignite.compute.ComputeJobContext;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.compute.ComputeJobResultPolicy;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskAdapter;
import org.apache.ignite.compute.ComputeTaskContinuousMapper;
import org.apache.ignite.compute.ComputeTaskFuture;
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionAttributeListener;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSplitAdapter;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.JobContextResource;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.resources.TaskContinuousMapperResource;
import org.apache.ignite.resources.TaskSessionResource;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.testframework.junits.common.GridCommonTest;
import org.jetbrains.annotations.Nullable;

/**
 * Continuous task test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridContinuousTaskSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int JOB_COUNT = 10;

    /** */
    private static final int THREAD_COUNT = 10;

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousJobsChain() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            IgniteCompute comp = ignite.compute().withAsync();

            comp.execute(TestJobsChainTask.class, true);

            ComputeTaskFuture<Integer> fut1 = comp.future();

            comp.execute(TestJobsChainTask.class, false);

            ComputeTaskFuture<Integer> fut2 = comp.future();

            assert fut1.get() == 55;
            assert fut2.get() == 55;
        }
        finally {
            stopGrid(0);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousJobsChainMultiThreaded() throws Exception {
        try {
            final Ignite ignite = startGrid(0);
            startGrid(1);

            GridTestUtils.runMultiThreaded(new Runnable() {
                /** {@inheritDoc} */
                @Override public void run() {
                    try {
                        IgniteCompute comp = ignite.compute().withAsync();

                        comp.execute(TestJobsChainTask.class, true);

                        ComputeTaskFuture<Integer> fut1 = comp.future();

                        comp.execute(TestJobsChainTask.class, false);

                        ComputeTaskFuture<Integer> fut2 = comp.future();

                        assert fut1.get() == 55;
                        assert fut2.get() == 55;
                    }
                    catch (IgniteException e) {
                        assert false : "Test task failed: " + e;
                    }
                }

            }, THREAD_COUNT, "continuous-jobs-chain");
        }
        finally {
            stopGrid(0);
            stopGrid(1);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousJobsSessionChain() throws Exception {
        try {
            Ignite ignite = startGrid(0);
            startGrid(1);

            ignite.compute().execute(SessionChainTestTask.class, false);
        }
        finally {
            stopGrid(0);
            stopGrid(1);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousSlowMap() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            Integer cnt = ignite.compute().execute(SlowMapTestTask.class, null);

            assert cnt != null;
            assert cnt == 2 : "Unexpected result: " + cnt;
        }
        finally {
            stopGrid(0);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testClearTimeouts() throws Exception {
        int holdccTimeout = 4000;

        try {
            Ignite grid = startGrid(0);

            TestClearTimeoutsClosure closure = new TestClearTimeoutsClosure();

            grid.compute().apply(closure, holdccTimeout);

            Thread.sleep(holdccTimeout * 2);

            assert closure.counter == 2;
        }
        finally {
            stopGrid(0);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testMultipleHoldccCalls() throws Exception {
        try {
            Ignite grid = startGrid(0);

            assertTrue(grid.compute().apply(new TestMultipleHoldccCallsClosure(), (Object)null));
        }
        finally {
            stopGrid(0);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testClosureWithNestedInternalTask() throws Exception {
        try {
            IgniteEx ignite = startGrid(0);

            ComputeTaskInternalFuture<String> fut = ignite.context().closure().callAsync(GridClosureCallMode.BALANCE, new Callable<String>() {
                /** */
                @IgniteInstanceResource
                private IgniteEx g;

                @Override public String call() throws Exception {
                    return g.compute(g.cluster()).execute(NestedHoldccTask.class, null);
                }
            }, ignite.cluster().nodes());

            assertEquals("DONE", fut.get(3000));
        }
        finally {
            stopGrid(0, true);
        }
    }

    /** Test task with continuation. */
    @GridInternal
    public static class NestedHoldccTask extends ComputeTaskAdapter<String, String> {
        /** {@inheritDoc} */
        @Nullable @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid,
            @Nullable String arg) throws IgniteException {
            Map<ComputeJob, ClusterNode> map = new HashMap<>();

            for (ClusterNode node : subgrid)
                map.put(new NestedHoldccJob(), node);

            return map;

        }

        /** {@inheritDoc} */
        @Nullable @Override public String reduce(List<ComputeJobResult> results) throws IgniteException {
            return results.get(0).getData();
        }
    }

    /** Test job. */
    public static class NestedHoldccJob extends ComputeJobAdapter {
        /** */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** */
        private int cnt = 0;

        /** {@inheritDoc} */
        @Override public Object execute() throws IgniteException {
            if (cnt < 1) {
                cnt++;

                jobCtx.holdcc();

                new Timer().schedule(new TimerTask() {
                    @Override public void run() {
                        jobCtx.callcc();
                    }
                }, 500);

                return "NOT DONE";
            }

            return "DONE";
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestMultipleHoldccCallsClosure implements IgniteClosure<Object, Boolean> {
        /** */
        private int counter;

        /** */
        private volatile boolean success;

        /** Auto-inject job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        /** */
        @LoggerResource
        private IgniteLogger log;

        @Override public Boolean apply(Object param) {
            counter++;

            if (counter == 2)
                return success;

            jobCtx.holdcc(4000);

            try {
                jobCtx.holdcc();
            }
            catch (IllegalStateException ignored) {
                success = true;
                log.info("Second holdcc() threw IllegalStateException as expected.");
            }
            finally {
                new Timer().schedule(new TimerTask() {
                    @Override public void run() {
                        jobCtx.callcc();
                    }
                }, 1000);
            }

            return false;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestClearTimeoutsClosure implements IgniteClosure<Integer, Object> {
        /** */
        private int counter;

        /** Auto-inject job context. */
        @JobContextResource
        private ComputeJobContext jobCtx;

        @Override public Object apply(Integer holdccTimeout) {
            assert holdccTimeout >= 2000;

            counter++;

            if (counter == 1) {
                new Timer().schedule(new TimerTask() {
                    @Override public void run() {
                        jobCtx.callcc();
                    }
                }, 1000);

                jobCtx.holdcc(holdccTimeout);
            }

            if (counter == 2)
                // Job returned from the suspended state.
                return null;

            return null;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestJobsChainTask implements ComputeTask<Boolean, Integer> {
        /** */
        @TaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Boolean arg) {
            assert mapper != null;
            assert arg != null;

            ComputeJob job = new TestJob(++cnt);

            if (arg) {
                mapper.send(job, subgrid.get(0));

                log.info("Sent test task by continuous mapper: " + job);
            }
            else {
                log.info("Will return job as map() result: " + job);

                return Collections.singletonMap(job, subgrid.get(0));
            }

            return null;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) {
            assert mapper != null;
            assert res.getException() == null : "Unexpected exception: " + res.getException();

            log.info("Received job result [result=" + res + ", count=" + cnt + ']');

            int tmp = ++cnt;

            if (tmp <= JOB_COUNT) {
                mapper.send(new TestJob(tmp));

                log.info("Sent test task by continuous mapper (from result() method).");
            }

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            assert results.size() == 10 : "Unexpected result count: " + results.size();

            log.info("Called reduce() method [results=" + results + ']');

            int res = 0;

            for (ComputeJobResult result : results) {
                assert result.getData() != null : "Unexpected result data (null): " + result;

                res += (Integer)result.getData();
            }

            return res;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestJob extends ComputeJobAdapter {
        /** */
        public TestJob() { /* No-op. */ }

        /**
         * @param arg Job argument.
         */
        public TestJob(Integer arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            Integer i = argument(0);

            return i != null ? i : 0;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    @ComputeTaskSessionFullSupport
    public static class SessionChainTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** */
        @TaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** */
        @LoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            ses.addAttributeListener(new ComputeTaskSessionAttributeListener() {
                @Override public void onAttributeSet(Object key, Object val) {
                    if (key instanceof String) {
                        if (((String)key).startsWith("sendJob")) {
                            assert val instanceof Integer;

                            int cnt = (Integer)val;

                            if (cnt < JOB_COUNT) {
                                try {
                                    mapper.send(new SessionChainTestJob(cnt));
                                }
                                catch (IgniteException e) {
                                    log.error("Failed to send new job.", e);
                                }
                            }
                        }
                    }
                }
            }, true);

            Collection<ComputeJob> jobs = new ArrayList<>();

            for (int i = 0; i < JOB_COUNT; i++)
                jobs.add(new SessionChainTestJob(0));

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) {
            assertEquals(JOB_COUNT * JOB_COUNT, results.size());

            return null;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class SessionChainTestJob extends ComputeJobAdapter {
        /** */
        @TaskSessionResource
        private ComputeTaskSession ses;

        /** */
        @JobContextResource
        private ComputeJobContext ctx;

        /** */
        public SessionChainTestJob() { /* No-op. */}

        /**
         * @param arg Job argument.
         */
        public SessionChainTestJob(Integer arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() {
            Integer i = argument(0);

            int arg = i != null ? i : 0;

            ses.setAttribute("sendJob" + ctx.getJobId(), 1 + arg);

            return arg;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class SlowMapTestTask extends ComputeTaskAdapter<Object, Integer> {
        /** */
        @TaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) {
            mapper.send(new TestJob(++cnt));

            try {
                Thread.sleep(10000);
            }
            catch (InterruptedException e) {
                throw new IgniteException("Job has been interrupted.", e);
            }

            mapper.send(new TestJob(++cnt));

            return null;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteException {
            return results == null ? 0 : results.size();
        }
    }
}
