/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

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

            IgniteCompute comp = ignite.compute().enableAsync();

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
                        IgniteCompute comp = ignite.compute().enableAsync();

                        comp.execute(TestJobsChainTask.class, true);

                        ComputeTaskFuture<Integer> fut1 = comp.future();

                        comp.execute(TestJobsChainTask.class, false);

                        ComputeTaskFuture<Integer> fut2 = comp.future();

                        assert fut1.get() == 55;
                        assert fut2.get() == 55;
                    }
                    catch (IgniteCheckedException e) {
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

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestJobsChainTask implements ComputeTask<Boolean, Integer> {
        /** */
        @IgniteTaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Boolean arg) throws IgniteCheckedException {
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
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) throws IgniteCheckedException {
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
        @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
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
        @Override public Serializable execute() throws IgniteCheckedException {
            Integer i = argument(0);

            return i != null ? i : 0;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    @ComputeTaskSessionFullSupport
    public static class SessionChainTestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** */
        @IgniteTaskSessionResource
        private ComputeTaskSession ses;

        /** */
        @IgniteTaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws IgniteCheckedException {
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
                                catch (IgniteCheckedException e) {
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
        @Override public Object reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            assertEquals(JOB_COUNT * JOB_COUNT, results.size());

            return null;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class SessionChainTestJob extends ComputeJobAdapter {
        /** */
        @IgniteTaskSessionResource
        private ComputeTaskSession ses;

        /** */
        @IgniteJobContextResource
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
        @Override public Serializable execute() throws IgniteCheckedException {
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
        @IgniteTaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, Object arg) throws IgniteCheckedException {
            mapper.send(new TestJob(++cnt));

            try {
                Thread.sleep(10000);
            }
            catch (InterruptedException e) {
                throw new IgniteCheckedException("Job has been interrupted.", e);
            }

            mapper.send(new TestJob(++cnt));

            return null;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) throws IgniteCheckedException {
            return results == null ? 0 : results.size();
        }
    }
}
