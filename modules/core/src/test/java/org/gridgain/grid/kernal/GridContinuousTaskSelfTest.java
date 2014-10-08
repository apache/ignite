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
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
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
            Grid grid = startGrid(0);

            GridComputeTaskFuture<Integer> fut1 = grid.compute().execute(TestJobsChainTask.class, true);
            GridComputeTaskFuture<Integer> fut2 = grid.compute().execute(TestJobsChainTask.class, false);

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
            final Grid grid = startGrid(0);
            startGrid(1);

            GridTestUtils.runMultiThreaded(new Runnable() {
                /** {@inheritDoc} */
                @Override public void run() {
                    GridComputeTaskFuture<Integer> fut1 = grid.compute().execute(TestJobsChainTask.class, true);
                    GridComputeTaskFuture<Integer> fut2 = grid.compute().execute(TestJobsChainTask.class, false);

                    try {
                        assert fut1.get() == 55;
                        assert fut2.get() == 55;
                    }
                    catch (GridException e) {
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
            Grid grid = startGrid(0);
            startGrid(1);

            grid.compute().execute(SessionChainTestTask.class, false).get();
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
            Grid grid = startGrid(0);

            Integer cnt = grid.compute().execute(SlowMapTestTask.class, null).get();

            assert cnt != null;
            assert cnt == 2 : "Unexpected result: " + cnt;
        }
        finally {
            stopGrid(0);
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestJobsChainTask implements GridComputeTask<Boolean, Integer> {
        /** */
        @GridTaskContinuousMapperResource
        private GridComputeTaskContinuousMapper mapper;

        /** */
        @GridLoggerResource
        private GridLogger log;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Boolean arg) throws GridException {
            assert mapper != null;
            assert arg != null;

            GridComputeJob job = new TestJob(++cnt);

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
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> received) throws GridException {
            assert mapper != null;
            assert res.getException() == null : "Unexpected exception: " + res.getException();

            log.info("Received job result [result=" + res + ", count=" + cnt + ']');

            int tmp = ++cnt;

            if (tmp <= JOB_COUNT) {
                mapper.send(new TestJob(tmp));

                log.info("Sent test task by continuous mapper (from result() method).");
            }

            return GridComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
            assert results.size() == 10 : "Unexpected result count: " + results.size();

            log.info("Called reduce() method [results=" + results + ']');

            int res = 0;

            for (GridComputeJobResult result : results) {
                assert result.getData() != null : "Unexpected result data (null): " + result;

                res += (Integer)result.getData();
            }

            return res;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestJob extends GridComputeJobAdapter {
        /** */
        public TestJob() { /* No-op. */ }

        /**
         * @param arg Job argument.
         */
        public TestJob(Integer arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws GridException {
            Integer i = argument(0);

            return i != null ? i : 0;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    @GridComputeTaskSessionFullSupport
    public static class SessionChainTestTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession ses;

        /** */
        @GridTaskContinuousMapperResource
        private GridComputeTaskContinuousMapper mapper;

        /** */
        @GridLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @Override protected Collection<? extends GridComputeJob> split(int gridSize, Object arg) throws GridException {
            ses.addAttributeListener(new GridComputeTaskSessionAttributeListener() {
                @Override public void onAttributeSet(Object key, Object val) {
                    if (key instanceof String) {
                        if (((String)key).startsWith("sendJob")) {
                            assert val instanceof Integer;

                            int cnt = (Integer)val;

                            if (cnt < JOB_COUNT) {
                                try {
                                    mapper.send(new SessionChainTestJob(cnt));
                                }
                                catch (GridException e) {
                                    log.error("Failed to send new job.", e);
                                }
                            }
                        }
                    }
                }
            }, true);

            Collection<GridComputeJob> jobs = new ArrayList<>();

            for (int i = 0; i < JOB_COUNT; i++)
                jobs.add(new SessionChainTestJob(0));

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            assertEquals(JOB_COUNT * JOB_COUNT, results.size());

            return null;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class SessionChainTestJob extends GridComputeJobAdapter {
        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession ses;

        /** */
        @GridJobContextResource
        private GridComputeJobContext ctx;

        /** */
        public SessionChainTestJob() { /* No-op. */}

        /**
         * @param arg Job argument.
         */
        public SessionChainTestJob(Integer arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws GridException {
            Integer i = argument(0);

            int arg = i != null ? i : 0;

            ses.setAttribute("sendJob" + ctx.getJobId(), 1 + arg);

            return arg;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class SlowMapTestTask extends GridComputeTaskAdapter<Object, Integer> {
        /** */
        @GridTaskContinuousMapperResource
        private GridComputeTaskContinuousMapper mapper;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Object arg) throws GridException {
            mapper.send(new TestJob(++cnt));

            try {
                Thread.sleep(10000);
            }
            catch (InterruptedException e) {
                throw new GridException("Job has been interrupted.", e);
            }

            mapper.send(new TestJob(++cnt));

            return null;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<GridComputeJobResult> results) throws GridException {
            return results == null ? 0 : results.size();
        }
    }
}
