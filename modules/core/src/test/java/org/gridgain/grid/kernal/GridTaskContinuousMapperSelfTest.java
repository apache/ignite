/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * {@link GridComputeTaskContinuousMapper} test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskContinuousMapperSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If test failed.
     */
    public void testContinuousMapperMethods() throws Exception {
        try {
            Ignite ignite = startGrid(0);
            startGrid(1);

            ignite.compute().execute(TestAllMethodsTask.class, null);
        }
        finally {
            stopGrid(0);
            stopGrid(1);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousMapperLifeCycle() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            ignite.compute().execute(TestLifeCycleTask.class, null);
        }
        finally {
            stopGrid(0);
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousMapperNegative() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            ignite.compute().execute(TestNegativeTask.class, null);
        }
        finally {
            stopGrid(0);
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestAllMethodsTask extends GridComputeTaskAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration"})
        @GridTaskContinuousMapperResource private GridComputeTaskContinuousMapper mapper;

        /** */
        private int cnt;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Object arg) throws GridException {
            assert subgrid.size() == 2;

            mapper.send(new TestJob(cnt++), subgrid.get(0));

            Map<GridComputeJob, GridNode> mappedJobs = new HashMap<>(2);

            mappedJobs.put(new TestJob(cnt++), subgrid.get(0));
            mappedJobs.put(new TestJob(cnt++), subgrid.get(1));

            mapper.send(mappedJobs);

            mapper.send(new TestJob(cnt++));

            int size = subgrid.size();

            Collection<GridComputeJob> jobs = new ArrayList<>(size);

            for (GridNode n : subgrid)
                jobs.add(new TestJob(cnt++));

            mapper.send(jobs);

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            assert results.size() == cnt : "Unexpected result count: " + results.size();

            return null;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestLifeCycleTask extends GridComputeTaskAdapter<Object, Object> {
        /** */
        @GridLoggerResource private GridLogger log;

        /** */
        private GridComputeTaskContinuousMapper mapper;

        /**
         * @param mapper Continuous mapper.
         * @throws GridException Thrown if any exception occurs.
         */
        @SuppressWarnings("unused")
        @GridTaskContinuousMapperResource private void setMapper(GridComputeTaskContinuousMapper mapper) throws GridException {
            this.mapper = mapper;

            mapper.send(new TestJob());
        }

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Object arg) throws GridException {
            mapper.send(new TestJob());

            return null;
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res, List<GridComputeJobResult> received) throws GridException {
            GridComputeJobResultPolicy plc = super.result(res, received);

            if (received != null && received.size() == 2)
                mapper.send(new TestJob());

            return plc;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            assert results.size() == 3 : "Unexpected result count: " + results.size();

            GridNode node = results.get(0).getNode();

            try {
                mapper.send(new TestJob(), node);

                assert false;
            }
            catch (GridException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(Collections.singletonMap(new TestJob(), node));

                assert false;
            }
            catch (GridException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(new TestJob());

                assert false;
            }
            catch (GridException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(Collections.singleton(new TestJob()));

                assert false;
            }
            catch (GridException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            return null;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestNegativeTask extends GridComputeTaskAdapter<Object, Object> {
        /** */
        @SuppressWarnings({"UnusedDeclaration"})
        @GridTaskContinuousMapperResource private GridComputeTaskContinuousMapper mapper;

        /** */
        @GridLoggerResource private GridLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, Object arg) throws GridException {
            try {
                mapper.send(new TestJob(), null);

                assert false;

            }
            catch (NullPointerException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(null, subgrid.get(0));

                assert false;
            }
            catch (NullPointerException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send((Map<? extends GridComputeJob, GridNode>)null);

                assert false;
            }
            catch (NullPointerException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(Collections.singletonMap(new TestJob(), (GridNode)null));

                assert false;
            }
            catch (GridException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send((GridComputeJob)null);

                assert false;
            }
            catch (NullPointerException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send((Collection<GridComputeJob>)null);

                assert false;
            }
            catch (NullPointerException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            try {
                mapper.send(Collections.singleton((GridComputeJob)null));

                assert false;
            }
            catch (GridException e) {
                if (log.isInfoEnabled())
                    log.info("Expected exception: " + e);
            }

            mapper.send(new TestJob());

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            assert results.size() == 1;

            return null;
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class TestJob extends GridComputeJobAdapter {
        /** */
        public TestJob() {
            super(-1);
        }

        /**
         * @param idx Index.
         */
        public TestJob(int idx) {
            super(idx);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws GridException {
            return argument(0);
        }
    }
}
