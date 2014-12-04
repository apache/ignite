/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Test continuous mapper with siblings.
 */
@GridCommonTest(group = "Kernal Self")
public class GridContinuousJobSiblingsSelfTest extends GridCommonAbstractTest {
    /** */
    private static final int JOB_COUNT = 10;

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousJobSiblings() throws Exception {
        try {
            Ignite ignite = startGrid(0);
            startGrid(1);

            ignite.compute().execute(TestTask.class, null);
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If test failed.
     */
    public void testContinuousJobSiblingsLocalNode() throws Exception {
        try {
            Ignite ignite = startGrid(0);

            compute(ignite.cluster().forLocal()).execute(TestTask.class, null);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    private static class TestTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** */
        @GridTaskContinuousMapperResource
        private ComputeTaskContinuousMapper mapper;

        /** */
        @GridTaskSessionResource
        private ComputeTaskSession ses;

        /** */
        private volatile int jobCnt;

        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws GridException {
            return Collections.singleton(new TestJob(++jobCnt));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received)
            throws GridException {
            if (res.getException() != null)
                throw new GridException("Job resulted in error: " + res, res.getException());

            assert ses.getJobSiblings().size() == jobCnt;

            if (jobCnt < JOB_COUNT) {
                mapper.send(new TestJob(++jobCnt));

                assert ses.getJobSiblings().size() == jobCnt;
            }

            return ComputeJobResultPolicy.WAIT;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            assertEquals(JOB_COUNT, results.size());

            return null;
        }
    }

    /** */
    private static class TestJob extends ComputeJobAdapter {
        /** */
        @GridTaskSessionResource
        private ComputeTaskSession ses;

        /** */
        @GridLoggerResource
        private GridLogger log;

        /**
         * @param sibCnt Siblings count to check.
         */
        TestJob(int sibCnt) {
            super(sibCnt);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws GridException {
            assert ses != null;
            assert argument(0) != null;

            Integer sibCnt = argument(0);

            log.info("Executing job.");

            assert sibCnt != null;

            Collection<ComputeJobSibling> sibs = ses.getJobSiblings();

            assert sibs != null;
            assert sibs.size() == sibCnt : "Unexpected siblings collection [expectedSize=" + sibCnt +
                ", siblingsCnt=" + sibs.size() + ", siblings=" + sibs + ']';

            return null;
        }
    }
}
