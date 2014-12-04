/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.session;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Grid session set job attribute self test.
 */
@GridCommonTest(group = "Task Session")
public class GridSessionSetJobAttributeOrderSelfTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_ATTR_KEY = "grid.task.session.test.attr";

    /** */
    private static final int SETS_ATTR_COUNT = 100;

    /** */
    private static final int TESTS_COUNT = 10;

    /**
     * @throws Exception If failed.
     */
    public void testJobSetAttribute() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ignite1.compute().localDeployTask(SessionTestTask.class, SessionTestTask.class.getClassLoader());

            GridCompute comp = ignite1.compute().enableAsync();

            for (int i = 0; i < TESTS_COUNT; i++) {
                comp.withTimeout(100000).execute(SessionTestTask.class.getName(), ignite2.cluster().localNode().id());

                GridComputeTaskFuture<?> fut = comp.future();

                fut.getTaskSession().setAttribute(TEST_ATTR_KEY, SETS_ATTR_COUNT);

                Integer res = (Integer)fut.get();

                assert res != null && res.equals(SETS_ATTR_COUNT) : "Unexpected result [res=" + res +
                    ", expected=" + SETS_ATTR_COUNT + ']';

                info("Session attribute value was correct for test [res=" + res +
                    ", expected=" + SETS_ATTR_COUNT + ']');
            }
        }
        finally {
            stopAllGrids(false);
        }
    }

    /** */
    @GridComputeTaskSessionFullSupport
    private static class SessionTestTask extends GridComputeTaskAdapter<UUID, Serializable> {
        /** */
        @GridTaskSessionResource private GridComputeTaskSession taskSes;

        /** */
        @GridLoggerResource private GridLogger log;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, ClusterNode> map(List<ClusterNode> subgrid, UUID arg) throws GridException {
            assert subgrid.size() == 2;
            assert arg != null;

            for (ClusterNode node : subgrid) {
                if (node.id().equals(arg))
                    return Collections.singletonMap(new SessionTestJob(), node);
            }

            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Serializable reduce(List<GridComputeJobResult> results) throws GridException {
            try {
                if (taskSes.waitForAttribute(TEST_ATTR_KEY, SETS_ATTR_COUNT, 20000)) {
                    log.info("Successfully waited for attribute [key=" + TEST_ATTR_KEY +
                        ", val=" + SETS_ATTR_COUNT + ']');
                }
            }
            catch (InterruptedException e) {
                throw new GridException("Got interrupted while waiting for attribute to be set.", e);
            }

            return taskSes.getAttribute(TEST_ATTR_KEY);
        }
    }

    /** */
    private static class SessionTestJob extends GridComputeJobAdapter {
        /** */
        @GridTaskSessionResource
        private GridComputeTaskSession taskSes;

        /** */
        @GridLoggerResource
        private GridLogger log;

        /** {@inheritDoc} */
        @Override public Serializable execute() throws GridException {
            assert taskSes != null;

            try {
                boolean attr = taskSes.waitForAttribute(TEST_ATTR_KEY, SETS_ATTR_COUNT, 20000);

                assert attr : "Failed to wait for attribute value.";
            }
            catch (InterruptedException e) {
                throw new GridException("Got interrupted while waiting for attribute to be set.", e);
            }

            Integer res = taskSes.getAttribute(TEST_ATTR_KEY);

            assert res != null && res.equals(SETS_ATTR_COUNT) :
                "Unexpected result [res=" + res + ", expected=" + SETS_ATTR_COUNT + ']';

            log.info("Session attribute order was correct for job [res=" + res + ", expected=" + SETS_ATTR_COUNT + ']');

            taskSes.setAttribute(TEST_ATTR_KEY, SETS_ATTR_COUNT);

            return null;
        }
    }
}
