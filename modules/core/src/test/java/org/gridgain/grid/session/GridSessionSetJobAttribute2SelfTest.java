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
import org.apache.ignite.compute.*;
import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Job attribute test.
 */
@GridCommonTest(group = "Task Session")
public class GridSessionSetJobAttribute2SelfTest extends GridCommonAbstractTest {
    /** */
    private static final String TEST_ATTR_KEY = "grid.tasksession.test.attr";

    /** */
    public GridSessionSetJobAttribute2SelfTest() {
        super(/*start Grid*/false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testJobSetAttribute() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            ignite1.compute().localDeployTask(SessionTestTask.class, SessionTestTask.class.getClassLoader());

            GridComputeTaskFuture<?> fut =
                executeAsync(ignite1.compute(), SessionTestTask.class.getName(), ignite2.cluster().localNode().id());

            fut.get();
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     *
     */
    @GridComputeTaskSessionFullSupport
    private static class SessionTestTask extends ComputeTaskAdapter<UUID, Object> {
        /** */
        @GridTaskSessionResource private GridComputeTaskSession taskSes;

        /** */
        private UUID attrVal;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, UUID arg) throws GridException {
            assert subgrid.size() == 2;
            assert arg != null;

            attrVal = UUID.randomUUID();

            for (ClusterNode node : subgrid) {
                if (node.id().equals(arg))
                    return Collections.singletonMap(new SessionTestJob(attrVal), node);
            }

            assert false;

            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            try {
                Thread.sleep(100);
            }
            catch (InterruptedException e) {
                throw new GridException("Got interrupted while while sleeping.", e);
            }

            Serializable ser = taskSes.getAttribute(TEST_ATTR_KEY);

            assert ser != null;

            assert attrVal.equals(ser);

            return null;
        }
    }

    /** */
    private static class SessionTestJob extends ComputeJobAdapter {
        /** */
        @GridTaskSessionResource private GridComputeTaskSession taskSes;

        /**
         * @param arg Argument.
         */
        private SessionTestJob(UUID arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public Serializable execute() throws GridException {
            assert taskSes != null;
            assert argument(0) != null;

            taskSes.setAttribute(TEST_ATTR_KEY, argument(0));

            return argument(0);
        }
    }
}
