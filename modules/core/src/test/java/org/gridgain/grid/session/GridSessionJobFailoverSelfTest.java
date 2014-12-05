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
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.apache.ignite.spi.failover.always.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Job failover test.
 */
@GridCommonTest(group = "Task Session")
public class GridSessionJobFailoverSelfTest extends GridCommonAbstractTest {
    /**
     * Default constructor.
     */
    public GridSessionJobFailoverSelfTest() {
        super(/*start Grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailoverSpi(new AlwaysFailoverSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testFailoverJobSession() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);

            startGrid(2);

            ignite1.compute().localDeployTask(SessionTestTask.class, SessionTestTask.class.getClassLoader());

            Object res = ignite1.compute().execute(SessionTestTask.class.getName(), "1");

            assert (Integer)res == 1;
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     * Session test task implementation.
     */
    @ComputeTaskSessionFullSupport
    private static class SessionTestTask implements ComputeTask<String, Object> {
        /** */
        @IgniteTaskSessionResource
        private ComputeTaskSession ses;

        /** */
        private boolean jobFailed;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) throws GridException {
            ses.setAttribute("fail", true);

            for (int i = 0; i < 10; i++) {
                for (int ii = 0; ii < 10; ii++)
                    ses.setAttribute("test.task.attr." + i, ii);
            }

            return Collections.singletonMap(new ComputeJobAdapter(arg) {
                /** */
                @IgniteLocalNodeIdResource
                private UUID locNodeId;

                @Override public Serializable execute() throws GridException {
                    boolean fail;

                    try {
                        fail = ses.waitForAttribute("fail", 0);
                    }
                    catch (InterruptedException e) {
                        throw new GridException("Got interrupted while waiting for attribute to be set.", e);
                    }

                    if (fail) {
                        ses.setAttribute("fail", false);

                        for (int i = 0; i < 10; i++) {
                            for (int ii = 0; ii < 10; ii++)
                                ses.setAttribute("test.job.attr." + i, ii);
                        }

                        throw new GridException("Job exception.");
                    }

                    try {
                        for (int i = 0; i < 10; i++) {
                            boolean attr = ses.waitForAttribute("test.task.attr." + i, 9, 100000);

                            assert attr;
                        }

                        for (int i = 0; i < 10; i++) {
                            boolean attr = ses.waitForAttribute("test.job.attr." + i, 9, 100000);

                            assert attr;
                        }
                    }
                    catch (InterruptedException e) {
                        throw new GridException("Got interrupted while waiting for attribute to be set.", e);
                    }

                    // This job does not return any result.
                    return Integer.parseInt(this.<String>argument(0));
                }
            }, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received)
            throws GridException {
            if (res.getException() != null) {
                assert !jobFailed;

                jobFailed = true;

                return ComputeJobResultPolicy.FAILOVER;
            }

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            assert results.size() == 1;

            return results.get(0).getData();
        }
    }
}
