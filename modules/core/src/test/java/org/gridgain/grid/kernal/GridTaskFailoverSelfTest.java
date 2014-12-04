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
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Test for task failover.
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskFailoverSelfTest extends GridCommonAbstractTest {
    /** Don't change it value. */
    public static final int SPLIT_COUNT = 2;

    /** */
    public GridTaskFailoverSelfTest() {
        super(false);
    }

    /**
     * @throws Exception If test failed.
     */
    @SuppressWarnings("unchecked")
    public void testFailover() throws Exception {
        Ignite ignite = startGrid();

        try {
            ignite.compute().localDeployTask(GridFailoverTestTask.class, GridFailoverTestTask.class.getClassLoader());

            ComputeTaskFuture<?> fut = ignite.compute().execute(GridFailoverTestTask.class.getName(), null);

            assert fut != null;

            fut.get();

            assert false : "Should never be reached due to exception thrown.";
        }
        catch (GridTopologyException e) {
            info("Received correct exception: " + e);
        }
        finally {
            stopGrid();
        }
    }

    /** */
    @SuppressWarnings({"PublicInnerClass"})
    public static class GridFailoverTestTask extends ComputeTaskSplitAdapter<Serializable, Integer> {
        /** */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Collection<ComputeJobAdapter> split(int gridSize, Serializable arg) {
            if (log.isInfoEnabled())
                log.info("Splitting job [job=" + this + ", gridSize=" + gridSize + ", arg=" + arg + ']');

            Collection<ComputeJobAdapter> jobs = new ArrayList<>(SPLIT_COUNT);

            for (int i = 0; i < SPLIT_COUNT; i++)
                jobs.add(new ComputeJobAdapter() {
                    @Override public Serializable execute() {
                        if (log.isInfoEnabled())
                            log.info("Computing job [job=" + this + ']');

                        return null;
                    }
                });

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) throws
            GridException {
            if (res.getException() != null)
                throw res.getException();

            return ComputeJobResultPolicy.FAILOVER;
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            if (log.isInfoEnabled())
                log.info("Reducing job [job=" + this + ", results=" + results + ']');

            int res = 0;

            for (ComputeJobResult result : results)
                res += (Integer)result.getData();

            return res;
        }
    }
}
