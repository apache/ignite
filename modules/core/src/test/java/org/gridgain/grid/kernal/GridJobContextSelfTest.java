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
import org.gridgain.grid.resources.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 * Job context test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridJobContextSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If anything failed.
     */
    public void testJobContext() throws Exception {
        Ignite ignite = startGrid(1);

        try {
            startGrid(2);

            try {
                ignite.compute().execute(JobContextTask.class, null);
            }
            finally {
                stopGrid(2);
            }
        }
        finally{
            stopGrid(1);
        }
    }

    /** */
    @SuppressWarnings("PublicInnerClass")
    public static class JobContextTask extends GridComputeTaskSplitAdapter<Object, Object> {
        @Override protected Collection<? extends ComputeJob> split(int gridSize, Object arg) throws GridException {
            Collection<ComputeJobAdapter> jobs = new ArrayList<>(gridSize);

            for (int i = 0; i < gridSize; i++) {
                jobs.add(new ComputeJobAdapter() {
                    /** */
                    @GridJobContextResource
                    private GridComputeJobContext jobCtx;

                    /** */
                    @GridLocalNodeIdResource
                    private UUID locNodeId;

                    /** {@inheritDoc} */
                    @Override public Serializable execute() {
                        jobCtx.setAttribute("nodeId", locNodeId);
                        jobCtx.setAttribute("jobId", jobCtx.getJobId());

                        Map<String, String> attrs = new HashMap<>(10);

                        for (int i = 0; i < 10; i++) {
                            String s = jobCtx.getJobId().toString() + i;

                            attrs.put(s, s);
                        }

                        jobCtx.setAttributes(attrs);

                        assert jobCtx.getAttribute("nodeId").equals(locNodeId);
                        assert jobCtx.getAttributes().get("nodeId").equals(locNodeId);
                        assert jobCtx.getAttributes().keySet().containsAll(attrs.keySet());
                        assert jobCtx.getAttributes().values().containsAll(attrs.values());

                        return null;
                    }
                });
            }

            return jobs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            for (GridComputeJobResult res : results) {
                GridComputeJobContext jobCtx = res.getJobContext();

                assert jobCtx.getAttribute("nodeId").equals(res.getNode().id());
                assert jobCtx.getAttributes().get("nodeId").equals(res.getNode().id());

                assert jobCtx.getAttribute("jobId").equals(jobCtx.getJobId());

                for (int i = 0; i < 10; i++) {
                    String s = jobCtx.getJobId().toString() + i;

                    assert jobCtx.getAttribute(s).equals(s);
                    assert jobCtx.getAttributes().get(s).equals(s);
                }
            }

            return null;
        }
    }
}
