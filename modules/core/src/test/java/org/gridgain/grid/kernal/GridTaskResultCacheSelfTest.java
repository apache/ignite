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
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;

/**
 *
 */
@GridCommonTest(group = "Kernal Self")
public class GridTaskResultCacheSelfTest extends GridCommonAbstractTest {
    /**
     *
     */
    public GridTaskResultCacheSelfTest() {
        super(true);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoCacheResults() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().execute(GridResultNoCacheTestTask.class, "Grid Result No Cache Test Argument");
    }

    /**
     * @throws Exception If failed.
     */
    public void testCacheResults() throws Exception {
        Ignite ignite = G.grid(getTestGridName());

        ignite.compute().execute(GridResultCacheTestTask.class, "Grid Result Cache Test Argument");
    }

    /**
     *
     */
    @ComputeTaskNoResultCache
    private static class GridResultNoCacheTestTask extends GridAbstractCacheTestTask {
        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd) throws GridException {
            assert res.getData() != null;
            assert rcvd.isEmpty();

            return super.result(res, rcvd);
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            assert results.isEmpty();

            return null;
        }
    }

    /**
     *
     */
    private static class GridResultCacheTestTask extends GridAbstractCacheTestTask {
        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> rcvd)
            throws GridException {
            assert res.getData() != null;
            assert rcvd.contains(res);

            for (ComputeJobResult jobRes : rcvd)
                assert jobRes.getData() != null;

            return super.result(res, rcvd);
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            for (ComputeJobResult res : results) {
                if (res.getException() != null)
                    throw res.getException();

                assert res.getData() != null;
            }

            return null;
        }
    }

    /**
     * Test task.
     */
    private abstract static class GridAbstractCacheTestTask extends ComputeTaskSplitAdapter<String, Object> {
        /** {@inheritDoc} */
        @Override protected Collection<? extends ComputeJob> split(int gridSize, String arg) throws GridException {
            String[] words = arg.split(" ");

            Collection<ComputeJobAdapter> jobs = new ArrayList<>(words.length);

            for (String word : words) {
                jobs.add(new ComputeJobAdapter(word) {
                    @Override public Serializable execute() {
                        return argument(0);
                    }
                });
            }

            return jobs;
        }
    }
}
