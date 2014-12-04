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
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.failover.always.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Failover tests.
 */
@GridCommonTest(group = "Kernal Self")
public class GridFailoverSelfTest extends GridCommonAbstractTest {
    /** Initial node that job has been mapped to. */
    private static final AtomicReference<ClusterNode> nodeRef = new AtomicReference<>(null);

    /** */
    public GridFailoverSelfTest() {
        super(/*start Grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setFailoverSpi(new GridAlwaysFailoverSpi());

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testJobFail() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);
            Ignite ignite2 = startGrid(2);

            assert ignite1 != null;
            assert ignite2 != null;

            Integer res = ignite1.compute().withTimeout(10000).execute(JobFailTask.class.getName(), "1");

            assert res != null;
            assert res == 1;
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /**
     *
     */
    @ComputeTaskSessionFullSupport
    private static class JobFailTask implements ComputeTask<String, Object> {
        /** */
        @GridTaskSessionResource
        private ComputeTaskSession ses;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) throws GridException {
            ses.setAttribute("fail", true);

            nodeRef.set(subgrid.get(0));

            return Collections.singletonMap(new ComputeJobAdapter(arg) {
                /** Local node ID. */
                @GridLocalNodeIdResource
                private UUID locId;

                /** {@inheritDoc} */
                @Override public Serializable execute() throws GridException {
                    boolean fail;

                    try {
                        fail = ses.<String, Boolean>waitForAttribute("fail", 0);
                    }
                    catch (InterruptedException e) {
                        throw new GridException("Got interrupted while waiting for attribute to be set.", e);
                    }

                    if (fail) {
                        ses.setAttribute("fail", false);

                        assert nodeRef.get().id().equals(locId);

                        throw new GridException("Job exception.");
                    }

                    assert !nodeRef.get().id().equals(locId);

                    // This job does not return any result.
                    return Integer.parseInt(this.<String>argument(0));
                }
            }, subgrid.get(0));
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> received) throws GridException {
            if (res.getException() != null && !(res.getException() instanceof ComputeUserUndeclaredException)) {
                assert res.getNode().id().equals(nodeRef.get().id());

                return ComputeJobResultPolicy.FAILOVER;
            }

            assert !res.getNode().id().equals(nodeRef.get().id());

            return ComputeJobResultPolicy.REDUCE;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            assert results.size() == 1;

            assert nodeRef.get() != null;

            assert !results.get(0).getNode().id().equals(nodeRef.get().id()) :
                "Initial node and result one are the same (should be different).";

            return results.get(0).getData();
        }
    }
}
