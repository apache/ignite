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
import org.gridgain.grid.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.grid.spi.failover.always.*;
import org.gridgain.testframework.junits.common.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Test failover and topology. It don't pick local node if it has been excluded from topology.
 */
@GridCommonTest(group = "Kernal Self")
public class GridFailoverTopologySelfTest extends GridCommonAbstractTest {
    /** */
    private final AtomicBoolean failed = new AtomicBoolean(false);

    /** */
    public GridFailoverTopologySelfTest() {
        super(/*start Grid*/false);
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setNodeId(null);

        cfg.setFailoverSpi(new GridAlwaysFailoverSpi() {
            /** */
            @GridLocalNodeIdResource private UUID locNodeId;

            /** {@inheritDoc} */
            @Override public ClusterNode failover(GridFailoverContext ctx, List<ClusterNode> grid) {
                if (grid.size() != 1) {
                    failed.set(true);

                    error("Unexpected grid size [expected=1, grid=" + grid + ']');
                }

                for (ClusterNode node : grid) {
                    if (node.id().equals(locNodeId)) {
                        failed.set(true);

                        error("Grid shouldn't contain local node [localNodeId=" + locNodeId + ", grid=" + grid + ']');
                    }
                }

                return super.failover(ctx, grid);
            }
        });

        return cfg;
    }

    /**
     * Tests that failover don't pick local node if it has been excluded from topology.
     *
     * @throws Exception If failed.
     */
    @SuppressWarnings("unchecked")
    public void testFailoverTopology() throws Exception {
        try {
            Ignite ignite1 = startGrid(1);

            startGrid(2);

            ignite1.compute().localDeployTask(JobFailTask.class, JobFailTask.class.getClassLoader());

            try {
                compute(ignite1.cluster().forRemotes()).execute(JobFailTask.class, null);
            }
            catch (GridException e) {
                info("Got expected grid exception: " + e);
            }

            assert !failed.get();
        }
        finally {
            stopGrid(1);
            stopGrid(2);
        }
    }

    /** */
    private static class JobFailTask implements ComputeTask<String, Object> {
         /** */
        @GridLocalNodeIdResource private UUID locNodeId;

        /** */
        private boolean jobFailedOver;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, String arg) throws GridException {
            assert locNodeId != null;

            ClusterNode remoteNode = null;

            for (ClusterNode node : subgrid) {
                if (!node.id().equals(locNodeId))
                    remoteNode = node;
            }

            return Collections.singletonMap(new ComputeJobAdapter(arg) {
                @Override public Serializable execute() throws GridException {
                    throw new GridException("Job exception.");
                }
            }, remoteNode);
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res, List<ComputeJobResult> received) throws GridException {
            if (res.getException() != null && !jobFailedOver) {
                jobFailedOver = true;

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
