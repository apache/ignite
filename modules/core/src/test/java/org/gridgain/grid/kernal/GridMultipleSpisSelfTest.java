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
import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.checkpoint.sharedfs.*;
import org.gridgain.grid.spi.failover.*;
import org.gridgain.grid.spi.failover.always.*;
import org.gridgain.grid.spi.loadbalancing.roundrobin.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Multiple SPIs test.
 */
@GridCommonTest(group = "Kernal Self")
public class GridMultipleSpisSelfTest extends GridCommonAbstractTest {
    /** */
    private boolean isTaskFailoverCalled;

    /** */
    private boolean isWrongTaskFailoverCalled;

    /** */
    private boolean isTaskLoadBalancingCalled;

    /** */
    private boolean isWrongTaskLoadBalancingCalled;

    /** */
    private boolean isTaskCheckPntCalled;

    /** */
    private boolean isWrongTaskCheckPntCalled;

    /** */
    private boolean isJobCheckPntCalled;

    /** */
    private boolean isWrongJobCheckPntCalled;

    /** */
    public GridMultipleSpisSelfTest() {
        super(false);
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        GridTestFailoverSpi fail1 = new GridTestFailoverSpi("fail2");
        GridTestFailoverSpi fail2 = new GridTestFailoverSpi("fail2");

        fail1.setName("fail1");
        fail2.setName("fail2");

        GridTestLoadBalancingSpi load1 = new GridTestLoadBalancingSpi("load2");
        GridTestLoadBalancingSpi load2 = new GridTestLoadBalancingSpi("load2");

        load1.setName("load1");
        load2.setName("load2");

        GridTestCheckpointSpi cp1 = new GridTestCheckpointSpi("cp2");
        GridTestCheckpointSpi cp2 = new GridTestCheckpointSpi("cp2");

        cp1.setName("cp1");
        cp2.setName("cp2");

        cfg.setFailoverSpi(fail1, fail2);
        cfg.setLoadBalancingSpi(load1, load2);
        cfg.setCheckpointSpi(cp1, cp2);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings({"UnusedCatchParameter"})
    public void testFailoverTask() throws Exception {
        // Start local and remote grids.
        Ignite ignite1 = startGrid(1);
        startGrid(2);

        try {
            // Say grid1 is a local one. Deploy task and execute it.
            ignite1.compute().localDeployTask(GridTestMultipleSpisTask.class,
                GridTestMultipleSpisTask.class.getClassLoader());

            try {
                ignite1.compute().execute(GridTestMultipleSpisTask.class.getName(), ignite1.cluster().localNode().id());
            }
            catch (GridException e) {
                e.printStackTrace();

                assert false : "Unexpected exception.";
            }
        }
        finally {
            stopGrid(2);
            stopGrid(1);
        }

        assert isTaskFailoverCalled : "Expected Failover SPI has not been called.";
        assert isTaskLoadBalancingCalled : "Expected Load balancing SPI has not been called.";
        assert isTaskCheckPntCalled : "Expected Checkpoint SPI has not been called on task side.";
        assert isJobCheckPntCalled : "Expected Checkpoint SPI has not been called on job side.";

        // All of them should remain false.
        assert !isWrongTaskFailoverCalled : "Unexpected Failover SPI has been called.";
        assert !isWrongTaskLoadBalancingCalled : "Unexpected Load balancing SPI has been called.";
        assert !isWrongTaskCheckPntCalled : "Unexpected Checkpoint SPI has been called on task side.";
        assert !isWrongJobCheckPntCalled : "Unexpected Checkpoint SPI has been called on job side.";
    }

    /** */
    private class GridTestFailoverSpi extends GridAlwaysFailoverSpi {
        /** */
        private String expName;

        /**
         * Creates new failover SPI.
         *
         * @param expName Name of the SPI expected to be called.
         */
        GridTestFailoverSpi(String expName) {
            this.expName = expName;
        }

        /** {@inheritDoc} */
        @Override public ClusterNode failover(GridFailoverContext ctx, List<ClusterNode> grid) {
            if (getName().equals(expName))
                isTaskFailoverCalled = true;
            else
                isWrongTaskFailoverCalled = true;

            return super.failover(ctx, grid);
        }
    }

    /** */
    private class GridTestLoadBalancingSpi extends GridRoundRobinLoadBalancingSpi {
        /** */
        private String expName;

        /**
         * Creates new load balancing SPI.
         *
         * @param expName Name of the SPI expected to be called.
         */
        GridTestLoadBalancingSpi(String expName) {
            this.expName = expName;
        }

        /** {@inheritDoc} */
        @Override public ClusterNode getBalancedNode(GridComputeTaskSession ses, List<ClusterNode> top,
            GridComputeJob job) throws GridException {
            if (getName().equals(expName))
                isTaskLoadBalancingCalled = true;
            else
                isWrongTaskLoadBalancingCalled = true;

            return super.getBalancedNode(ses, top, job);
        }
    }

    /** */
    private class GridTestCheckpointSpi extends GridSharedFsCheckpointSpi {
        /** */
        private String expName;

        /**
         * Creates new checkpoint SPI.
         *
         * @param expName Name of the SPI expected to be called.
         */
        GridTestCheckpointSpi(String expName) {
            this.expName = expName;
        }

        /** {@inheritDoc} */
        @Override public boolean saveCheckpoint(String key, byte[] state, long timeout,
            boolean overwrite) throws GridSpiException {
            if (getName().equals(expName))
                isTaskCheckPntCalled = true;
            else
                isWrongTaskCheckPntCalled = true;

            return super.saveCheckpoint(key, state, timeout, overwrite);
        }

        /** {@inheritDoc} */
        @Override public byte[] loadCheckpoint(String key) throws GridSpiException {
            if (getName().equals(expName))
                isJobCheckPntCalled = true;
            else
                isWrongJobCheckPntCalled = true;

            return super.loadCheckpoint(key);
        }
    }

    /**
     * Task which splits to the jobs that uses SPIs from annotation.
     */
    @SuppressWarnings({"PublicInnerClass"})
    @GridComputeTaskSpis(loadBalancingSpi = "load2", failoverSpi = "fail2", checkpointSpi = "cp2")
    @GridComputeTaskSessionFullSupport
    public static final class GridTestMultipleSpisTask extends GridComputeTaskAdapter<UUID, Integer> {
        /** */
        @GridTaskSessionResource private GridComputeTaskSession taskSes;

        /** */
        @GridInstanceResource private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, ClusterNode> map(List<ClusterNode> subgrid, UUID arg) throws GridException {
            assert subgrid.size() == 2;
            assert taskSes != null;
            assert ignite != null;
            assert ignite.cluster().localNode().id().equals(arg);

            taskSes.saveCheckpoint("test", arg);

            // Always map job to the local node where it will fail.
            return Collections.singletonMap(new GridTestMultipleSpisJob(arg), ignite.cluster().localNode());
        }

        /** {@inheritDoc} */
        @Override public GridComputeJobResultPolicy result(GridComputeJobResult res,
            List<GridComputeJobResult> received) throws GridException {
            if (res.getException() != null)
                return GridComputeJobResultPolicy.FAILOVER;

            return super.result(res, received);
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<GridComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Job that always throws exception.
     */
    private static class GridTestMultipleSpisJob extends GridComputeJobAdapter {
        /** Local node ID. */
        @GridLocalNodeIdResource private UUID locId;

        /** */
        @GridTaskSessionResource private GridComputeTaskSession jobSes;

        /**
         * @param arg Job argument.
         */
        GridTestMultipleSpisJob(UUID arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public UUID execute() throws GridException {
            assert locId != null;
            assert jobSes != null;
            assert argument(0) != null;

            // Should always fail on task originating node and work on another one.
            if (locId.equals(argument(0)))
                throw new GridException("Expected exception to failover job.");

            // Use checkpoint on job side. This will happen on remote node.
            jobSes.loadCheckpoint("test");

            return argument(0);
        }
    }
}
