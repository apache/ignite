/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.*;
import org.apache.ignite.spi.checkpoint.sharedfs.*;
import org.apache.ignite.spi.failover.*;
import org.apache.ignite.spi.failover.always.*;
import org.apache.ignite.spi.loadbalancing.roundrobin.*;
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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

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
            catch (IgniteCheckedException e) {
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
    private class GridTestFailoverSpi extends AlwaysFailoverSpi {
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
        @Override public ClusterNode failover(FailoverContext ctx, List<ClusterNode> grid) {
            if (getName().equals(expName))
                isTaskFailoverCalled = true;
            else
                isWrongTaskFailoverCalled = true;

            return super.failover(ctx, grid);
        }
    }

    /** */
    private class GridTestLoadBalancingSpi extends RoundRobinLoadBalancingSpi {
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
        @Override public ClusterNode getBalancedNode(ComputeTaskSession ses, List<ClusterNode> top,
            ComputeJob job) throws IgniteCheckedException {
            if (getName().equals(expName))
                isTaskLoadBalancingCalled = true;
            else
                isWrongTaskLoadBalancingCalled = true;

            return super.getBalancedNode(ses, top, job);
        }
    }

    /** */
    private class GridTestCheckpointSpi extends SharedFsCheckpointSpi {
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
            boolean overwrite) throws IgniteSpiException {
            if (getName().equals(expName))
                isTaskCheckPntCalled = true;
            else
                isWrongTaskCheckPntCalled = true;

            return super.saveCheckpoint(key, state, timeout, overwrite);
        }

        /** {@inheritDoc} */
        @Override public byte[] loadCheckpoint(String key) throws IgniteSpiException {
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
    @ComputeTaskSpis(loadBalancingSpi = "load2", failoverSpi = "fail2", checkpointSpi = "cp2")
    @ComputeTaskSessionFullSupport
    public static final class GridTestMultipleSpisTask extends ComputeTaskAdapter<UUID, Integer> {
        /** */
        @IgniteTaskSessionResource
        private ComputeTaskSession taskSes;

        /** */
        @IgniteInstanceResource
        private Ignite ignite;

        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, UUID arg) throws IgniteCheckedException {
            assert subgrid.size() == 2;
            assert taskSes != null;
            assert ignite != null;
            assert ignite.cluster().localNode().id().equals(arg);

            taskSes.saveCheckpoint("test", arg);

            // Always map job to the local node where it will fail.
            return Collections.singletonMap(new GridTestMultipleSpisJob(arg), ignite.cluster().localNode());
        }

        /** {@inheritDoc} */
        @Override public ComputeJobResultPolicy result(ComputeJobResult res,
            List<ComputeJobResult> received) throws IgniteCheckedException {
            if (res.getException() != null)
                return ComputeJobResultPolicy.FAILOVER;

            return super.result(res, received);
        }

        /** {@inheritDoc} */
        @Override public Integer reduce(List<ComputeJobResult> results) {
            return null;
        }
    }

    /**
     * Job that always throws exception.
     */
    private static class GridTestMultipleSpisJob extends ComputeJobAdapter {
        /** Local node ID. */
        @IgniteLocalNodeIdResource
        private UUID locId;

        /** */
        @IgniteTaskSessionResource
        private ComputeTaskSession jobSes;

        /**
         * @param arg Job argument.
         */
        GridTestMultipleSpisJob(UUID arg) {
            super(arg);
        }

        /** {@inheritDoc} */
        @Override public UUID execute() throws IgniteCheckedException {
            assert locId != null;
            assert jobSes != null;
            assert argument(0) != null;

            // Should always fail on task originating node and work on another one.
            if (locId.equals(argument(0)))
                throw new IgniteCheckedException("Expected exception to failover job.");

            // Use checkpoint on job side. This will happen on remote node.
            jobSes.loadCheckpoint("test");

            return argument(0);
        }
    }
}
