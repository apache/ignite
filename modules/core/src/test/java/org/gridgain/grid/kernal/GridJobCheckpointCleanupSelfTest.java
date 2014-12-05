package org.gridgain.grid.kernal;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.compute.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.resources.*;
import org.apache.ignite.spi.*;
import org.gridgain.grid.*;
import org.gridgain.grid.spi.checkpoint.*;
import org.gridgain.testframework.junits.common.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

/**
 * Test for checkpoint cleanup.
 */
public class GridJobCheckpointCleanupSelfTest extends GridCommonAbstractTest {
    /** Number of currently alive checkpoints. */
    private final AtomicInteger cntr = new AtomicInteger();

    /** Checkpoint. */
    private CheckpointSpi checkpointSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration c = super.getConfiguration(gridName);

        c.setCheckpointSpi(checkpointSpi);

        return c;
    }

    /**
     * Spawns one job on the node other than task node and
     * ensures that all checkpoints were removed after task completion.
     *
     * @throws Exception if failed.
     */
    public void testCheckpointCleanup() throws Exception {
        try {
            checkpointSpi = new TestCheckpointSpi("task-checkpoints", cntr);

            Ignite taskIgnite = startGrid(0);

            checkpointSpi = new TestCheckpointSpi("job-checkpoints", cntr);

            Ignite jobIgnite = startGrid(1);

            taskIgnite.compute().execute(new CheckpointCountingTestTask(), jobIgnite.cluster().localNode());
        }
        finally {
            stopAllGrids();
        }

        assertEquals(cntr.get(), 0);
    }

    /**
     * Test checkpoint SPI.
     */
    @IgniteSpiMultipleInstancesSupport(true)
    private static class TestCheckpointSpi extends IgniteSpiAdapter implements CheckpointSpi {
        /** Counter. */
        private final AtomicInteger cntr;

        /**
         * @param name Name.
         * @param cntr Counter.
         */
        TestCheckpointSpi(String name, AtomicInteger cntr) {
            setName(name);

            this.cntr = cntr;
        }

        /** {@inheritDoc} */
        @Override public byte[] loadCheckpoint(String key) throws IgniteSpiException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean saveCheckpoint(String key, byte[] state, long timeout, boolean overwrite)
            throws IgniteSpiException {
            cntr.incrementAndGet();

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean removeCheckpoint(String key) {
            cntr.decrementAndGet();

            return true;
        }

        /** {@inheritDoc} */
        @Override public void setCheckpointListener(CheckpointListener lsnr) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws IgniteSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws IgniteSpiException {
            // No-op.
        }
    }

    /**
     *
     */
    @ComputeTaskSessionFullSupport
    private static class CheckpointCountingTestTask extends ComputeTaskAdapter<ClusterNode, Object> {
        /** {@inheritDoc} */
        @Override public Map<? extends ComputeJob, ClusterNode> map(List<ClusterNode> subgrid, @Nullable ClusterNode arg)
            throws GridException {
            for (ClusterNode node : subgrid) {
                if (node.id().equals(arg.id()))
                    return Collections.singletonMap(new ComputeJobAdapter() {
                        @IgniteTaskSessionResource
                        private ComputeTaskSession ses;

                        @Nullable @Override public Object execute() throws GridException {
                            ses.saveCheckpoint("checkpoint-key", "checkpoint-value");

                            return null;
                        }
                    }, node);
            }

            assert false : "Expected node wasn't found in grid";

            // Never accessible.
            return null;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            return null;
        }
    }
}
