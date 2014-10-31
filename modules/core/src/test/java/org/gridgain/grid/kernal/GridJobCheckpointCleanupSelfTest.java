package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.spi.*;
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
    private GridCheckpointSpi checkpointSpi;

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration c = super.getConfiguration(gridName);

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

            Grid taskGrid = startGrid(0);

            checkpointSpi = new TestCheckpointSpi("job-checkpoints", cntr);

            Grid jobGrid = startGrid(1);

            taskGrid.compute().execute(new CheckpointCountingTestTask(), jobGrid.cluster().localNode());
        }
        finally {
            stopAllGrids();
        }

        assertEquals(cntr.get(), 0);
    }

    /**
     * Test checkpoint SPI.
     */
    @GridSpiMultipleInstancesSupport(true)
    private static class TestCheckpointSpi extends GridSpiAdapter implements GridCheckpointSpi {
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
        @Override public byte[] loadCheckpoint(String key) throws GridSpiException {
            return null;
        }

        /** {@inheritDoc} */
        @Override public boolean saveCheckpoint(String key, byte[] state, long timeout, boolean overwrite)
            throws GridSpiException {
            cntr.incrementAndGet();

            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean removeCheckpoint(String key) {
            cntr.decrementAndGet();

            return true;
        }

        /** {@inheritDoc} */
        @Override public void setCheckpointListener(GridCheckpointListener lsnr) {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStart(@Nullable String gridName) throws GridSpiException {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public void spiStop() throws GridSpiException {
            // No-op.
        }
    }

    /**
     *
     */
    @GridComputeTaskSessionFullSupport
    private static class CheckpointCountingTestTask extends GridComputeTaskAdapter<GridNode, Object> {
        /** {@inheritDoc} */
        @Override public Map<? extends GridComputeJob, GridNode> map(List<GridNode> subgrid, @Nullable GridNode arg)
            throws GridException {
            for (GridNode node : subgrid) {
                if (node.id().equals(arg.id()))
                    return Collections.singletonMap(new GridComputeJobAdapter() {
                        @GridTaskSessionResource
                        private GridComputeTaskSession ses;

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
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return null;
        }
    }
}
