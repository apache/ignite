/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.events.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.resources.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.gridgain.grid.events.GridEventType.*;

/**
 *
 */
public class GridNonHistoryMetricsSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        startGrid();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = super.getConfiguration(gridName);

        cfg.setMetricsHistorySize(5);

        cfg.setCacheConfiguration();

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testSingleTaskMetrics() throws Exception {
        final Ignite ignite = grid();

        ignite.compute().execute(new TestTask(), "testArg");

        // Let metrics update twice.
        final CountDownLatch latch = new CountDownLatch(2);

        ignite.events().localListen(new GridPredicate<GridEvent>() {
            @Override public boolean apply(GridEvent evt) {
                assert evt.type() == EVT_NODE_METRICS_UPDATED;

                latch.countDown();

                return true;
            }
        }, EVT_NODE_METRICS_UPDATED);

        latch.await();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                GridNodeMetrics metrics = ignite.cluster().localNode().metrics();

                return metrics.getTotalExecutedJobs() == 5;
            }
        }, 5000);

        GridNodeMetrics metrics = ignite.cluster().localNode().metrics();

        info("Node metrics: " + metrics);

        assertEquals(5, metrics.getTotalExecutedJobs());
        assertEquals(0, metrics.getTotalCancelledJobs());
        assertEquals(0, metrics.getTotalRejectedJobs());
    }

    /**
     * Test task.
     */
    private static class TestTask extends GridComputeTaskSplitAdapter<Object, Object> {
        /** Logger. */
        @GridLoggerResource private GridLogger log;

        /** {@inheritDoc} */
        @Override public Collection<? extends GridComputeJob> split(int gridSize, Object arg) {
            Collection<GridComputeJob> refs = new ArrayList<>(gridSize*5);

            for (int i = 0; i < gridSize * 5; i++)
                refs.add(new GridTestJob(arg.toString() + i + 1));

            return refs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<GridComputeJobResult> results) throws GridException {
            return results;
        }
    }
}
