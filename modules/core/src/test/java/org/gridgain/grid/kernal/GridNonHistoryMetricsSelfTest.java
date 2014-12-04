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
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.logger.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;
import java.util.concurrent.*;

import static org.apache.ignite.events.IgniteEventType.*;

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
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

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

        ignite.events().localListen(new IgnitePredicate<IgniteEvent>() {
            @Override public boolean apply(IgniteEvent evt) {
                assert evt.type() == EVT_NODE_METRICS_UPDATED;

                latch.countDown();

                return true;
            }
        }, EVT_NODE_METRICS_UPDATED);

        latch.await();

        GridTestUtils.waitForCondition(new GridAbsPredicate() {
            @Override public boolean apply() {
                ClusterNodeMetrics metrics = ignite.cluster().localNode().metrics();

                return metrics.getTotalExecutedJobs() == 5;
            }
        }, 5000);

        ClusterNodeMetrics metrics = ignite.cluster().localNode().metrics();

        info("Node metrics: " + metrics);

        assertEquals(5, metrics.getTotalExecutedJobs());
        assertEquals(0, metrics.getTotalCancelledJobs());
        assertEquals(0, metrics.getTotalRejectedJobs());
    }

    /**
     * Test task.
     */
    private static class TestTask extends ComputeTaskSplitAdapter<Object, Object> {
        /** Logger. */
        @IgniteLoggerResource
        private IgniteLogger log;

        /** {@inheritDoc} */
        @Override public Collection<? extends ComputeJob> split(int gridSize, Object arg) {
            Collection<ComputeJob> refs = new ArrayList<>(gridSize*5);

            for (int i = 0; i < gridSize * 5; i++)
                refs.add(new GridTestJob(arg.toString() + i + 1));

            return refs;
        }

        /** {@inheritDoc} */
        @Override public Object reduce(List<ComputeJobResult> results) throws GridException {
            return results;
        }
    }
}
