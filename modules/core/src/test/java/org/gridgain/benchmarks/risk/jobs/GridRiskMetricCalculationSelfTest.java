/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.benchmarks.risk.jobs;

import org.gridgain.benchmarks.risk.*;
import org.gridgain.benchmarks.risk.model.*;
import org.gridgain.grid.*;
import org.gridgain.grid.cache.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

import static org.gridgain.benchmarks.risk.GridRiskMain.*;

/**
 *
 */
public class GridRiskMetricCalculationSelfTest extends GridCommonAbstractTest {
    /** {@inheritDoc} */
    @Override protected GridConfiguration getConfiguration(String gridName) throws Exception {
        GridConfiguration cfg = loadConfiguration(WORKER_CFG);

        cfg.setGridName(gridName);

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testApproach1() throws Exception {
        Grid worker = startGridsMultiThreaded(1);

        try {
            GridRiskMain.main(
                String.valueOf(ENTRIES_COUNT / 10),
                String.valueOf(EVENTS_COUNT / 10),
                MASTER_CFG,
                "true",
                "approach1"
            );

            GridCache<GridRiskAffinityKey, Map<GridRiskDataKey, GridRiskDataEntry>> cache = worker.cache("approach1");

            assertNotNull("Expects cache configured.", cache);

            List<GridRiskEvent> evts = events(EVENTS_COUNT);

            for (int i = 0; i < 10; i++) {
                Map<GridRiskDataKey, GridRiskDataEntry> valMap = cache.randomEntry().get();

                GridRiskDataEntry val = F.first(valMap.values());

                if (val == null) {
                    i--;

                    continue;
                }

                GridRiskDataEntry check = new GridRiskDataEntry(val);

                updateEntry(check, evts);

                System.out.println("Grid: " + val);
                System.out.println("Calc: " + check);
                System.out.println();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testApproach2() throws Exception {
        Grid worker = startGridsMultiThreaded(1);

        try {
            GridRiskMain.main(
                String.valueOf(ENTRIES_COUNT / 10),
                String.valueOf(EVENTS_COUNT / 10),
                MASTER_CFG,
                "true",
                "approach2"
            );

            GridCache<GridRiskAffinityKey, GridRiskDataEntry> cache = worker.cache("approach2");

            assertNotNull("Expects cache configured.", cache);

            List<GridRiskEvent> evts = events(EVENTS_COUNT);

            for (int i = 0; i < 10; i++) {
                GridRiskDataEntry val = cache.randomEntry().get();

                if (val == null) {
                    i--;

                    continue;
                }

                GridRiskDataEntry check = new GridRiskDataEntry(val);

                updateEntry(check, evts);

                System.out.println("Grid: " + val);
                System.out.println("Calc: " + check);
                System.out.println();
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Generate processing events collection.
     *
     * @param cnt Number of pre-defined events.
     * @return Pre-defined collection of processing events.
     */
    private List<GridRiskEvent> events(int cnt) {
        List<GridRiskEvent> evts = new ArrayList<>(cnt);

        for (GridRiskEventGenerator evtsSrc = new GridRiskEventGenerator(cnt); evtsSrc.hasNext(); )
            evts.add(evtsSrc.next());

        return evts;
    }

    /**
     * Update entry with sequence of events.
     *
     * @param dataEntry Data entry to apply new metrics to.
     * @param evts Events to apply to entry.
     */
    private void updateEntry(GridRiskDataEntry dataEntry, Iterable<GridRiskEvent> evts) {
        // Publish {@code updateMetrics} method.
        GridRiskEventJobAdapter<?, ?> job = new GridRiskEventJobAdapter<Object, Object>() {
            @Override public Object call() throws Exception {
                throw new UnsupportedOperationException("Not implemented.");
            }
        };

        for (GridRiskEvent evt : evts)
            if (dataEntry.factor1().equals(evt.factor()) || dataEntry.factor2().equals(evt.factor()))
                job.updateMetrics(dataEntry, evt.value());
    }
}
