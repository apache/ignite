/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.clock;

import org.apache.ignite.*;
import org.apache.ignite.configuration.*;
import org.apache.ignite.lifecycle.*;
import org.apache.ignite.resources.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.spi.discovery.tcp.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.*;
import org.gridgain.grid.spi.discovery.tcp.ipfinder.vm.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.testframework.*;
import org.gridgain.testframework.junits.common.*;

import java.util.*;

/**
 * Time sync processor self test.
 */
public class GridTimeSyncProcessorSelfTest extends GridCommonAbstractTest {
    /** IP finder. */
    private static final TcpDiscoveryIpFinder IP_FINDER = new TcpDiscoveryVmIpFinder(true);

    /** Number of grids in test. */
    public static final int GRID_CNT = 4;

    /** Starting grid index. */
    private int idx;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        TcpDiscoverySpi discoSpi = new TcpDiscoverySpi();

        discoSpi.setIpFinder(IP_FINDER);

        cfg.setDiscoverySpi(discoSpi);

        cfg.setLifecycleBeans(new TimeShiftLifecycleBean(idx * 2000));

        idx++;

        return cfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimeSync() throws Exception {
        startGrids(GRID_CNT);

        try {
            // Check coordinator time deltas.
            final GridKernal kernal = (GridKernal)grid(0);

            // Wait for latest time sync history.
            GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    NavigableMap<GridClockDeltaVersion, GridClockDeltaSnapshot> hist = kernal.context().clockSync()
                        .timeSyncHistory();

                    info("Checking time sync history: " + hist);

                    for (GridClockDeltaVersion ver : hist.keySet()) {
                        if (ver.topologyVersion() == 4)
                            return true;
                    }

                    return false;
                }
            }, 10000);

            NavigableMap<GridClockDeltaVersion, GridClockDeltaSnapshot> history = kernal.context().clockSync()
                .timeSyncHistory();

            GridClockDeltaSnapshot snap = history.lastEntry().getValue();

            assertEquals(3, snap.deltas().size());

            for (int i = 1; i < GRID_CNT; i++) {
                Long delta = snap.deltas().get(grid(i).localNode().id());

                // Give 300ms range for test?
                int idealDelta = - i * 2000;

                int threshold = 100;

                assertTrue("Invalid time delta for node [expected=" + idealDelta + ", " +
                    "actual=" + delta + ", threshold=" + threshold,
                    delta <= idealDelta + threshold && delta >= idealDelta - threshold);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * @throws Exception If failed.
     */
    public void testTimeSyncChangeCoordinator() throws Exception {
        startGrids(GRID_CNT);

        try {
            for (int i = 0; i < GRID_CNT; i++) {
                // Not coordinator now.
                stopGrid(i);

                startGrid(i);
            }

            // Check coordinator time deltas.
            final GridKernal kernal = (GridKernal)grid(0);

            assertEquals(6, kernal.localNode().order());

            // Wait for latest time sync history.
            GridTestUtils.waitForCondition(new PA() {
                @Override public boolean apply() {
                    NavigableMap<GridClockDeltaVersion, GridClockDeltaSnapshot> hist = kernal.context().clockSync()
                        .timeSyncHistory();

                    info("Checking time sync history: " + hist);

                    for (GridClockDeltaVersion ver : hist.keySet()) {
                        if (ver.topologyVersion() == 12)
                            return true;
                    }

                    return false;
                }
            }, 10000);

            NavigableMap<GridClockDeltaVersion, GridClockDeltaSnapshot> history = kernal.context().clockSync()
                .timeSyncHistory();

            GridClockDeltaSnapshot snap = history.lastEntry().getValue();

            assertEquals(3, snap.deltas().size());

            for (int i = 1; i < GRID_CNT; i++) {
                Long delta = snap.deltas().get(grid(i).localNode().id());

                // Give 300ms range for test?
                int idealDelta = - i * 2000;

                int threshold = 100;

                assertTrue("Invalid time delta for node [expected=" + idealDelta + ", " +
                    "actual=" + delta + ", threshold=" + threshold,
                    delta <= idealDelta + threshold && delta >= idealDelta - threshold);
            }
        }
        finally {
            stopAllGrids();
        }
    }

    /**
     * Time bean that sets shifted time source to context.
     */
    private static class TimeShiftLifecycleBean implements LifecycleBean {
        /** Injected grid. */
        @IgniteInstanceResource
        private Ignite g;

        /** Time delta. */
        private int delta;

        /**
         * Constructs lifecycle bean.
         *
         * @param delta Time delta.
         */
        private TimeShiftLifecycleBean(int delta) {
            this.delta = delta;
        }

        /** {@inheritDoc} */
        @Override public void onLifecycleEvent(LifecycleEventType evt) throws GridException {
            if (evt == LifecycleEventType.BEFORE_GRID_START)
                ((GridKernalContextImpl)((GridKernal)g).context()).timeSource(new TimeShiftClockSource(delta));
        }
    }

    /**
     * Time shift time source.
     */
    private static class TimeShiftClockSource implements GridClockSource {
        /** Time shift delta. */
        private int delta;

        /**
         * @param delta Time shift delta.
         */
        private TimeShiftClockSource(int delta) {
            this.delta = delta;
        }

        /** {@inheritDoc} */
        @Override public long currentTimeMillis() {
            return System.currentTimeMillis() + delta;
        }
    }
}
