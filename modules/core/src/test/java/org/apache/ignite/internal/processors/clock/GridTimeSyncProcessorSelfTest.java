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

package org.apache.ignite.internal.processors.clock;

import java.util.NavigableMap;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContextImpl;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lifecycle.LifecycleBean;
import org.apache.ignite.lifecycle.LifecycleEventType;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

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
            final IgniteKernal kernal = (IgniteKernal)grid(0);

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
            final IgniteKernal kernal = (IgniteKernal)grid(0);

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
        @Override public void onLifecycleEvent(LifecycleEventType evt) {
            if (evt == LifecycleEventType.BEFORE_NODE_START)
                ((GridKernalContextImpl)((IgniteKernal)g).context()).timeSource(new TimeShiftClockSource(delta));
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