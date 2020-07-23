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

package org.apache.ignite.events;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.cluster.IgniteClusterEx;
import org.apache.ignite.internal.commandline.CommandHandler;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/** */
public abstract class BaselineEventsTest extends GridCommonAbstractTest {
    /** */
    private int[] includedEvtTypes = EventType.EVTS_ALL;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setDataStorageConfiguration(
                new DataStorageConfiguration()
                    .setDefaultDataRegionConfiguration(
                        new DataRegionConfiguration()
                            .setPersistenceEnabled(true)
                    )
                    .setWalSegments(3)
                    .setWalSegmentSize(512 * 1024)
            )
            .setConsistentId(igniteInstanceName)
            .setIncludeEventTypes(includedEvtTypes);
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        stopAllGrids();

        cleanPersistenceDir();
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();
    }

    /** */
    protected abstract void listen(IgniteEx ignite, IgnitePredicate<Event> lsnr, int... types);

    /** */
    @Test
    public void testChangeBltWithControlUtility() throws Exception {
        startGrid(0).cluster().active(true);

        AtomicBoolean baselineChanged = new AtomicBoolean();

        startGrid(1);

        String consistentIds = grid(0).localNode().consistentId() + "," + grid(1).localNode().consistentId();

        listen(
            grid(1),
            event -> {
                baselineChanged.set(true);

                BaselineChangedEvent baselineChangedEvt = (BaselineChangedEvent)event;

                assertEquals(2, baselineChangedEvt.baselineNodes().size());

                return true;
            },
            EventType.EVT_BASELINE_CHANGED
        );

        assertEquals(
            CommandHandler.EXIT_CODE_OK,
            new CommandHandler().execute(Arrays.asList("--baseline", "set", consistentIds, "--yes"))
        );

        assertTrue(GridTestUtils.waitForCondition(baselineChanged::get, 3_000));
    }

    /** */
    @Test
    public void testChangeBltWithPublicApi() throws Exception {
        startGrid(0).cluster().active(true);

        AtomicBoolean baselineChanged = new AtomicBoolean();

        listen(
            startGrid(1),
            event -> {
                baselineChanged.set(true);

                BaselineChangedEvent baselineChangedEvt = (BaselineChangedEvent)event;

                assertEquals(2, baselineChangedEvt.baselineNodes().size());

                return true;
            },
            EventType.EVT_BASELINE_CHANGED
        );

        grid(0).cluster().setBaselineTopology(grid(0).cluster().topologyVersion());

        assertTrue(GridTestUtils.waitForCondition(baselineChanged::get, 3_000));
    }

    /** */
    @Test
    public void testDeactivateActivate() throws Exception {
        IgniteEx ignite = startGrids(2);

        AtomicBoolean baselineChanged = new AtomicBoolean();

        listen(
            ignite,
            event -> {
                baselineChanged.set(true);

                return true;
            },
            EventType.EVT_BASELINE_CHANGED
        );

        ignite.cluster().active(true);

        assertTrue(GridTestUtils.waitForCondition(baselineChanged::get, 3_000));
        baselineChanged.set(false);

        ignite.cluster().active(false);
        ignite.cluster().active(true);

        assertFalse(GridTestUtils.waitForCondition(baselineChanged::get, 3_000));
    }

    /** */
    @Test
    public void testChangeAutoAdjustEnabled() throws Exception {
        IgniteClusterEx cluster = startGrids(2).cluster();

        cluster.active(true);

        assertFalse(cluster.isBaselineAutoAdjustEnabled());

        AtomicBoolean autoAdjustEnabled = new AtomicBoolean();

        listen(
            grid(0),
            event -> {
                BaselineConfigurationChangedEvent bltCfgChangedEvt = (BaselineConfigurationChangedEvent)event;

                autoAdjustEnabled.set(bltCfgChangedEvt.isAutoAdjustEnabled());

                return true;
            },
            EventType.EVT_BASELINE_AUTO_ADJUST_ENABLED_CHANGED
        );

        assertEquals(
            CommandHandler.EXIT_CODE_OK,
            new CommandHandler().execute(Arrays.asList("--baseline", "auto_adjust", "enable", "timeout", "10", "--yes"))
        );
        assertTrue(GridTestUtils.waitForCondition(autoAdjustEnabled::get, 3_000));

        assertEquals(
            CommandHandler.EXIT_CODE_OK,
            new CommandHandler().execute(Arrays.asList("--baseline", "auto_adjust", "disable", "--yes"))
        );
        assertFalse(autoAdjustEnabled.get());

        cluster.baselineAutoAdjustEnabled(true);
        assertTrue(GridTestUtils.waitForCondition(autoAdjustEnabled::get, 3_000));

        cluster.baselineAutoAdjustEnabled(false);
        assertTrue(GridTestUtils.waitForCondition(() -> !autoAdjustEnabled.get(), 3_000));
    }

    /** */
    @Test
    public void testChangeAutoAdjustTimeout() throws Exception {
        IgniteClusterEx cluster = startGrids(2).cluster();

        cluster.active(true);

        AtomicLong autoAdjustTimeout = new AtomicLong();

        listen(
            grid(0),
            event -> {
                BaselineConfigurationChangedEvent bltCfgChangedEvt = (BaselineConfigurationChangedEvent)event;

                autoAdjustTimeout.set(bltCfgChangedEvt.autoAdjustTimeout());

                return true;
            },
            EventType.EVT_BASELINE_AUTO_ADJUST_AWAITING_TIME_CHANGED
        );

        assertEquals(
            CommandHandler.EXIT_CODE_OK,
            new CommandHandler().execute(Arrays.asList("--baseline", "auto_adjust", "enable", "timeout", "10", "--yes"))
        );
        assertTrue(GridTestUtils.waitForCondition(() -> autoAdjustTimeout.get() == 10L, 3_000));

        cluster.baselineAutoAdjustTimeout(50);
        assertTrue(GridTestUtils.waitForCondition(() -> autoAdjustTimeout.get() == 50L, 3_000));
    }

    /** */
    @Test
    public void testEventsDisabledByDefault() throws Exception {
        //noinspection ZeroLengthArrayAllocation
        includedEvtTypes = new int[0];

        IgniteClusterEx cluster = startGrid(0).cluster();
        cluster.active(true);

        AtomicInteger evtsTriggered = new AtomicInteger();

        listen(
            grid(0),
            event -> {
                evtsTriggered.incrementAndGet();

                return true;
            },
            EventType.EVT_BASELINE_CHANGED,
            EventType.EVT_BASELINE_AUTO_ADJUST_ENABLED_CHANGED,
            EventType.EVT_BASELINE_AUTO_ADJUST_AWAITING_TIME_CHANGED
        );

        startGrid(1);

        String consistentIds = grid(0).localNode().consistentId() + "," + grid(1).localNode().consistentId();

        assertEquals(
            CommandHandler.EXIT_CODE_OK,
            new CommandHandler().execute(Arrays.asList("--baseline", "set", consistentIds, "--yes"))
        );

        awaitPartitionMapExchange();

        startGrid(2);

        cluster.setBaselineTopology(cluster.topologyVersion());

        awaitPartitionMapExchange();

        assertEquals(
            CommandHandler.EXIT_CODE_OK,
            new CommandHandler().execute(Arrays.asList("--baseline", "auto_adjust", "enable", "timeout", "10", "--yes"))
        );

        cluster.baselineAutoAdjustEnabled(false);
        cluster.baselineAutoAdjustTimeout(50);

        assertFalse(GridTestUtils.waitForCondition(() -> evtsTriggered.get() > 0, 3_000L));
    }
}
