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

package org.apache.ignite.internal.managers.events;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.events.EventType.EVT_BASELINE_AUTO_ADJUST_ENABLED_CHANGED;
import static org.apache.ignite.events.EventType.EVT_BASELINE_CHANGED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_ACTIVATED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_DEACTIVATED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_STATE_CHANGED;

/**
 * Tests for {@link GridEventStorageManager}.
 */
public class GridEventStorageManagerInternalEventsSelfTest extends GridCommonAbstractTest {
    /** */
    public GridEventStorageManagerInternalEventsSelfTest() {
        super(/* start grid */false);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * Test that activation events are received on non-coordinator node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testClusterActivationEventsOnOtherNode() throws Exception {
        Ignite ignite1 = startGrid(0);
        Ignite ignite2 = startGrid(1);

        CountDownLatch deactivatedLatch = addDisposableLocalListener(ignite2, EVT_CLUSTER_DEACTIVATED);
        CountDownLatch stateChangedLatch = addDisposableLocalListener(ignite2, EVT_CLUSTER_STATE_CHANGED);

        ignite1.cluster().state(ClusterState.INACTIVE);

        assertTrue(deactivatedLatch.await(2, TimeUnit.SECONDS));
        assertTrue(stateChangedLatch.await(2, TimeUnit.SECONDS));

        CountDownLatch activatedLatch = addDisposableLocalListener(ignite2, EVT_CLUSTER_ACTIVATED);
        CountDownLatch stateChangedLatch2 = addDisposableLocalListener(ignite2, EVT_CLUSTER_STATE_CHANGED);

        ignite1.cluster().state(ClusterState.ACTIVE);

        assertTrue(activatedLatch.await(2, TimeUnit.SECONDS));
        assertTrue(stateChangedLatch2.await(2, TimeUnit.SECONDS));
    }

    /**
     * Test that baseline changed event is received on non-coordinator node.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testBaselineChangedEventOnOtherNode() throws Exception {
        Ignite ignite1 = startGrid(0);
        Ignite ignite2 = startGrid(1);
        Ignite ignite3 = startGrid(2);

        Object consistentId3 = ignite3.cluster().localNode().consistentId();

        ignite1.cluster().baselineAutoAdjustEnabled(false);

        stopGrid(ignite3.name(), true, true);

        CountDownLatch baselineEvtLatch = addDisposableLocalListener(ignite2, EVT_BASELINE_CHANGED);

        List<BaselineNode> newBaseline = ignite1.cluster().currentBaselineTopology().stream()
            .filter(node -> !node.consistentId().equals(consistentId3))
            .collect(Collectors.toList());

        ignite1.cluster().setBaselineTopology(newBaseline);

        assertTrue(baselineEvtLatch.await(2, TimeUnit.SECONDS));
    }

    /**
     * Test that non-internal event will not be received.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testNotReceiveAutoAdjustChangedEvent() throws Exception {
        Ignite ignite1 = startGrid(0);
        Ignite ignite2 = startGrid(1);

        CountDownLatch latch1 = addDisposableLocalListener(ignite1, EVT_BASELINE_AUTO_ADJUST_ENABLED_CHANGED);
        CountDownLatch latch2 = addDisposableLocalListener(ignite2, EVT_BASELINE_AUTO_ADJUST_ENABLED_CHANGED);

        ignite1.cluster().baselineAutoAdjustEnabled(false);
        ignite1.cluster().baselineAutoAdjustEnabled(true);

        assertFalse(latch1.await(2, TimeUnit.SECONDS));
        assertFalse(latch2.await(2, TimeUnit.SECONDS));
    }

    /**
     * Add local listener to Ignite that will be unregistered after event receiving.
     *
     * @param ignite Ignite.
     * @param evtType Event type.
     * @return {@link CountDownLatch} that will be released when event is received.
     */
    private CountDownLatch addDisposableLocalListener(Ignite ignite, int evtType) {
        CountDownLatch latch = new CountDownLatch(1);

        ignite.events().localListen(evt -> {
            assert evt.type() == evtType : "Unexpected event type: " + evt;

            latch.countDown();

            return false;
        }, evtType);

        return latch;
    }
}
