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

package org.apache.ignite.internal.processors.cache.persistence.baseline;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCluster;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.failure.StopNodeOrHaltFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static java.util.Comparator.comparingLong;
import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_ACTIVATED;
import static org.apache.ignite.events.EventType.EVT_CLUSTER_DEACTIVATED;

/**
 * Tests cluster activation events.
 */
public class ClusterActivationEventTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 2;

    /** Listener delay. */
    private static final long DELAY = 1000L;

    /** Logger message format. */
    private static final String LOG_MESSAGE_FORMAT = "Received event [id=%s, type=%s], msg=%s";

    /** */
    private final IgnitePredicate<? extends Event> lsnr = (evt) -> {
        log.info(String.format(LOG_MESSAGE_FORMAT, evt.id(), evt.type(), evt.message()));

        return true;
    };

    /** */
    private final IgnitePredicate<? extends Event> delayLsnr = (evt) -> {
        log.info(String.format(LOG_MESSAGE_FORMAT, evt.id(), evt.type(), evt.message()));

        try {
            U.sleep(DELAY);
        }
        catch (IgniteInterruptedCheckedException e) {
            log.error("Sleep interrupted", e);
        }

        return true;
    };

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setIncludeEventTypes(EventType.EVTS_ALL)
            .setCacheConfiguration(new CacheConfiguration(DEFAULT_CACHE_NAME))
            .setFailureHandler(new StopNodeOrHaltFailureHandler());
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        startGrids(NODES_CNT);

        startClientGrid(NODES_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        for (Ignite ignite : G.allGrids()) {
            ignite.events().stopLocalListen(lsnr);
            ignite.events().stopLocalListen(delayLsnr);
        }

        grid(0).cluster().state(ACTIVE);

        grid(0).cache(DEFAULT_CACHE_NAME).removeAll();

        Map<Integer, Integer> vals = IntStream.range(0, 100).boxed().collect(Collectors.toMap(i -> i, i -> i));

        grid(0).cachex(DEFAULT_CACHE_NAME).putAll(vals);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        for (Ignite ignite : G.allGrids()) {
            ignite.events().stopLocalListen(lsnr);
            ignite.events().stopLocalListen(delayLsnr);
        }

        super.afterTest();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterActivation() throws Exception {
        clusterChangeState(INACTIVE, ACTIVE, EVT_CLUSTER_ACTIVATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterDeactivation() throws Exception {
        clusterChangeState(ACTIVE, INACTIVE, EVT_CLUSTER_DEACTIVATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterDoubleActivation() throws Exception {
        clusterChangeStateTwice(INACTIVE, ACTIVE, EVT_CLUSTER_ACTIVATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterDoubleDeactivation() throws Exception {
        clusterChangeStateTwice(ACTIVE, INACTIVE, EVT_CLUSTER_DEACTIVATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterActivationListenerSleep() throws Exception {
        clusterChangeStateWithDelay(INACTIVE, ACTIVE, EVT_CLUSTER_ACTIVATED);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testClusterDeactivationListenerSleep() throws Exception {
        clusterChangeStateWithDelay(ACTIVE, INACTIVE, EVT_CLUSTER_DEACTIVATED);
    }

    /**
     * Checks that change cluster state from {@code initState} to {@code state} generates correct number of events on
     * each node with {@code evtType} type.
     *
     * @param initState Initial cluster state.
     * @param state Target cluster state.
     * @param evtType Event type.
     * @throws Exception If failed.
     */
    private void clusterChangeState(ClusterState initState, ClusterState state, int evtType) throws Exception {
        assertNotSame(initState, state);

        checkClusterEvents(cluster -> cluster.state(state), lsnr, initState, evtType, 1);
    }

    /**
     * Checks that change cluster state from {@code initState} to {@code state} generates correct number of events on
     * each node with {@code evtType} type and delay on event listener doesn't breaks cluster.
     *
     * @param initState Initial cluster state.
     * @param state Target cluster state.
     * @param evtType Event type.
     * @throws Exception If failed.
     */
    private void clusterChangeStateWithDelay(ClusterState initState, ClusterState state, int evtType) throws Exception {
        assertNotSame(initState, state);

        checkClusterEvents(cluster -> cluster.state(state), delayLsnr, initState, evtType, 1);
    }

    /**
     * Checks that change cluster state from {@code initState} to {@code state} generates correct number of events on
     * each node with {@code evtType} type.
     *
     * @param initState Initial cluster state.
     * @param state Target cluster state.
     * @param evtType Event type.
     * @throws Exception If failed.
     */
    private void clusterChangeStateTwice(ClusterState initState, ClusterState state, int evtType) throws Exception {
        assertNotSame(initState, state);

        ClusterActivationTestTask task = new ClusterActivationTestTask() {
            @Override public void execute(IgniteCluster cluster) {
                cluster.state(state);
                cluster.state(state);
            }
        };

        checkClusterEvents(task, lsnr, initState, evtType, 1);
    }

    /**
     * @param task Test.
     * @param lsnr Listener.
     * @param evtType Event type.
     * @param evtCnt Events count.
     */
    private void checkClusterEvents(
        ClusterActivationTestTask task,
        IgnitePredicate<? extends Event> lsnr,
        ClusterState initState,
        int evtType,
        int evtCnt
    ) throws Exception {
        IgniteEx crd = grid(0);

        if (crd.cluster().state() != initState)
            crd.cluster().state(initState);

        for (Ignite ignite : G.allGrids())
            assertEquals(ignite.name(), initState, ignite.cluster().state());

        Map<Ignite, Long> maxLocEvtId = new HashMap<>();
        Map<Ignite, IgniteFuture<Event>> evtFuts = new HashMap<>();

        for (Ignite ignite : G.allGrids()) {
            Collection<Event> evts = ignite.events().localQuery(F.alwaysTrue(), evtType);

            long id = evts.isEmpty() ? 0 : Collections.max(evts, comparingLong(Event::localOrder)).localOrder();

            ignite.events().localListen(lsnr, evtType);

            maxLocEvtId.put(ignite, id);

            evtFuts.put(ignite, waitForLocalEvent(ignite.events(), e -> e.localOrder() > id, evtType));
        }

        task.execute(crd.cluster());

        for (Ignite ignite : maxLocEvtId.keySet()) {
            // We should wait received event on local node.
            evtFuts.get(ignite).get(2 * DELAY);

            Collection<Event> evts = ignite.events().localQuery(e -> e.localOrder() > maxLocEvtId.get(ignite), evtType);

            assertEquals(ignite.name() + " events: " + evts, evtCnt, evts.size());
        }
    }

    /**
     * Cluster activation test task interface
     */
    private interface ClusterActivationTestTask {
        /**
         * @param cluster Cluster
         */
        void execute(IgniteCluster cluster);
    }
}
