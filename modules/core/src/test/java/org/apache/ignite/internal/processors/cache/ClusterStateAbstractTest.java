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
 *
 */

package org.apache.ignite.internal.processors.cache;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.cluster.ClusterState.INACTIVE;

/**
 *
 */
public abstract class ClusterStateAbstractTest extends GridCommonAbstractTest {
    /** */
    protected static final int GRID_CNT = 2;

    /** */
    protected static final String CACHE_NAME = "cache1";

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String gridName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(gridName);

        cfg.setClusterStateOnStart(INACTIVE);

        cfg.setCacheConfiguration(cacheConfiguration(CACHE_NAME));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        startGridsMultiThreaded(GRID_CNT);
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        grid(0).cluster().state(INACTIVE);

        checkInactive(nodesCount());
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        grid(0).cluster().state(INACTIVE);

        checkInactive(nodesCount());

        super.afterTest();
    }

    /** */
    @Test
    public void testActivation() {
        changeStateAndCheckBehaviour(INACTIVE, ACTIVE);
    }

    /** */
    @Test
    public void testActivationWithReadOnly() {
        changeStateAndCheckBehaviour(INACTIVE, ACTIVE_READ_ONLY);
    }

    /** */
    @Test
    public void testEnablingReadOnly() {
        changeStateAndCheckBehaviour(ACTIVE, ACTIVE_READ_ONLY);
    }

    /** */
    @Test
    public void testDisablingReadOnly() {
        changeStateAndCheckBehaviour(ACTIVE_READ_ONLY, ACTIVE);
    }

    /** */
    @Test
    public void testDeactivation() {
        changeStateAndCheckBehaviour(ACTIVE, INACTIVE);
    }

    /** */
    @Test
    public void testDeactivationFromReadOnly() {
        changeStateAndCheckBehaviour(ACTIVE_READ_ONLY, INACTIVE);
    }

    /**
     * @param cacheName Cache name.
     * @return Cache configuration.
     */
    protected abstract CacheConfiguration cacheConfiguration(String cacheName);

    /** */
    protected abstract void changeState(ClusterState state);

    /**
     * Changes cluster state from {@code initialState} to {@code targetState}.
     *
     * @param initialState Initial state.
     * @param targetState Target state.
     */
    private void changeStateAndCheckBehaviour(ClusterState initialState, ClusterState targetState) {
        assertNotSame(initialState, targetState);

        IgniteEx crd = grid(0);

        checkInactive(nodesCount());

        long timeOnStart = crd.context().state().lastStateChangeTime();

        assertNotSame(0, timeOnStart);
        assertTrue(String.valueOf(timeOnStart), timeOnStart > 0);

        crd.cluster().state(initialState);

        if (initialState == INACTIVE)
            assertEquals(timeOnStart, crd.context().state().lastStateChangeTime());
        else {
            long activationTime = crd.context().state().lastStateChangeTime();

            assertNotSame(timeOnStart, activationTime);
            assertTrue(activationTime + " " + timeOnStart, activationTime > timeOnStart);
        }

        checkClusterState(nodesCount(), initialState);

        changeState(targetState);

        checkClusterState(nodesCount(), targetState);

        List<IgniteEx> nodes = IntStream.range(0, nodesCount())
            .mapToObj(this::grid)
            .collect(Collectors.toList());

        ClusterStateTestUtils.putSomeDataAndCheck(log, nodes, CACHE_NAME);
    }

    /** */
    protected int nodesCount() {
        return GRID_CNT;
    }

    /** */
    private void checkClusterState(int nodesCnt, ClusterState state) {
        for (int g = 0; g < nodesCnt; g++)
            assertEquals(grid(g).name(), state, grid(g).cluster().state());
    }

    /** */
    private void checkInactive(int cnt) {
        checkClusterState(cnt, INACTIVE);
    }
}
