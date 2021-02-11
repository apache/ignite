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

package org.apache.ignite.internal.processors.cache.mvcc;

import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.apache.ignite.util.AttributeNodeFilter;
import org.junit.Test;

import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL_SNAPSHOT;

/**
 * Tests cover cases for correct MVCC flag set for client topology.
 * Client topology, in relation to a specific cache, consist of nodes that match both conditions:
 * - filtered with AttributeNodeFilter for the cache;
 * - don't configured with CacheConfiguration for the cache.
 */
@SuppressWarnings("unchecked")
public class CacheMvccClientTopologyTest extends GridCommonAbstractTest {
    /**
     * Index of node that in client topology
     */
    private static final int clientModeIdx = 0;

    /**
     * Index of node that holds a cache
     */
    private static final int cacheModeIdx = 1;

    /**
     * Check that by default MVCC is disabled for client topology
     */
    @Test
    public void testMvccDisabledByDefaultForClientTopology() throws Exception {
        // when
        final IgniteEx crd = startGrid(getTestIgniteInstanceName(clientModeIdx));
        checkTopology(1);

        // then
        assertNodesMvccDisabled(crd);
    }

    /**
     * Check that MVCC is enabled for client topology if node with MVCC cache joined
     */
    @Test
    public void testMvccEnabledForClientTopology() throws Exception {
        // when
        final IgniteEx crd = startGrid(getTestIgniteInstanceName(clientModeIdx));
        final IgniteEx node = startGrid(getTestIgniteInstanceName(cacheModeIdx));

        checkTopology(2);

        // then
        assertNodesMvccEnabled(crd, node);
    }

    /**
     * Check that MVCC status doesn't change for client topology when MVCC cache node left
     */
    @Test
    public void testMvccEnabledForClientTopologyAfterCacheNodeLeft() throws Exception {
        // when
        final IgniteEx crd = startGrid(getTestIgniteInstanceName(clientModeIdx));
        startGrid(getTestIgniteInstanceName(cacheModeIdx));

        checkTopology(2);
        stopGrid(cacheModeIdx);
        checkTopology(1);

        // then
        assertNodesMvccEnabled(crd);
    }

    /**
     * Check that MVCC status doesn't change for client topology when MVCC cache node coordinator left
     */
    @Test
    public void testMvccEnabledForClientTopologyStartedAfterCacheJoined() throws Exception {
        // when
        startGrid(getTestIgniteInstanceName(cacheModeIdx));
        final IgniteEx node = startGrid(getTestIgniteInstanceName(clientModeIdx));

        checkTopology(2);
        stopGrid(cacheModeIdx);
        checkTopology(1);

        // then
        assertNodesMvccEnabled(node);
    }

    /**
     * Check that MVCC is disabled when cluster state changed to INACTIVE
     */
    @Test
    public void testMvccDisbledForClientTopologyAfterClusterStateChangeToInactive() throws Exception {
        // when
        final IgniteEx crd = startGrid(getTestIgniteInstanceName(clientModeIdx));
        final IgniteEx node = startGrid(getTestIgniteInstanceName(cacheModeIdx));

        checkTopology(2);
        crd.cluster().state(ClusterState.INACTIVE);

        // then
        assertNodesMvccDisabled(crd, node);
    }

    /**
     * Check that MVCC is disabled for client topology if cluster state is INACTIVE but cache node joined
     */
    @Test
    public void testMvccDisabledForClientTopologyCrdBeforeClusterStateChange() throws Exception {
        // given
        IgniteConfiguration crdCfg = getInactiveConfiguration(clientModeIdx);
        IgniteConfiguration nodeCfg = getInactiveConfiguration(cacheModeIdx);

        // when
        final IgniteEx crd = startGrid(crdCfg);
        final IgniteEx node = startGrid(nodeCfg);
        checkTopology(2);

        // then
        assertNodesMvccDisabled(crd);
        assertNodesMvccEnabled(node);
    }

    /**
     * Check that MVCC is disabled for client topology if cluster state is INACTIVE but cache node joined
     */
    @Test
    public void testMvccDisabledForClientTopologyBeforeClusterStateChange() throws Exception {
        // given
        IgniteConfiguration crdCfg = getInactiveConfiguration(cacheModeIdx);
        IgniteConfiguration nodeCfg = getInactiveConfiguration(clientModeIdx);

        // when
        final IgniteEx crd = startGrid(crdCfg);
        final IgniteEx node = startGrid(nodeCfg);
        checkTopology(2);

        // then
        assertNodesMvccEnabled(crd);
        assertNodesMvccDisabled(node);
    }

    /**
     * Check that MVCC is disabled for client topology if cluster changed state to ACTIVE
     */
    @Test
    public void testMvccDisabledForClientTopologyAfterClusterStateChange() throws Exception {
        // given
        IgniteConfiguration crdCfg = getConfiguration(getTestIgniteInstanceName(clientModeIdx))
            .setClusterStateOnStart(ClusterState.INACTIVE);

        // when
        final IgniteEx crd = (IgniteEx) startGrid(getTestIgniteInstanceName(clientModeIdx), crdCfg);

        checkTopology(1);
        crd.cluster().state(ClusterState.ACTIVE);

        // then
        assertNodesMvccDisabled(crd);
    }

    /**
     * Check that MVCC is enabled for client topology if cluster state changed to ACTIVE and cache node joined
     */
    @Test
    public void testMvccEnabledForClientTopologyAfterClusterStateChange() throws Exception {
        // given
        IgniteConfiguration crdCfg = getInactiveConfiguration(clientModeIdx);
        IgniteConfiguration nodeCfg = getInactiveConfiguration(cacheModeIdx);

        // when
        final IgniteEx crd = startGrid(crdCfg);
        final IgniteEx node = startGrid(nodeCfg);
        checkTopology(2);

        crd.cluster().state(ClusterState.ACTIVE);

        // then
        assertNodesMvccEnabled(crd, node);
    }

    /**
     * Check that MVCC is enabled for client topology when cluster state changed to ACTIVE and cache node left
     */
    @Test
    public void testMvccEnabledForClientTopologyAfterClusterStateChangeAndNodeLeft() throws Exception {
        // given
        IgniteConfiguration crdCfg = getInactiveConfiguration(clientModeIdx);
        IgniteConfiguration nodeCfg = getInactiveConfiguration(cacheModeIdx);

        // when
        final IgniteEx crd = startGrid(crdCfg);
        startGrid(nodeCfg);

        checkTopology(2);
        crd.cluster().state(ClusterState.ACTIVE);

        stopGrid(cacheModeIdx);
        checkTopology(1);

        // then
        assertNodesMvccEnabled(crd);
    }

    /**
     * Check that MVCC is enabled for client topology when cluster state changed to ACTIVE and new cache node joined
     */
    @Test
    public void testMvccEnabledForClientTopologyAfterClusterStateChangeAndCacheNodeJoined() throws Exception {
        // given
        IgniteConfiguration crdCfg = getInactiveConfiguration(clientModeIdx);

        // when
        final IgniteEx crd = startGrid(crdCfg);
        crd.cluster().state(ClusterState.ACTIVE);

        final IgniteEx node = startGrid(getTestIgniteInstanceName(cacheModeIdx));
        checkTopology(2);

        // then
        assertNodesMvccEnabled(crd, node);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();
        stopAllGrids();
    }

    /**
     * Configure nodes in client mode (filtered by AttributeNodeFilter, no CacheConfiguration is set)
     * or in ordinary server mode.
     */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (getTestIgniteInstanceIndex(igniteInstanceName) != clientModeIdx) {
            String attrName = "has_cache";
            Object attrVal = Boolean.TRUE;

            CacheConfiguration ccfg = defaultCacheConfiguration()
                .setNearConfiguration(null)
                .setNodeFilter(new AttributeNodeFilter(attrName, attrVal))
                .setAtomicityMode(TRANSACTIONAL_SNAPSHOT);

            return cfg
                .setCacheConfiguration(ccfg)
                .setUserAttributes(F.asMap(attrName, attrVal));
        }

        return cfg;
    }

    /**
     * Configuration with INACTIVE state on start
     */
    private IgniteConfiguration getInactiveConfiguration(int idx) throws Exception {
        return getConfiguration(getTestIgniteInstanceName(idx))
            .setClusterStateOnStart(ClusterState.INACTIVE);
    }

    /**
     * Asserts if any node enables MVCC
     */
    private void assertNodesMvccDisabled(IgniteEx... nodes) {
        assertNodesMvccIs(false, nodes);
    }

    /**
     * Asserts if any node doesn't enable MVCC
     */
    private void assertNodesMvccEnabled(IgniteEx... nodes) {
        assertNodesMvccIs(true, nodes);
    }

    /**
     * Asserts if any node MVCC status doesn't equal expected
     */
    private void assertNodesMvccIs(boolean enabled, IgniteEx... nodes) {
        for (IgniteEx n: nodes)
            assertEquals("Check node: " + n.name(), enabled, n.context().coordinators().mvccEnabled());
    }
}
