/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.cluster.ClusterState.ACTIVE;
import static org.apache.ignite.cluster.ClusterState.ACTIVE_READ_ONLY;
import static org.apache.ignite.testframework.GridTestUtils.assertNotContains;

/**
 * Checks that node out of baseline will no be added to baseline during transition from one active cluster state to
 * another active cluster state.
 */
public class ClusterActiveStateChangeWithNodeOutOfBaselineTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Id of node out of baseline. */
    private static final int NODE_OUT_OF_BASELINE_ID = NODES_CNT;

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        stopAllGrids();

        cleanPersistenceDir();

        Ignite crd = startGridsMultiThreaded(NODES_CNT);

        crd.cluster().state(ClusterState.ACTIVE);

        startGrid(NODE_OUT_OF_BASELINE_ID);

        checkBaseline();
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();

        cleanPersistenceDir();

        super.afterTestsStopped();
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)
                )
            );
    }

    /** */
    @Test
    public void testActiveActive() {
        check(ACTIVE, ACTIVE);
    }

    /** */
    @Test
    public void testActiveReadOnly() {
        check(ACTIVE, ACTIVE_READ_ONLY);
    }

    /** */
    @Test
    public void testReadOnlyActive() {
        check(ACTIVE_READ_ONLY, ACTIVE);
    }

    /** */
    @Test
    public void testReadOnlyReadOnly() {
        check(ACTIVE_READ_ONLY, ACTIVE_READ_ONLY);
    }

    /** */
    private void check(ClusterState initialState, ClusterState targetState) {
        assertTrue(String.valueOf(initialState), initialState.active());
        assertTrue(String.valueOf(targetState), targetState.active());

        if (grid(0).cluster().state() != initialState)
            grid(0).cluster().state(initialState);

        assertEquals(initialState, grid(0).cluster().state());

        checkBaseline();

        grid(0).cluster().state(targetState);

        assertEquals(targetState, grid(0).cluster().state());

        checkBaseline();
    }

    /** */
    private void checkBaseline() {
        Collection<BaselineNode> bltNodes = grid(0).cluster().currentBaselineTopology();

        assertEquals(NODES_CNT, bltNodes.size());

        assertNotContains(log, bltNodes, grid(NODE_OUT_OF_BASELINE_ID).cluster().localNode());
    }
}
