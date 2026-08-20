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

package org.apache.ignite.internal.metric;

import java.util.Iterator;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.cluster.ClusterState;
import org.apache.ignite.configuration.DataRegionConfiguration;
import org.apache.ignite.configuration.DataStorageConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.systemview.BaselineNodeAttributeViewWalker;
import org.apache.ignite.internal.systemview.NodeAttributeViewWalker;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.systemview.view.BaselineNodeAttributeView;
import org.apache.ignite.spi.systemview.view.BaselineNodeView;
import org.apache.ignite.spi.systemview.view.ClusterNodeView;
import org.apache.ignite.spi.systemview.view.FiltrableSystemView;
import org.apache.ignite.spi.systemview.view.NodeAttributeView;
import org.apache.ignite.spi.systemview.view.NodeMetricsView;
import org.apache.ignite.spi.systemview.view.SystemView;
import org.apache.ignite.testframework.junits.WithSystemProperty;
import org.junit.Test;

import static org.apache.ignite.IgniteSystemProperties.IGNITE_DATA_CENTER_ID;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODES_SYS_VIEW;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODE_ATTRIBUTES_SYS_VIEW;
import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.NODE_METRICS_SYS_VIEW;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.BASELINE_NODES_SYS_VIEW;
import static org.apache.ignite.internal.processors.cluster.GridClusterStateProcessor.BASELINE_NODE_ATTRIBUTES_SYS_VIEW;
import static org.apache.ignite.internal.util.IgniteUtils.toStringSafe;

/** Tests for {@link SystemView} for nodes. */
public class SystemViewNodesTest extends SystemViewAbstractTest {
    /** */
    @Test
    @WithSystemProperty(key = IGNITE_DATA_CENTER_ID, value = "DC0")
    public void testNodes() throws Exception {
        try (IgniteEx g1 = startGrid(0)) {
            SystemView<ClusterNodeView> views = g1.context().systemView().view(NODES_SYS_VIEW);

            assertEquals(1, views.size());

            try (IgniteEx g2 = startGrid(1)) {
                awaitPartitionMapExchange();

                checkViewsState(views, g1.localNode(), g2.localNode());
                checkViewsState(g2.context().systemView().view(NODES_SYS_VIEW), g2.localNode(), g1.localNode());
            }

            assertEquals(1, views.size());
        }
    }

    /** */
    @Test
    public void testNodeAttributes() throws Exception {
        try (
            IgniteEx ignite0 = startGrid(getConfiguration(getTestIgniteInstanceName(0))
                .setUserAttributes(F.asMap("name", "val0")));
            IgniteEx ignite1 = startGrid(getConfiguration(getTestIgniteInstanceName(1))
                .setUserAttributes(F.asMap("name", "val1")))
        ) {
            awaitPartitionMapExchange();

            SystemView<NodeAttributeView> view = ignite0.context().systemView().view(NODE_ATTRIBUTES_SYS_VIEW);

            assertEquals(ignite0.cluster().localNode().attributes().size() +
                ignite1.cluster().localNode().attributes().size(), view.size());

            assertEquals(1, F.size(view.iterator(), row -> "name".equals(row.name()) && "val0".equals(row.value())));
            assertEquals(1, F.size(view.iterator(), row -> "name".equals(row.name()) && "val1".equals(row.value())));

            // Test filtering.
            assertTrue(view instanceof FiltrableSystemView);

            Iterator<NodeAttributeView> iter = ((FiltrableSystemView<NodeAttributeView>)view)
                .iterator(F.asMap(NodeAttributeViewWalker.NODE_ID_FILTER, ignite0.cluster().localNode().id()));

            assertEquals(1, F.size(iter, row -> "name".equals(row.name()) && "val0".equals(row.value())));

            iter = ((FiltrableSystemView<NodeAttributeView>)view).iterator(
                F.asMap(NodeAttributeViewWalker.NODE_ID_FILTER, ignite1.cluster().localNode().id().toString()));

            assertEquals(1, F.size(iter, row -> "name".equals(row.name()) && "val1".equals(row.value())));

            iter = ((FiltrableSystemView<NodeAttributeView>)view).iterator(
                F.asMap(NodeAttributeViewWalker.NODE_ID_FILTER, "malformed-id"));

            assertEquals(0, F.size(iter));

            iter = ((FiltrableSystemView<NodeAttributeView>)view).iterator(
                F.asMap(NodeAttributeViewWalker.NAME_FILTER, "name"));

            assertEquals(2, F.size(iter));

            iter = ((FiltrableSystemView<NodeAttributeView>)view)
                .iterator(F.asMap(NodeAttributeViewWalker.NODE_ID_FILTER, ignite0.cluster().localNode().id(),
                    NodeAttributeViewWalker.NAME_FILTER, "name"));

            assertEquals(1, F.size(iter));
        }
    }

    /** */
    @Test
    public void testNodeMetrics() throws Exception {
        long ts = U.currentTimeMillis();

        try (IgniteEx ignite0 = startGrid(0); IgniteEx ignite1 = startGrid(1)) {
            awaitPartitionMapExchange();

            SystemView<NodeMetricsView> view = ignite0.context().systemView().view(NODE_METRICS_SYS_VIEW);

            assertEquals(2, view.size());
            assertEquals(1, F.size(view.iterator(), row -> row.nodeId().equals(ignite0.cluster().localNode().id())));
            assertEquals(1, F.size(view.iterator(), row -> row.nodeId().equals(ignite1.cluster().localNode().id())));
            assertEquals(2, F.size(view.iterator(), row -> row.lastUpdateTime().getTime() >= ts));
        }
    }

    /** */
    @Test
    public void testBaselineNodes() throws Exception {
        cleanPersistenceDir();

        try (
            IgniteEx ignite0 = startGrid(getConfiguration(getTestIgniteInstanceName(0))
                .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)))
                .setConsistentId("consId0"));
            IgniteEx ignite1 = startGrid(getConfiguration(getTestIgniteInstanceName(1))
                .setDataStorageConfiguration(new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setPersistenceEnabled(true)))
                .setConsistentId("consId1"));
        ) {
            ignite0.cluster().state(ClusterState.ACTIVE);

            ignite1.close();

            awaitPartitionMapExchange();

            SystemView<BaselineNodeView> view = ignite0.context().systemView().view(BASELINE_NODES_SYS_VIEW);

            assertEquals(2, view.size());
            assertEquals(1, F.size(view.iterator(), row -> "consId0".equals(row.consistentId()) && row.online()));
            assertEquals(1, F.size(view.iterator(), row -> "consId1".equals(row.consistentId()) && !row.online()));
        }
    }

    /** */
    @Test
    public void testBaselineNodeAttributes() throws Exception {
        cleanPersistenceDir();

        try (IgniteEx ignite = startGrid(getConfiguration()
            .setDataStorageConfiguration(
                new DataStorageConfiguration().setDefaultDataRegionConfiguration(
                    new DataRegionConfiguration().setName("pds").setPersistenceEnabled(true)
                ))
            .setUserAttributes(F.asMap("name", "val"))
            .setConsistentId("consId"))
        ) {
            ignite.cluster().state(ClusterState.ACTIVE);

            SystemView<BaselineNodeAttributeView> view = ignite.context().systemView()
                .view(BASELINE_NODE_ATTRIBUTES_SYS_VIEW);

            assertEquals(ignite.cluster().localNode().attributes().size(), view.size());

            assertEquals(1, F.size(view.iterator(), row -> "consId".equals(row.nodeConsistentId()) &&
                "name".equals(row.name()) && "val".equals(row.value())));

            // Test filtering.
            assertTrue(view instanceof FiltrableSystemView);

            Iterator<BaselineNodeAttributeView> iter = ((FiltrableSystemView<BaselineNodeAttributeView>)view)
                .iterator(F.asMap(BaselineNodeAttributeViewWalker.NODE_CONSISTENT_ID_FILTER, "consId",
                    BaselineNodeAttributeViewWalker.NAME_FILTER, "name"));

            assertEquals(1, F.size(iter));

            iter = ((FiltrableSystemView<BaselineNodeAttributeView>)view).iterator(
                F.asMap(BaselineNodeAttributeViewWalker.NODE_CONSISTENT_ID_FILTER, "consId"));

            assertEquals(1, F.size(iter, row -> "name".equals(row.name())));

            iter = ((FiltrableSystemView<BaselineNodeAttributeView>)view).iterator(
                F.asMap(BaselineNodeAttributeViewWalker.NAME_FILTER, "name"));

            assertEquals(1, F.size(iter));
        }
    }

    /** */
    private void checkViewsState(SystemView<ClusterNodeView> views, ClusterNode loc, ClusterNode rmt) {
        assertEquals(2, views.size());

        for (ClusterNodeView nodeView : views) {
            if (nodeView.nodeId().equals(loc.id()))
                checkNodeView(nodeView, loc, true);
            else
                checkNodeView(nodeView, rmt, false);
        }
    }

    /** */
    private void checkNodeView(ClusterNodeView view, ClusterNode node, boolean isLoc) {
        assertEquals(node.id(), view.nodeId());
        assertEquals(node.consistentId().toString(), view.consistentId());
        assertEquals(toStringSafe(node.addresses()), view.addresses());
        assertEquals(toStringSafe(node.hostNames()), view.hostnames());
        assertEquals(node.order(), view.nodeOrder());
        assertEquals(node.version().toString(), view.version());
        assertEquals(isLoc, view.isLocal());
        assertEquals(node.isClient(), view.isClient());
        assertEquals(node.dataCenterId(), view.dataCenterId());
        assertEquals("DC0", view.dataCenterId());
    }
}
