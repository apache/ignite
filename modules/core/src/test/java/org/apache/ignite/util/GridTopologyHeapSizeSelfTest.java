/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.util;

import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_JVM_PID;
import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Tests for calculation logic for topology heap size.
 */
@RunWith(JUnit4.class)
public class GridTopologyHeapSizeSelfTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testTopologyHeapSizeInOneJvm() throws Exception {
        try {
            ClusterNode node1 = startGrid(1).cluster().node();
            ClusterNode node2 = startGrid(2).cluster().node();

            double allSize = U.heapSize(F.asList(node1, node2), 10);

            double size1 = U.heapSize(node1, 10);

            assertEquals(size1, allSize, 1E-5);
        }
        finally {
            stopAllGrids();
        }
    }

    /** */
    @Test
    public void testTopologyHeapSizeForNodesWithDifferentPids() {
        GridTestNode node1 = getNode("123456789ABC", 1000);
        GridTestNode node2 = getNode("123456789ABC", 1001);

        double size1 = U.heapSize(node1, 10);
        double size2 = U.heapSize(node2, 10);

        double allSize = U.heapSize(F.asList((ClusterNode)node1, node2), 10);

        assertEquals(size1 + size2, allSize, 1E-5);
    }

    /** */
    @Test
    public void testTopologyHeapSizeForNodesWithDifferentMacs() {
        GridTestNode node1 = getNode("123456789ABC", 1000);
        GridTestNode node2 = getNode("CBA987654321", 1000);

        double size1 = U.heapSize(node1, 10);
        double size2 = U.heapSize(node2, 10);

        double allSize = U.heapSize(F.asList((ClusterNode)node1, node2), 10);

        assertEquals(size1 + size2, allSize, 1E-5);
    }

    /**
     * Creates test node with specified attributes.
     *
     * @param mac Node mac addresses.
     * @param pid Node PID.
     * @return Node.
     */
    private GridTestNode getNode(String mac, int pid) {
        ClusterMetricsSnapshot metrics = new ClusterMetricsSnapshot();

        metrics.setHeapMemoryMaximum(1024L * 1024 * 1024);
        metrics.setHeapMemoryInitialized(1024L * 1024 * 1024);

        GridTestNode node = new GridTestNode(UUID.randomUUID(), metrics);

        node.addAttribute(ATTR_MACS, mac);
        node.addAttribute(ATTR_JVM_PID, pid);

        return node;
    }
}
