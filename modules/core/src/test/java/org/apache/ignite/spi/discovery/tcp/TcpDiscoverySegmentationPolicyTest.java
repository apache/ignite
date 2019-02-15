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

package org.apache.ignite.spi.discovery.tcp;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Tests for segmentation policy and failure handling in {@link TcpDiscoverySpi}.
 */
public class TcpDiscoverySegmentationPolicyTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Default failure handler invoked. */
    private static volatile boolean dfltFailureHndInvoked;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (igniteInstanceName.endsWith("2"))
            cfg.setFailureHandler(new TestFailureHandler());

        // Disable recovery
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setConnectionRecoveryTimeout(0);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testStopOnSegmentation() throws Exception {
        startGrids(NODES_CNT);

        IgniteEx ignite1 = grid(1);
        IgniteEx ignite2 = grid(2);

        ((TcpDiscoverySpi)ignite1.configuration().getDiscoverySpi()).brakeConnection();
        ((TcpDiscoverySpi)ignite2.configuration().getDiscoverySpi()).brakeConnection();

        waitForTopology(2);

        assertFalse(dfltFailureHndInvoked);

        Collection<ClusterNode> nodes = ignite1.cluster().forServers().nodes();

        assertEquals(2, nodes.size());
        assertTrue(nodes.containsAll(Arrays.asList(((IgniteKernal)ignite(0)).localNode(), ((IgniteKernal)ignite(1)).localNode())));

        System.out.println();
    }

    /**
     * Test failure handler.
     */
    private static class TestFailureHandler extends AbstractFailureHandler {
        /** {@inheritDoc} */
        @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
            dfltFailureHndInvoked = true;

            return true;
        }
    }
}
