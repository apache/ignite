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

package org.apache.ignite.spi.discovery.tcp;

import java.util.Arrays;
import java.util.Collection;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.FailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

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
    private static class TestFailureHandler implements FailureHandler {
        /** {@inheritDoc} */
        @Override public boolean onFailure(Ignite ignite, FailureContext failureCtx) {
            dfltFailureHndInvoked = true;

            return true;
        }
    }
}
