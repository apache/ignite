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

import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.apache.ignite.testframework.ListeningTestLogger;
import org.apache.ignite.testframework.LogListener;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/**
 * Tests for segmentation policy and failure handling in {@link TcpDiscoverySpi}.
 */
public class TcpDiscoverySegmentationPolicyTest extends GridCommonAbstractTest {
    /** Nodes count. */
    private static final int NODES_CNT = 3;

    /** Default failure handler invoked. */
    private static volatile boolean dfltFailureHndInvoked;

    /** Segmentation policy. */
    private static SegmentationPolicy segPlc;

    /** */
    private LogListener lsnr;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (segPlc != null)
            cfg.setSegmentationPolicy(segPlc);

        cfg.setFailureHandler(new TestFailureHandler());

        // Disable recovery
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setConnectionRecoveryTimeout(0);

        lsnr = LogListener.matches("Local node SEGMENTED").build();

        ListeningTestLogger listeningLog = new ListeningTestLogger(log);
        listeningLog.registerListener(lsnr);

        cfg.setGridLogger(listeningLog);

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void beforeTest() throws Exception {
        super.beforeTest();

        dfltFailureHndInvoked = false;
        segPlc = null;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();
    }

    /**
     * Test STOP segmentation policy.
     */
    @Test
    public void testStopOnSegmentation() throws Exception {
        segPlc = SegmentationPolicy.STOP;

        checkNodeStop(false);
    }

    /**
     * Test that default segmentation policy invokes configured failure handler.
     */
    @Test
    public void testDefaultPolicy() throws Exception {
        checkNodeStop(true);
    }

    /**
     * @param byFailureHnd By failure handler flag.
     */
    private void checkNodeStop(boolean byFailureHnd) throws Exception {
        startGrids(NODES_CNT);

        IgniteEx ignite1 = grid(1);
        IgniteEx ignite2 = grid(2);

        ((TcpDiscoverySpi)ignite1.configuration().getDiscoverySpi()).brakeConnection();
        ((TcpDiscoverySpi)ignite2.configuration().getDiscoverySpi()).brakeConnection();

        waitForCondition(() -> G.allGrids().size() < NODES_CNT, getTestTimeout() / 2);

        assertTrue("Segmentation was not happened.",
            waitForCondition(() -> lsnr.check(), getTestTimeout() / 2));

        assertTrue(byFailureHnd == dfltFailureHndInvoked);
    }

    /**
     * Test failure handler.
     */
    private static class TestFailureHandler extends AbstractFailureHandler {
        /** {@inheritDoc} */
        @Override protected boolean handle(Ignite ignite, FailureContext failureCtx) {
            dfltFailureHndInvoked = true;

            ((IgniteEx)ignite).context().failure().process(failureCtx, new StopNodeFailureHandler());

            return true;
        }
    }
}
