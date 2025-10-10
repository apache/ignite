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

import java.io.IOException;
import java.io.OutputStream;
import java.net.Socket;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteState;
import org.apache.ignite.IgnitionListener;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.AbstractFailureHandler;
import org.apache.ignite.failure.FailureContext;
import org.apache.ignite.failure.StopNodeFailureHandler;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.segmentation.SegmentationPolicy;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        if (segPlc != null)
            cfg.setSegmentationPolicy(segPlc);

        cfg.setFailureHandler(new TestFailureHandler());

        // Disable recovery
        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setConnectionRecoveryTimeout(0);

        // Fastens the tests.
        cfg.setFailureDetectionTimeout(3000);

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
        AtomicBoolean segmented = new AtomicBoolean();

        G.addListener(new IgnitionListener() {
            @Override public void onStateChange(@Nullable String name, IgniteState state) {
                if (state == IgniteState.STOPPED_ON_SEGMENTATION)
                    segmented.set(true);
            }
        });

        startGrid(0);

        AtomicBoolean netLost = new AtomicBoolean();

        /** */
        class TestDiscoverySpi extends TcpDiscoverySpi {
            /** {@inheritDoc} */
            @Override protected void writeToSocket(Socket sock, OutputStream out, TcpDiscoveryAbstractMessage msg,
                long timeout) throws IOException, IgniteCheckedException {
                U.sleep(50);

                if (netLost.get())
                    throw new IOException("Text error");

                super.writeToSocket(sock, out, msg, timeout);
            }
        }

        for (int i = 1; i < NODES_CNT; i++) {
            IgniteConfiguration cfg = getConfiguration(getTestIgniteInstanceName(i));

            TcpDiscoveryIpFinder ipFinder = ((TcpDiscoverySpi)cfg.getDiscoverySpi()).getIpFinder();

            TcpDiscoverySpi discoverySpi = new TestDiscoverySpi();
            discoverySpi.setIpFinder(ipFinder);

            cfg.setDiscoverySpi(discoverySpi);

            startGrid(cfg);
        }

        assert NODES_CNT > 2;

        IgniteEx ignite1 = grid(1);
        IgniteEx ignite2 = grid(2);

        netLost.set(true);
        ((TcpDiscoverySpi)ignite1.configuration().getDiscoverySpi()).brakeConnection();
        ((TcpDiscoverySpi)ignite2.configuration().getDiscoverySpi()).brakeConnection();

        waitForCondition(() -> G.allGrids().size() < NODES_CNT, getTestTimeout());

        assertTrue("Segmentation was not happened.", segmented.get());

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
