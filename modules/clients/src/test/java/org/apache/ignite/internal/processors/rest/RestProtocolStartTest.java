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

package org.apache.ignite.internal.processors.rest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.ConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.PluginProvider;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;

import static org.apache.ignite.configuration.ConnectorConfiguration.DFLT_TCP_PORT;
import static org.apache.ignite.internal.processors.rest.AbstractRestProcessorSelfTest.LOC_HOST;
import static org.apache.ignite.internal.processors.rest.protocols.tcp.GridTcpRestProtocol.REST_CONNECTOR_METRIC_REGISTRY_NAME;
import static org.apache.ignite.internal.util.nio.GridNioServer.SESSIONS_CNT_METRIC_NAME;
import static org.apache.ignite.testframework.GridTestUtils.assertThrowsWithCause;

/**
 * Test of start rest protocol and stop node.
 */
public class RestProtocolStartTest extends GridCommonAbstractTest {
    /** */
    private static final CountDownLatch cliConnected = new CountDownLatch(1);

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName)
            .setConnectorConfiguration(new ConnectorConfiguration())
            .setPluginProviders(new TestRestProcessorProvider());
    }

    /** */
    @Test
    public void test() throws Exception {
        IgniteInternalFuture startFut = GridTestUtils.runAsync(
            () -> assertThrowsWithCause(() -> startGrid(0), IgniteCheckedException.class));

        FailingGridRestProtocol.restStarted.await();

        IgniteInternalFuture<Object> cliFut = GridTestUtils.runAsync(() -> {
            try (TestBinaryClient client = new TestBinaryClient(LOC_HOST, DFLT_TCP_PORT)) {
                // No-op.
            }
            catch (RuntimeException e) {
                if (!"Client disconnected".equals(e.getMessage()))
                    throw e;
            }
        });

        IntMetric restSessions = IgnitionEx.gridx(getTestIgniteInstanceName(0)).context().metric()
            .registry(REST_CONNECTOR_METRIC_REGISTRY_NAME).findMetric(SESSIONS_CNT_METRIC_NAME);

        GridTestUtils.waitForCondition(() -> restSessions.value() > 0, getTestTimeout());

        cliConnected.countDown();

        try {
            startFut.get(10_000);
        }
        catch (IgniteFutureTimeoutCheckedException e) {
            fail("Node has hang.");
        }

        cliFut.get();
    }

    /** Test implementation of {@link PluginProvider} for obtaining {@link TestGridRestProcessorImpl}. */
    private static class TestRestProcessorProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "TEST_REST_PROCESSOR";
        }

        /** {@inheritDoc} */
        @Nullable @Override public Object createComponent(PluginContext ctx, Class cls) {
            if (cls.equals(IgniteRestProcessor.class))
                return new TestGridRestProcessorImpl(((IgniteEx)ctx.grid()).context());

            return null;
        }
    }

    /** Test no-op implementation of {@link IgniteRestProcessor}. */
    private static class TestGridRestProcessorImpl extends GridRestProcessor {
        /** @param ctx Context. */
        public TestGridRestProcessorImpl(GridKernalContext ctx) {
            super(ctx);
        }

        /** {@inheritDoc} */
        @Override public void start() throws IgniteCheckedException {
            Collection<GridRestProtocol> protos = U.field(this, "protos");

            ArrayList<GridRestProtocol> testProtos = new ArrayList<>();

            testProtos.add(new FailingGridRestProtocol());
            testProtos.addAll(protos);

            protos.clear();
            protos.addAll(testProtos);

            super.start();
        }
    }

    /** */
    private static class FailingGridRestProtocol implements GridRestProtocol {
        /** */
        private static final CountDownLatch restStarted = new CountDownLatch(1);

        /** {@inheritDoc} */
        @Override public String name() {
            return "FailingGridRestProtocol";
        }

        /** {@inheritDoc} */
        @Override public Collection<IgniteBiTuple<String, Object>> getProperties() {
            return Collections.emptyList();
        }

        /** {@inheritDoc} */
        @Override public void start(GridRestProtocolHandler hnd) { }

        /** {@inheritDoc} */
        @Override public void onKernalStart() { }

        /** {@inheritDoc} */
        @Override public void stop() { }

        /** {@inheritDoc} */
        @Override public void onProcessorStart() {
            restStarted.countDown();

            try {
                cliConnected.await();
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            throw new IgniteException("Test exception");
        }
    }
}
