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

package org.apache.ignite.spi.communication.tcp;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.BooleanGauge;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.NODE_METRIC_ACTIVE_CONNS_CNT_NAME;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.NODE_METRIC_CONNS_CNT_NAME;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.NODE_METRIC_CONN_ACQUIRE_TIME_NAME;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.NODE_METRIC_REMOVED_CONNS_CNT_NAME;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.nodeMetricsRegName;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests metrics of {@link ConnectionClientPool}. */
@RunWith(Parameterized.class)
public class CommunicationConnectionPoolMetricsTest extends GridCommonAbstractTest {
    /** */
    @Parameterized.Parameter(0)
    public int connectionsPerNode;

    /** */
    @Parameterized.Parameter(1)
    public boolean pairedConnections;

    /** */
    @Parameterized.Parameter(2)
    public int msgQueueLimit;

    /** */
    @Parameterized.Parameter(3)
    public boolean clientLoader;

    /** */
    @Parameterized.Parameters(name = "connsPerNode={0}, pairedConns={1}, msgQueueLimit={2}, clientLdr={3}")
    public static Collection<Object[]> params() {
        return GridTestUtils.cartesianProduct(
            F.asList(1, 4), // Connections per node.
            F.asList(false, true), // Paired connections.
            F.asList(0, 100), // Message queue limit.
            F.asList(true, false) // Use client as a load.
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CommunicationSpi<?> communicationSpi = new TcpCommunicationSpi()
            .setConnectionsPerNode(connectionsPerNode)
            .setUsePairedConnections(pairedConnections)
            .setMessageQueueLimit(msgQueueLimit);

        cfg.setCommunicationSpi(communicationSpi);

        cfg.setPluginProviders(new TestPluginProvider());

        return cfg;
    }

    /** */
    @Test
    public void testMetrics() throws Exception {
        int srvCnt = 2;
        int clientCnt = 1;
        int preloadCnt = 500;
        int minLoadDelay = 0;
        int maxLoadDelay = 5;

        Ignite server = startGridsMultiThreaded(srvCnt);

        Ignite client = startClientGridsMultiThreaded(srvCnt, clientCnt);

        Ignite ldr = clientLoader ? client : server;

        GridMetricManager metricsMgr = ((IgniteEx)ldr).context().metric();

        MetricRegistryImpl mreg0 = metricsMgr.registry(ConnectionClientPool.SHARED_METRICS_REGISTRY_NAME);

        assertEquals(connectionsPerNode, mreg0.<IntMetric>findMetric(ConnectionClientPool.METRIC_POOL_SIZE_NAME).value());
        assertEquals(pairedConnections, mreg0.<BooleanGauge>findMetric(ConnectionClientPool.METRIC_PAIRED_CONN_NAME).value());

        AtomicBoolean runFlag = new AtomicBoolean(true);
        AtomicLong loadCnt = new AtomicLong(preloadCnt);

        IgniteInternalFuture<?> loadFut = runLoad(ldr, runFlag, loadCnt, minLoadDelay, maxLoadDelay);

        assertTrue(waitForCondition(() -> loadCnt.get() <= 0 || !runFlag.get(), getTestTimeout(), 25));

        // Ensure that preloaded without a failure.
        assertTrue(runFlag.get());

        // Check metrics.
        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(node.cluster().localNode().id()));

            // We assume that entire pool was used at least once.
            assertTrue(waitForCondition(() -> connectionsPerNode == mreg.<IntMetric>findMetric(NODE_METRIC_CONNS_CNT_NAME).value(),
                getTestTimeout(), 25));

            assertTrue(waitForCondition(() -> mreg.<LongMetric>findMetric(NODE_METRIC_CONN_ACQUIRE_TIME_NAME).value() > 0,
                getTestTimeout(), 25));

            assertTrue(waitForCondition(() -> mreg.<IntMetric>findMetric(NODE_METRIC_ACTIVE_CONNS_CNT_NAME).value() > 0,
                getTestTimeout(), 25));

            // Default connection idle timeout is large enough. Connections should not be failed/deleted.
            assertEquals(0, mreg.<LongMetric>findMetric(NODE_METRIC_REMOVED_CONNS_CNT_NAME).value());
        }

        // Current connection implementations are async.
        assertEquals(true, mreg0.<BooleanGauge>findMetric(ConnectionClientPool.METRIC_ASYNC_NAME).value());

        if (log.isInfoEnabled())
            dumpMetrics(ldr);

        runFlag.set(false);
        loadFut.get(getTestTimeout());
    }

    /** */
    private IgniteInternalFuture<?> runLoad(Ignite ldr, AtomicBoolean runFlag, AtomicLong loadCnt, int minDelay, int maxDelay) {
        assert maxDelay >= minDelay && minDelay >= 0;

        GridIoManager io = ((IgniteEx)ldr).context().io();

        return GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                while (runFlag.get()) {
                    Message msg = new TestMessage(() -> ThreadLocalRandom.current().nextLong(minDelay, maxDelay + 1));

                    for (Ignite node : G.allGrids()) {
                        if (node == ldr)
                            continue;

                        io.sendToCustomTopic(node.cluster().localNode().id(), "tt", msg, GridIoPolicy.PUBLIC_POOL);
                    }

                    if (loadCnt.get() > 0)
                        loadCnt.decrementAndGet();

                    U.sleep(ThreadLocalRandom.current().nextLong(minDelay, maxDelay + 1));
                }
            }
            catch (Throwable ignored) {
                // No-op.
                runFlag.set(false);
            }
        }, Math.max(3, connectionsPerNode), "testLoader");
    }

    /** */
    private static void dumpMetrics(Ignite ldr) {
        GridMetricManager metricsMgr = ((IgniteEx)ldr).context().metric();

        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(node.cluster().localNode().id()));

            StringBuilder b = new StringBuilder()
                .append("TCP Communication Connection Pool metrics from node ").append(ldr.cluster().localNode().order())
                .append(" to node ").append(node.cluster().localNode().order())
                .append(": ");

            for (Metric m : mreg) {
                b.append(System.lineSeparator()).append('\t');

                b.append(m.name()).append(" = ").append(m.getAsString());
            }

            log.info(b.toString());
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        stopAllGrids();
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return 120 * 1000;
    }

    /** */
    private static final class TestMessage extends GridTestMessage {
        /** */
        private final Supplier<Long> writeDelaySimulator;

        /** */
        private TestMessage(Supplier<Long> writeDelaySimulator) {
            this.writeDelaySimulator = writeDelaySimulator;
        }

        /** */
        public TestMessage() {
            this.writeDelaySimulator = null;
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            if (writeDelaySimulator != null) {
                long sleep = writeDelaySimulator.get();

                if (sleep < 1)
                    return super.writeTo(buf, writer);

                try {
                    U.sleep(sleep);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // No-op.
                }
            }

            return super.writeTo(buf, writer);
        }
    }

    /** */
    public static class TestPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "TEST_PLUGIN";
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            registry.registerExtension(MessageFactoryProvider.class, new MessageFactoryProvider() {
                @Override public void registerAll(MessageFactory factory) {
                    factory.register(GridTestMessage.DIRECT_TYPE, TestMessage::new);
                }
            });
        }
    }
}
