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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.NODE_METRIC_NAME_CUR_CNT;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.NODE_METRIC_NAME_MAX_IDLE_TIME;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.NODE_METRIC_NAME_MIN_LIFE_TIME;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.NODE_METRIC_NAME_MSG_QUEUE_SIZE;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.NODE_METRIC_NAME_REMOVED_CNT;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.nodeMetricsRegName;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests metrics of {@link ConnectionClientPool}. */
@RunWith(Parameterized.class)
public class CommunicationConnectionPoolMetricsTest extends GridCommonAbstractTest {
    /** */
    private long idleTimeout = TcpCommunicationSpi.DFLT_IDLE_CONN_TIMEOUT;

    /** */
    @Parameterized.Parameter(0)
    public int connsPerNode;

    /** */
    @Parameterized.Parameter(1)
    public boolean pairedConnections;

    /** */
    @Parameterized.Parameter(2)
    public int msgQueueLimit;

    /** */
    @Parameterized.Parameter(3)
    public boolean clientLdr;

    /** */
    @Parameterized.Parameters(name = "connsPerNode={0}, pairedConns={1}, msgQueueLimit={2}, clientLdr={3}")
    public static Collection<Object[]> params() {
        return GridTestUtils.cartesianProduct(
            F.asList(4), // Connections per node.
            F.asList(false, true), // Paired connections.
            F.asList(0, 100), // Message queue limit.
            F.asList(true, false) // Use client as a load.
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        CommunicationSpi<?> communicationSpi = new TcpCommunicationSpi()
            .setConnectionsPerNode(connsPerNode)
            .setUsePairedConnections(pairedConnections)
            .setIdleConnectionTimeout(idleTimeout)
            .setMessageQueueLimit(msgQueueLimit);

        cfg.setCommunicationSpi(communicationSpi);

        cfg.setPluginProviders(new TestPluginProvider());

        return cfg;
    }

    /** */
    @Test
    public void testRemovedConnectionMetrics() throws Exception {
        idleTimeout = 500;

        Ignite server = startGridsMultiThreaded(2);
        Ignite client = startClientGrid(G.allGrids().size());

        Ignite ldr = clientLdr ? client : server;

        GridMetricManager metricsMgr = ((IgniteEx)ldr).context().metric();
        AtomicBoolean runFlag = new AtomicBoolean(true);

        IgniteInternalFuture<?> loadFut = runLoad(ldr, runFlag, TestMessage::new, null);

        // Wait until all connections are created and used.
        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(node.cluster().localNode().id()));

            assertTrue(waitForCondition(
                () -> {
                    IntMetric im = mreg.findMetric(NODE_METRIC_NAME_CUR_CNT);

                    if (im == null || connsPerNode != im.value())
                        return false;

                    LongMetric lm = mreg.findMetric(NODE_METRIC_NAME_MIN_LIFE_TIME);

                    return lm != null && lm.value() != 0;
                },
                getTestTimeout())
            );
        }

        // Ensure that the loading is ok.
        assertTrue(runFlag.get());

        runFlag.set(false);
        loadFut.get(getTestTimeout());

        // Wait until connections are closed.
        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(node.cluster().localNode().id()));

            assertTrue(waitForCondition(() -> mreg.<IntMetric>findMetric(NODE_METRIC_NAME_REMOVED_CNT).value() >= connsPerNode,
                getTestTimeout()));

            assertTrue(mreg.<LongMetric>findMetric(NODE_METRIC_NAME_MIN_LIFE_TIME).value() >=
                mreg.<LongMetric>findMetric(NODE_METRIC_NAME_MAX_IDLE_TIME).value());
        }

        dumpMetrics(ldr);
    }

    /** */
    @Test
    public void testMetricsUnderLoad() throws Exception {
        int preloadCnt = 300;

        Ignite server = startGridsMultiThreaded(2);
        Ignite client = startClientGrid(G.allGrids().size());

        Ignite ldr = clientLdr ? client : server;

        GridMetricManager metricsMgr = ((IgniteEx)ldr).context().metric();
        MetricRegistryImpl mreg0 = metricsMgr.registry(ConnectionClientPool.SHARED_METRICS_REGISTRY_NAME);

        assertEquals(connsPerNode, mreg0.<IntMetric>findMetric(ConnectionClientPool.METRIC_POOL_SIZE_NAME).value());
        assertEquals(pairedConnections, mreg0.<BooleanGauge>findMetric(ConnectionClientPool.METRIC_NAME_PAIRED_CONNS).value());

        AtomicBoolean runFlag = new AtomicBoolean(true);
        AtomicLong loadCnt = new AtomicLong(preloadCnt);

        IgniteInternalFuture<?> loadFut = runLoad(ldr, runFlag, TestMessage::new, null);

        assertTrue(waitForCondition(() -> loadCnt.get() <= 0 || !runFlag.get(), getTestTimeout(), 25));

        // Ensure that preloaded without a failure.
        assertTrue(runFlag.get());

        long checkPeriod = U.nanosToMillis(ConnectionClientPool.NODE_METRICS_UPDATE_THRESHOLD / 3);

        // Check metrics.
        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            MetricRegistryImpl poolMreg = metricsMgr.registry(nodeMetricsRegName(node.cluster().localNode().id()));

            // We assume that entire pool was used at least once.
            assertTrue(waitForCondition(() -> connsPerNode == poolMreg.<IntMetric>findMetric(NODE_METRIC_NAME_CUR_CNT).value(),
                getTestTimeout(), checkPeriod));

            assertTrue(poolMreg.<LongMetric>findMetric(NODE_METRIC_NAME_MIN_LIFE_TIME).value() >=
                poolMreg.<LongMetric>findMetric(NODE_METRIC_NAME_MAX_IDLE_TIME).value());

            assertTrue(waitForCondition(() -> poolMreg.<LongMetric>findMetric(NODE_METRIC_NAME_MAX_IDLE_TIME).value() < 50,
                getTestTimeout(), checkPeriod));

            // Default connection idle and write timeouts are large enough. Connections should not be failed/deleted.
            assertEquals(0, poolMreg.<IntMetric>findMetric(NODE_METRIC_NAME_REMOVED_CNT).value());
        }

        // Current connection implementations are async.
        assertEquals(true, mreg0.<BooleanGauge>findMetric(ConnectionClientPool.METRIC_NAME_ASYNC_CONNS).value());

        if (log.isInfoEnabled())
            dumpMetrics(ldr);

        runFlag.set(false);
        loadFut.get(getTestTimeout());
    }

    /** */
    @Test
    public void testDelayedConnectionsMetric() throws Exception {
        int preloadCnt = 500;

        Ignite server = startGridsMultiThreaded(2);
        Ignite client = startClientGrid(G.allGrids().size());

        Ignite ldr = clientLdr ? client : server;

        GridMetricManager metricsMgr = ((IgniteEx)ldr).context().metric();
        AtomicBoolean runFlag = new AtomicBoolean(true);
        AtomicLong loadCnt = new AtomicLong(preloadCnt);

        AtomicInteger writeDelay = new AtomicInteger();

        IgniteInternalFuture<?> loadFut = runLoad(
            ldr,
            runFlag,
            () -> new TestMessage(writeDelay.get()),
            loadCnt
        );

        assertTrue(waitForCondition(() -> loadCnt.get() <= 0 || !runFlag.get(), getTestTimeout(), 25));

        // Ensure that preloaded without a failure.
        assertTrue(runFlag.get());

        // Will delay message queue processing but not network i/o.
        writeDelay.set(50);

        long checkPeriod = U.nanosToMillis(ConnectionClientPool.NODE_METRICS_UPDATE_THRESHOLD / 3);

        // Check metrics.
        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            MetricRegistryImpl poolMreg = metricsMgr.registry(nodeMetricsRegName(node.cluster().localNode().id()));

            assertTrue(waitForCondition(
                () -> poolMreg.<IntMetric>findMetric(NODE_METRIC_NAME_MSG_QUEUE_SIZE).value() >= Math.max(10, msgQueueLimit),
                getTestTimeout(), checkPeriod)
            );
        }

        writeDelay.set(0);

        if (log.isInfoEnabled())
            dumpMetrics(ldr);

        runFlag.set(false);
        loadFut.get(getTestTimeout());
    }

    /** */
    private IgniteInternalFuture<?> runLoad(
        Ignite ldrNode,
        AtomicBoolean keepLoadingFlag,
        Supplier<Message> msgSupplier,
        @Nullable AtomicLong preloadCnt
    ) {
        GridIoManager io = ((IgniteEx)ldrNode).context().io();

        return GridTestUtils.runMultiThreadedAsync(() -> {
            try {
                while (keepLoadingFlag.get()) {
                    for (Ignite node : G.allGrids()) {
                        if (node == ldrNode)
                            continue;

                        Message msg = msgSupplier.get();

                        io.sendToCustomTopic(node.cluster().localNode().id(), "tt", msg, GridIoPolicy.PUBLIC_POOL);
                    }

                    if (preloadCnt != null && preloadCnt.get() > 0)
                        preloadCnt.decrementAndGet();

                    U.sleep(1);
                }
            }
            catch (Throwable ignored) {
                // No-op.
                keepLoadingFlag.set(false);
            }
        }, Math.max(2, connsPerNode), "testLoader");
    }

    /** */
    private static void dumpMetrics(Ignite ldr) {
        GridMetricManager metricsMgr = ((IgniteEx)ldr).context().metric();

        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(node.cluster().localNode().id()));

            StringBuilder b = new StringBuilder()
                .append("Pool metrics from node ").append(ldr.cluster().localNode().order())
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
    public static class TestPluginProvider extends AbstractTestPluginProvider {
        /** {@inheritDoc} */
        @Override public String name() {
            return "TEST_PLUGIN";
        }

        /** {@inheritDoc} */
        @Override public void initExtensions(PluginContext ctx, ExtensionRegistry registry) {
            registry.registerExtension(MessageFactoryProvider.class, new MessageFactoryProvider() {
                @Override public void registerAll(MessageFactory factory) {
                    factory.register(TestMessage.DIRECT_TYPE, TestMessage::new);
                }
            });
        }
    }

    /** */
    private static class TestMessage extends GridTestMessage {
        /** */
        private final int writeDelay;

        /** */
        public TestMessage(int writeDelay) {
            this.writeDelay = writeDelay;
        }

        /** */
        public TestMessage() {
            this(0);
        }

        /** {@inheritDoc} */
        @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
            if (writeDelay > 0) {
                try {
                    U.sleep(writeDelay);
                }
                catch (IgniteInterruptedCheckedException ignored) {
                    // No-op.
                }
            }

            return super.writeTo(buf, writer);
        }
    }
}
