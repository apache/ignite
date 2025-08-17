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
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistryImpl;
import org.apache.ignite.internal.processors.metric.impl.BooleanGauge;
import org.apache.ignite.internal.util.nio.GridCommunicationClient;
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

import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.METRIC_NAME_ACQUIRING_THREADS_CNT;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.METRIC_NAME_AVG_LIFE_TIME;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.METRIC_NAME_CUR_CNT;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.METRIC_NAME_MAX_NET_IDLE_TIME;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.METRIC_NAME_MSG_QUEUE_SIZE;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.METRIC_NAME_REMOVED_CNT;
import static org.apache.ignite.spi.communication.tcp.internal.ConnectionClientPool.nodeMetricsRegName;
import static org.apache.ignite.testframework.GridTestUtils.waitForCondition;

/** Tests metrics of {@link ConnectionClientPool}. */
@RunWith(Parameterized.class)
public class CommunicationConnectionPoolMetricsTest extends GridCommonAbstractTest {
    /** */
    private static final int MIN_LOAD_THREADS = 2;

    /** */
    private volatile long maxConnIdleTimeout = TcpCommunicationSpi.DFLT_IDLE_CONN_TIMEOUT;

    /** */
    private volatile int createClientDelay;

    /** */
    @Parameterized.Parameter(0)
    public int connsPerNode;

    /** */
    @Parameterized.Parameter(1)
    public boolean pairedConns;

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
            F.asList(1, 4), // Connections per node.
            F.asList(false, true), // Paired connections.
            F.asList(0, 100), // Message queue limit.
            F.asList(true, false) // Use client as a load.
        );
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        TcpCommunicationSpi communicationSpi = new TcpCommunicationSpi() {
            @Override protected GridCommunicationClient createTcpClient(ClusterNode node,
                int connIdx) throws IgniteCheckedException {
                if (createClientDelay > 0)
                    U.sleep(createClientDelay);

                return super.createTcpClient(node, connIdx);
            }
        };

        communicationSpi.setConnectionsPerNode(connsPerNode)
            .setUsePairedConnections(pairedConns)
            .setIdleConnectionTimeout(maxConnIdleTimeout)
            .setMessageQueueLimit(msgQueueLimit);

        cfg.setCommunicationSpi(communicationSpi);

        cfg.setPluginProviders(new TestCommunicationMessagePluginProvider());

        return cfg;
    }

    /** */
    @Test
    public void testRemovedConnectionMetrics() throws Exception {
        maxConnIdleTimeout = 1000;

        Ignite srvr = startGridsMultiThreaded(2);
        Ignite cli = startClientGrid(G.allGrids().size());

        Ignite ldr = clientLdr ? cli : srvr;

        GridMetricManager metricsMgr = ((IgniteEx)ldr).context().metric();
        AtomicBoolean runFlag = new AtomicBoolean(true);
        TestMessage msg = new TestMessage();

        IgniteInternalFuture<?> loadFut = runLoad(ldr, runFlag, () -> msg, null);

        // Wait until all connections are created and used.
        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(node.cluster().localNode().id()));

            assertTrue(waitForCondition(
                () -> {
                    IntMetric m = mreg.findMetric(METRIC_NAME_CUR_CNT);

                    return m != null && m.value() == connsPerNode;
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

            assertTrue(waitForCondition(() -> mreg.<LongMetric>findMetric(METRIC_NAME_REMOVED_CNT).value() == connsPerNode,
                getTestTimeout()));
        }

        dumpMetrics(ldr);
    }

    /** */
    @Test
    public void testIdleRemovedConnectionMetricsUnderLazyLoad() throws Exception {
        maxConnIdleTimeout = 10;

        Ignite srvr = startGridsMultiThreaded(2);
        Ignite cli = startClientGrid(G.allGrids().size());

        Ignite ldr = clientLdr ? cli : srvr;

        GridMetricManager metricsMgr = ((IgniteEx)ldr).context().metric();
        AtomicBoolean runFlag = new AtomicBoolean(true);
        Message msg = new TestMessage();

        IgniteInternalFuture<?> loadFut = runLoad(ldr, runFlag, () -> msg, null, maxConnIdleTimeout, maxConnIdleTimeout * 4);

        // Wait until all connections are created and used.
        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(node.cluster().localNode().id()));

            assertTrue(waitForCondition(
                () -> {
                    LongMetric m = mreg.findMetric(METRIC_NAME_REMOVED_CNT);

                    return m != null && m.value() >= connsPerNode * 4L;
                },
                getTestTimeout())
            );
        }

        // Ensure that the loading is ok.
        assertTrue(runFlag.get());

        runFlag.set(false);
        loadFut.get(getTestTimeout());

        dumpMetrics(ldr);
    }

    /** */
    @Test
    public void testMetricsBasics() throws Exception {
        int preloadCnt = 300;
        int srvrCnt = 3;

        Ignite srvr = startGridsMultiThreaded(srvrCnt);
        Ignite cli = startClientGrid(G.allGrids().size());

        Ignite ldr = clientLdr ? cli : srvr;

        GridMetricManager metricsMgr = ((IgniteEx)ldr).context().metric();
        MetricRegistryImpl mreg0 = metricsMgr.registry(ConnectionClientPool.SHARED_METRICS_REGISTRY_NAME);

        assertEquals(connsPerNode, mreg0.<IntMetric>findMetric(ConnectionClientPool.METRIC_NAME_POOL_SIZE).value());
        assertEquals(pairedConns, mreg0.<BooleanGauge>findMetric(ConnectionClientPool.METRIC_NAME_PAIRED_CONNS).value());

        AtomicBoolean runFlag = new AtomicBoolean(true);
        AtomicLong loadCnt = new AtomicLong(preloadCnt);
        TestMessage msg = new TestMessage();

        long loadMillis0 = System.currentTimeMillis();

        IgniteInternalFuture<?> loadFut = runLoad(ldr, runFlag, () -> msg, loadCnt);

        assertTrue(waitForCondition(() -> loadCnt.get() <= 0 || !runFlag.get(), getTestTimeout(), 25));

        long loadMillis1 = System.currentTimeMillis() - loadMillis0;

        // Ensure that preloaded without a failure.
        assertTrue(runFlag.get());

        long checkPeriod = U.nanosToMillis(ConnectionClientPool.METRICS_UPDATE_THRESHOLD / 3);

        // Check metrics.
        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            UUID nodeId = node.cluster().localNode().id();

            MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(nodeId));

            // We assume that entire pool was used at least once.
            assertTrue(waitForCondition(() -> connsPerNode == mreg.<IntMetric>findMetric(METRIC_NAME_CUR_CNT).value(),
                getTestTimeout(), checkPeriod));

            // Connections should not be idle under a heavy load.
            assertTrue(waitForCondition(() -> mreg.<LongMetric>findMetric(METRIC_NAME_MAX_NET_IDLE_TIME).value() < 50,
                getTestTimeout(), checkPeriod));

            assertTrue(waitForCondition(() -> mreg.<LongMetric>findMetric(METRIC_NAME_AVG_LIFE_TIME).value() > loadMillis1,
                getTestTimeout(), checkPeriod));

            // Default connection idle and write timeouts are large enough. Connections should not be failed/deleted.
            assertEquals(0, mreg.<LongMetric>findMetric(METRIC_NAME_REMOVED_CNT).value());
        }

        // Current connection implementations are async.
        assertEquals(true, mreg0.<BooleanGauge>findMetric(ConnectionClientPool.METRIC_NAME_ASYNC_CONNS).value());

        dumpMetrics(ldr);

        // Check node metrics are cleared if a node stops.
        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            // Keep last server node.
            if (!node.cluster().localNode().isClient() && --srvrCnt == 0)
                break;

            UUID nodeId = node.cluster().localNode().id();

            assertTrue(G.stop(node.name(), true));

            assertTrue(waitForCondition(() -> {
                MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(nodeId));

                return mreg == null || !mreg.iterator().hasNext();
            }, getTestTimeout()));
        }

        runFlag.set(false);
        loadFut.get(getTestTimeout());

        // Ensure that all the possible nodes are stopped.
        assertTrue(waitForCondition(() -> ldr.cluster().nodes().size() == (clientLdr ? 2 : 1), getTestTimeout()));
    }

    /** Simulates delay/concurrency of connections acquire. */
    @Test
    public void testAcquiringThreadsCntMetric() throws Exception {
        // Forces quick connection removing and recreating.
        maxConnIdleTimeout = 1;
        createClientDelay = 50;

        Ignite srvr = startGridsMultiThreaded(2);
        Ignite cli = startClientGrid(G.allGrids().size());

        Ignite ldr = clientLdr ? cli : srvr;

        GridMetricManager metricsMgr = ((IgniteEx)ldr).context().metric();
        AtomicBoolean runFlag = new AtomicBoolean(true);

        IgniteInternalFuture<?> monFut = GridTestUtils.runAsync(() -> {
            while (runFlag.get()) {
                for (Ignite node : G.allGrids()) {
                    if (node == ldr)
                        continue;

                    MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(node.cluster().localNode().id()));

                    IntMetric m = mreg.findMetric(METRIC_NAME_ACQUIRING_THREADS_CNT);

                    if (m != null && m.value() >= MIN_LOAD_THREADS)
                        runFlag.set(false);
                }

                U.sleep(1);
            }
        });

        IgniteInternalFuture<?> loadFut = runLoad(ldr, runFlag, () -> new TestMessage((int)maxConnIdleTimeout * 3), null);

        monFut.get(getTestTimeout());

        createClientDelay = 0;

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

        long checkPeriod = U.nanosToMillis(ConnectionClientPool.METRICS_UPDATE_THRESHOLD / 3);

        // Check metrics.
        for (Ignite node : G.allGrids()) {
            if (node == ldr)
                continue;

            MetricRegistryImpl mreg = metricsMgr.registry(nodeMetricsRegName(node.cluster().localNode().id()));

            assertTrue(waitForCondition(
                () -> mreg.<IntMetric>findMetric(METRIC_NAME_MSG_QUEUE_SIZE).value() >= Math.max(10, msgQueueLimit),
                getTestTimeout(), checkPeriod)
            );
        }

        writeDelay.set(0);

        dumpMetrics(ldr);

        runFlag.set(false);
        loadFut.get(getTestTimeout());
    }

    /** */
    private IgniteInternalFuture<?> runLoad(
        Ignite ldrNode,
        AtomicBoolean keepLoadingFlag,
        Supplier<Message> msgSupplier,
        @Nullable AtomicLong preloadCnt,
        long minIterationDelayMs,
        long maxIterationDelayMs
    ) {
        assert minIterationDelayMs >= 0 && maxIterationDelayMs >= minIterationDelayMs;

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

                    if (maxIterationDelayMs > 0)
                        U.sleep(ThreadLocalRandom.current().nextLong(minIterationDelayMs, maxIterationDelayMs + 1));
                }
            }
            catch (Throwable ignored) {
                // No-op.
                keepLoadingFlag.set(false);
            }
        }, Math.max(MIN_LOAD_THREADS, connsPerNode + connsPerNode / 2), "testLoader");
    }

    /** */
    private IgniteInternalFuture<?> runLoad(
        Ignite ldrNode,
        AtomicBoolean keepLoadingFlag,
        Supplier<Message> msgSupplier,
        @Nullable AtomicLong preloadCnt
    ) {
        return runLoad(ldrNode, keepLoadingFlag, msgSupplier, preloadCnt, 1, 3);
    }

    /** */
    private static void dumpMetrics(Ignite ldr) {
        if (!log.isInfoEnabled())
            return;

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
    public static class TestCommunicationMessagePluginProvider extends AbstractTestPluginProvider {
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
