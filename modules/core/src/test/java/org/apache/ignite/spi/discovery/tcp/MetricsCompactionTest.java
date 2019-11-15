/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.discovery.tcp;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.failure.NoOpFailureHandler;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryMetricsUpdateMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryRingLatencyCheckMessage;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 * Checks the metric messages processing in cases related to overflowing.
 */
public class MetricsCompactionTest extends GridCommonAbstractTest {
    /** Test uuid. */
    private static final UUID TEST_UUID = UUID.randomUUID();

    /**
     * Latches for blocking disco messages on a sender.
     */
    private final ConcurrentMap<Integer, CountDownLatch> latches = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setFailureHandler(new NoOpFailureHandler());
        cfg.setMetricsUpdateFrequency(100_000);
        cfg.setClientFailureDetectionTimeout(100_000);
        cfg.setFailureDetectionTimeout(100_000);

        cfg.setDiscoverySpi(new TestTcpDiscoverySpi(igniteInstanceName));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        super.afterTest();

        latches.entrySet().forEach(e -> e.getValue().countDown());

        stopAllGrids(true);
    }

    /**
     * Checks that message deduplication works and submit only last messages to the next receiver.
     */
    @Test
    public void testMessageShouldDeduplicate() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteEx srv = startGrid(1);
        TcpDiscoverySpi srvSpi = (TcpDiscoverySpi)srv.configuration().getDiscoverySpi();

        AtomicBoolean record = new AtomicBoolean(false);
        List<TcpDiscoveryMetricsUpdateMessage> records = Collections.synchronizedList(new ArrayList<>());

        srvSpi.addSendMessageListener(msg -> {
            if (msg instanceof TcpDiscoveryMetricsUpdateMessage) {
                try {
                    if (record.get())
                        records.add((TcpDiscoveryMetricsUpdateMessage)msg);

                    latches.get(1).await();
                }
                catch (InterruptedException ignore) {
                }
            }
        });

        latches.put(1, new CountDownLatch(1)); // Blocks srv discovery for sending

        TcpDiscoveryMetricsUpdateMessage msgLap1T1 = createMetricsMessage(crd); // Lap 0, time 1
        msgLap1T1.setMetrics(srv.localNode().id(), new ClusterMetricsSnapshot()); // Lap 1

        sendDiscoMessage(srvSpi, new TcpDiscoveryRingLatencyCheckMessage(crd.localNode().id(), 2)); // Dummy message for blocking GridWorker.
        sendDiscoMessage(srvSpi, msgLap1T1);
        sendDiscoMessage(srvSpi, createMetricsMessage(crd));

        List<TcpDiscoveryMetricsUpdateMessage> metricMsgT1 = metricsMessages(extractQueue(srvSpi));

        assertEquals(metricMsgT1.size(), 2);
        assertTrue(metricMsgT1.stream().anyMatch(m -> m.passedLaps(srv.localNode().id()) == 1));
        assertTrue(metricMsgT1.stream().anyMatch(m -> m.passedLaps(srv.localNode().id()) == 0));

        // Send two duplicates with new version of data.
        TcpDiscoveryMetricsUpdateMessage msgLap1T2 = createMetricsMessage(crd);
        msgLap1T2.setMetrics(srv.localNode().id(), new ClusterMetricsSnapshot());
        msgLap1T2.setMetrics(TEST_UUID, new ClusterMetricsSnapshot()); // Marker

        TcpDiscoveryMetricsUpdateMessage msg0T2 = createMetricsMessage(crd);
        msg0T2.setMetrics(TEST_UUID, new ClusterMetricsSnapshot());

        sendDiscoMessage(srvSpi, msg0T2);
        sendDiscoMessage(srvSpi, msgLap1T2);

        List<TcpDiscoveryMetricsUpdateMessage> metricMsgT2 = metricsMessages(extractQueue(srvSpi));

        assertEquals(metricMsgT2.size(), 2);
        assertTrue(metricMsgT2.stream().anyMatch(m -> m.passedLaps(srv.localNode().id()) == 1));
        assertTrue(metricMsgT2.stream().anyMatch(m -> m.passedLaps(srv.localNode().id()) == 0));

        record.set(true);
        latches.put(0, new CountDownLatch(1));
        latches.get(1).countDown();

        assertTrue(GridTestUtils.waitForCondition(() -> records.size() == 2, 10_000));
        assertTrue(records.stream().anyMatch(m -> m.passedLaps(srv.localNode().id()) == 1 && m.metrics().containsKey(TEST_UUID)));
        assertTrue(records.stream().anyMatch(m -> m.passedLaps(srv.localNode().id()) == 0 && m.metrics().containsKey(TEST_UUID)));
    }

    /**
     *
     */
    @Test
    public void testQueueDoesNotContainMoreThanTwoMetricMessages() throws Exception {
        IgniteEx crd = startGrid(0);

        IgniteEx srv = startGrid(1);
        TcpDiscoverySpi srvSpi = (TcpDiscoverySpi)srv.configuration().getDiscoverySpi();

        AtomicBoolean record = new AtomicBoolean(false);
        List<TcpDiscoveryMetricsUpdateMessage> records = Collections.synchronizedList(new ArrayList<>());

        srvSpi.addSendMessageListener(msg -> {
            if (msg instanceof TcpDiscoveryMetricsUpdateMessage) {
                try {
                    if (record.get())
                        records.add((TcpDiscoveryMetricsUpdateMessage)msg);

                    latches.get(1).await();
                }
                catch (InterruptedException ignore) {
                }
            }
        });

        latches.put(1, new CountDownLatch(1)); // Blocks srv discovery for sending

        TcpDiscoveryMetricsUpdateMessage msgLap1T1 = createMetricsMessage(crd); // Lap 0, time 1
        msgLap1T1.setMetrics(srv.localNode().id(), new ClusterMetricsSnapshot()); // Lap 1

        sendDiscoMessage(srvSpi, new TcpDiscoveryRingLatencyCheckMessage(crd.localNode().id(), 2)); // Dummy message for blocking GridWorker.
        sendDiscoMessage(srvSpi, msgLap1T1);
        sendDiscoMessage(srvSpi, createMetricsMessage(crd));

        List<TcpDiscoveryMetricsUpdateMessage> metricMsgT1 = metricsMessages(extractQueue(srvSpi));

        assertEquals(metricMsgT1.size(), 2);
        assertTrue(metricMsgT1.stream().anyMatch(m -> m.passedLaps(srv.localNode().id()) == 1));
        assertTrue(metricMsgT1.stream().anyMatch(m -> m.passedLaps(srv.localNode().id()) == 0));

        // Send two duplicates with new version of data.
        TcpDiscoveryMetricsUpdateMessage msgLap1T2 = createMetricsMessage(crd);
        msgLap1T2.setMetrics(srv.localNode().id(), new ClusterMetricsSnapshot());
        msgLap1T2.setMetrics(TEST_UUID, new ClusterMetricsSnapshot()); // Marker

        TcpDiscoveryMetricsUpdateMessage msg0T2 = createMetricsMessage(crd);
        msg0T2.setMetrics(TEST_UUID, new ClusterMetricsSnapshot());

        for (int i = 0; i < 10; i++) {
            sendDiscoMessage(srvSpi, msg0T2);
            sendDiscoMessage(srvSpi, msgLap1T2);
        }

        List<TcpDiscoveryMetricsUpdateMessage> metricMsgT2 = metricsMessages(extractQueue(srvSpi));

        assertEquals(metricMsgT2.size(), 2);
        assertTrue(metricMsgT2.stream().anyMatch(m -> m.passedLaps(srv.localNode().id()) == 1));
        assertTrue(metricMsgT2.stream().anyMatch(m -> m.passedLaps(srv.localNode().id()) == 0));
    }

    /**
     * @param discoverySpi Discovery spi.
     * @param msg Message.
     */
    private void sendDiscoMessage(TcpDiscoverySpi discoverySpi,
        TcpDiscoveryAbstractMessage msg) throws IgniteCheckedException {
        ServerImpl srvImpl = U.field(discoverySpi, "impl");
        Object msgWorker = U.field(srvImpl, "msgWorker");

        U.invoke(null, msgWorker, "addMessage", new Class[] {TcpDiscoveryAbstractMessage.class}, msg);
    }

    /**
     * Creates empty {@link TcpDiscoveryMetricsUpdateMessage}.
     */
    private TcpDiscoveryMetricsUpdateMessage createMetricsMessage(IgniteEx node) {
        TcpDiscoveryMetricsUpdateMessage msg = new TcpDiscoveryMetricsUpdateMessage(node.localNode().id());
        msg.verify(node.localNode().id());
        msg.senderNodeId(node.localNode().id());

        return msg;
    }

    /**
     * Extracts {@link TcpDiscoveryMetricsUpdateMessage} from disco queue.
     */
    private List<TcpDiscoveryMetricsUpdateMessage> metricsMessages(BlockingDeque queue) {
        return (List<TcpDiscoveryMetricsUpdateMessage>)queue.stream()
            .filter(msg -> msg instanceof TcpDiscoveryMetricsUpdateMessage)
            .collect(Collectors.toList());
    }

    /**
     * Extracts message worker queue from {@link TcpDiscoverySpi}.
     */
    private BlockingDeque extractQueue(TcpDiscoverySpi discoSpi) {
        ServerImpl srvImpl = U.field(discoSpi, "impl");
        Object msgWorker = U.field(srvImpl, "msgWorker");

        return U.field(msgWorker, "queue");
    }

    /**
     *
     */
    private class TestTcpDiscoverySpi extends TcpDiscoverySpi {
        /** Instance name. */
        private final String instanceName;

        /**
         * @param instanceName Instance name.
         */
        private TestTcpDiscoverySpi(String instanceName) {
            this.instanceName = instanceName;
        }

        /** {@inheritDoc} */
        @Override protected int readReceipt(Socket sock, long timeout) throws IOException {
            CountDownLatch latch = latches.get(getTestIgniteInstanceIndex(instanceName));

            if (latch != null) {
                try {
                    latch.await();
                }
                catch (InterruptedException ignore) {
                }
            }

            return super.readReceipt(sock, timeout);
        }
    }
}
