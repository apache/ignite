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

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.processors.metric.impl.MetricUtils;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.plugin.AbstractTestPluginProvider;
import org.apache.ignite.plugin.ExtensionRegistry;
import org.apache.ignite.plugin.PluginContext;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFactoryProvider;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.GridTestMessage;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME;

/**
 * Test for TcpCommunicationSpi statistics.
 */
public class TcpCommunicationStatisticsTest extends GridCommonAbstractTest {
    /** Mutex. */
    private final Object mux = new Object();

    /** */
    private final CountDownLatch latch = new CountDownLatch(1);

    /**
     * CommunicationSPI synchronized by {@code mux}.
     */
    private class SynchronizedCommunicationSpi extends TcpCommunicationSpi {
        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
            synchronized (mux) {
                super.sendMessage(node, msg);
            }
        }

        /** {@inheritDoc} */
        @Override public void sendMessage(ClusterNode node, Message msg,
            IgniteInClosure<IgniteException> ackC) throws IgniteSpiException {
            synchronized (mux) {
                super.sendMessage(node, msg, ackC);
            }
        }

        /** {@inheritDoc} */
        @Override protected void notifyListener(UUID sndId, Message msg, IgniteRunnable msgC) {
            super.notifyListener(sndId, msg, msgC);

            if (msg instanceof GridIoMessage && ((GridIoMessage)msg).message() instanceof GridTestMessage)
                latch.countDown();
        }
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        cfg.setConsistentId(igniteInstanceName);

        TcpCommunicationSpi spi = new SynchronizedCommunicationSpi();

        cfg.setCommunicationSpi(spi);

        cfg.setPluginProviders(new TestPluginProvider());

        return cfg;
    }

    /**
     * Gets TcpCommunicationSpiMBean for given node.
     *
     * @param nodeIdx Node index.
     * @return MBean instance.
     */
    private TcpCommunicationSpiMBean mbean(int nodeIdx) {
        return getMxBean(getTestIgniteInstanceName(nodeIdx), "SPIs",
            SynchronizedCommunicationSpi.class, TcpCommunicationSpiMBean.class);
    }

    /**
     * @throws Exception If failed.
     */
    @SuppressWarnings("ConstantConditions")
    @Test
    public void testStatistics() throws Exception {
        startGrids(2);

        try {
            Object node0consistentId = grid(0).localNode().consistentId();
            Object node1consistentId = grid(1).localNode().consistentId();

            String node0regName = MetricUtils.metricName(
                COMMUNICATION_METRICS_GROUP_NAME,
                node0consistentId.toString()
            );

            String node1regName = MetricUtils.metricName(
                COMMUNICATION_METRICS_GROUP_NAME,
                node1consistentId.toString()
            );

            // Send custom message from node0 to node1.
            grid(0).context().io().sendToGridTopic(grid(1).cluster().localNode(), GridTopic.TOPIC_IO_TEST, new GridTestMessage(), GridIoPolicy.PUBLIC_POOL);

            latch.await(10, TimeUnit.SECONDS);

            ClusterGroup clusterGrpNode1 = grid(0).cluster().forNodeId(grid(1).localNode().id());

            // Send job from node0 to node1.
            grid(0).compute(clusterGrpNode1).call(new IgniteCallable<Boolean>() {
                @Override public Boolean call() throws Exception {
                    return Boolean.TRUE;
                }
            });

            synchronized (mux) {
                TcpCommunicationSpiMBean mbean0 = mbean(0);
                TcpCommunicationSpiMBean mbean1 = mbean(1);

                Map<UUID, Long> msgsSentByNode0 = mbean0.getSentMessagesByNode();
                Map<UUID, Long> msgsSentByNode1 = mbean1.getSentMessagesByNode();
                Map<UUID, Long> msgsReceivedByNode0 = mbean0.getReceivedMessagesByNode();
                Map<UUID, Long> msgsReceivedByNode1 = mbean1.getReceivedMessagesByNode();

                UUID nodeId0 = grid(0).localNode().id();
                UUID nodeId1 = grid(1).localNode().id();

                assertEquals(msgsReceivedByNode0.get(nodeId1).longValue(), mbean0.getReceivedMessagesCount());
                assertEquals(msgsReceivedByNode1.get(nodeId0).longValue(), mbean1.getReceivedMessagesCount());
                assertEquals(msgsSentByNode0.get(nodeId1).longValue(), mbean0.getSentMessagesCount());
                assertEquals(msgsSentByNode1.get(nodeId0).longValue(), mbean1.getSentMessagesCount());

                assertEquals(mbean0.getSentMessagesCount(), mbean1.getReceivedMessagesCount());
                assertEquals(mbean1.getSentMessagesCount(), mbean0.getReceivedMessagesCount());

                Map<String, Long> msgsSentByType0 = mbean0.getSentMessagesByType();
                Map<String, Long> msgsSentByType1 = mbean1.getSentMessagesByType();
                Map<String, Long> msgsReceivedByType0 = mbean0.getReceivedMessagesByType();
                Map<String, Long> msgsReceivedByType1 = mbean1.getReceivedMessagesByType();

                // Node0 sent exactly the same types and count of messages as node1 received.
                assertEquals(msgsSentByType0, msgsReceivedByType1);

                // Node1 sent exactly the same types and count of messages as node0 received.
                assertEquals(msgsSentByType1, msgsReceivedByType0);

                assertEquals(1, msgsSentByType0.get(GridTestMessage.class.getName()).longValue());
                assertEquals(1, msgsReceivedByType1.get(GridTestMessage.class.getName()).longValue());

                MetricRegistry mreg0 = grid(0).context().metric().registry(node1regName);
                MetricRegistry mreg1 = grid(1).context().metric().registry(node0regName);

                LongAdderMetric sentMetric = mreg0.findMetric(SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME);
                assertNotNull(sentMetric);
                assertEquals(mbean0.getSentMessagesCount(), sentMetric.value());

                LongAdderMetric rcvMetric = mreg1.findMetric(RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME);
                assertNotNull(rcvMetric);
                assertEquals(mbean1.getReceivedMessagesCount(), rcvMetric.value());

                stopGrid(1);

                mreg0 = grid(0).context().metric().registry(node1regName);

                sentMetric = mreg0.findMetric(SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME);
                assertNotNull(sentMetric); // Automatically generated by MetricRegistryCreationListener.
                assertEquals(0, sentMetric.value());
            }
        }
        finally {
            stopAllGrids();
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
            registry.registerExtension(MessageFactory.class, new MessageFactoryProvider() {
                @Override public void registerAll(IgniteMessageFactory factory) {
                    factory.register(GridTestMessage.DIRECT_TYPE, GridTestMessage::new);
                }
            });
        }
    }
}
