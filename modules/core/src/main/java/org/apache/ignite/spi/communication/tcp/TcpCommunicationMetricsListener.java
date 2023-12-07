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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;
import org.apache.ignite.spi.metric.ReadOnlyMetricRegistry;

import static java.util.stream.Collectors.toMap;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.SEPARATOR;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.nio.GridNioServer.RECEIVED_BYTES_METRIC_DESC;
import static org.apache.ignite.internal.util.nio.GridNioServer.RECEIVED_BYTES_METRIC_NAME;
import static org.apache.ignite.internal.util.nio.GridNioServer.SENT_BYTES_METRIC_DESC;
import static org.apache.ignite.internal.util.nio.GridNioServer.SENT_BYTES_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_TYPE_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_TYPE_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_TYPE_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_TYPE_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_METRIC_NAME;

/**
 * Statistics for {@link org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi}.
 */
public class TcpCommunicationMetricsListener {
    /** SPI context. */
    private final IgniteSpiContext spiCtx;

    /** Current ignite instance. */
    private final Ignite ignite;

    /** Metrics registry. */
    private final MetricRegistry mreg;

    /** All registered metrics. */
    private final Set<ThreadMetrics> allMetrics = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** Thread-local metrics. */
    private final ThreadLocal<ThreadMetrics> threadMetrics = ThreadLocal.withInitial(() -> {
        ThreadMetrics metrics = new ThreadMetrics();

        allMetrics.add(metrics);

        return metrics;
    });

    /** Function to be used in {@link Map#computeIfAbsent(Object, Function)} of {@code #sentMsgsMetricsByConsistentId}. */
    private final Function<Object, LongAdderMetric> sentMsgsCntByConsistentIdMetricFactory;

    /** Function to be used in {@link Map#computeIfAbsent(Object, Function)} of {@code #rcvdMsgsMetricsByConsistentId}. */
    private final Function<Object, LongAdderMetric> rcvdMsgsCntByConsistentIdMetricFactory;

    /** Sent bytes count metric.*/
    private final LongAdderMetric sentBytesMetric;

    /** Received bytes count metric. */
    private final LongAdderMetric rcvdBytesMetric;

    /** Sent messages count metric. */
    private final LongAdderMetric sentMsgsMetric;

    /** Received messages count metric. */
    private final LongAdderMetric rcvdMsgsMetric;

    /** Counters of sent and received messages by direct type. */
    private final IntMap<IgniteBiTuple<LongAdderMetric, LongAdderMetric>> msgCntrsByType;

    /** Method to synchronize access to message type map. */
    private final Object msgTypeMapMux = new Object();

    /** Message type map. */
    private volatile Map<Short, String> msgTypeMap;

    /**
     * @param ignite Ignite instance.
     * @param spiCtx Ignite SPI context.
     */
    public TcpCommunicationMetricsListener(Ignite ignite, IgniteSpiContext spiCtx) {
        this.ignite = ignite;
        this.spiCtx = spiCtx;

        mreg = (MetricRegistry)spiCtx.getOrCreateMetricRegistry(COMMUNICATION_METRICS_GROUP_NAME);

        msgCntrsByType = createMessageCounters((IgniteMessageFactory)spiCtx.messageFactory());

        sentMsgsCntByConsistentIdMetricFactory = consistentId -> {
            String name = metricName(COMMUNICATION_METRICS_GROUP_NAME, consistentId.toString());

            return spiCtx.getOrCreateMetricRegistry(name)
                    .findMetric(SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME);
        };

        rcvdMsgsCntByConsistentIdMetricFactory = consistentId -> {
            String name = metricName(COMMUNICATION_METRICS_GROUP_NAME, consistentId.toString());

            return spiCtx.getOrCreateMetricRegistry(name)
                    .findMetric(RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME);
        };

        sentBytesMetric = mreg.longAdderMetric(SENT_BYTES_METRIC_NAME, SENT_BYTES_METRIC_DESC);
        rcvdBytesMetric = mreg.longAdderMetric(RECEIVED_BYTES_METRIC_NAME, RECEIVED_BYTES_METRIC_DESC);

        sentMsgsMetric = mreg.longAdderMetric(SENT_MESSAGES_METRIC_NAME, SENT_MESSAGES_METRIC_DESC);
        rcvdMsgsMetric = mreg.longAdderMetric(RECEIVED_MESSAGES_METRIC_NAME, RECEIVED_MESSAGES_METRIC_DESC);

        spiCtx.addMetricRegistryCreationListener(mreg -> {
            // Metrics for the specific nodes.
            if (!mreg.name().startsWith(COMMUNICATION_METRICS_GROUP_NAME + SEPARATOR))
                return;

            ((MetricRegistry)mreg).longAdderMetric(
                    SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME,
                    SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC
            );

            ((MetricRegistry)mreg).longAdderMetric(
                    RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME,
                    RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC
            );
        });
    }

    /**
     * Creates counters of sent and received messages by direct type.
     *
     * @param factory Message factory.
     * @return Counters of sent and received messages grouped by direct type.
     */
    private IntMap<IgniteBiTuple<LongAdderMetric, LongAdderMetric>> createMessageCounters(IgniteMessageFactory factory) {
        IgniteMessageFactoryImpl msgFactory = (IgniteMessageFactoryImpl)factory;

        short[] directTypes = msgFactory.registeredDirectTypes();

        IntMap<IgniteBiTuple<LongAdderMetric, LongAdderMetric>> msgCntrsByType = new IntHashMap<>(directTypes.length);

        for (short type : directTypes) {
            LongAdderMetric sentCnt =
                    mreg.longAdderMetric(sentMessagesByTypeMetricName(type), SENT_MESSAGES_BY_TYPE_METRIC_DESC);

            LongAdderMetric rcvCnt =
                    mreg.longAdderMetric(receivedMessagesByTypeMetricName(type), RECEIVED_MESSAGES_BY_TYPE_METRIC_DESC);

            msgCntrsByType.put(type, new IgniteBiTuple<>(sentCnt, rcvCnt));
        }

        return msgCntrsByType;
    }

    /** @return Metrics registry. */
    public MetricRegistry metricRegistry() {
        return mreg;
    }

    /**
     * Collects statistics for message sent by SPI.
     *
     * @param msg Sent message.
     * @param consistentId Receiver node consistent id.
     */
    public void onMessageSent(Message msg, Object consistentId) {
        assert msg != null;
        assert consistentId != null;

        if (msg instanceof GridIoMessage) {
            msg = ((GridIoMessage)msg).message();

            updateMessageTypeMap(msg);

            sentMsgsMetric.increment();

            threadMetrics.get().onMessageSent(msg, consistentId);
        }
    }

    /**
     * Collects statistics for message received by SPI.
     *
     * @param msg Received message.
     * @param consistentId Sender node consistent id.
     */
    public void onMessageReceived(Message msg, Object consistentId) {
        assert msg != null;
        assert consistentId != null;

        if (msg instanceof GridIoMessage) {
            msg = ((GridIoMessage)msg).message();

            updateMessageTypeMap(msg);

            rcvdMsgsMetric.increment();

            threadMetrics.get().onMessageReceived(msg, consistentId);
        }
    }

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    public int sentMessagesCount() {
        int res0 = (int)sentMsgsMetric.value();

        return res0 < 0 ? Integer.MAX_VALUE : res0;
    }

    /**
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    public long sentBytesCount() {
        return sentBytesMetric.value();
    }

    /**
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    public int receivedMessagesCount() {
        int res0 = (int)rcvdMsgsMetric.value();

        return res0 < 0 ? Integer.MAX_VALUE : res0;
    }

    /**
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    public long receivedBytesCount() {
        return rcvdBytesMetric.value();
    }

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> receivedMessagesByType() {
        return collectMessagesCountByType(RECEIVED_MESSAGES_BY_TYPE_METRIC_NAME + SEPARATOR);
    }

    /**
     * Gets received messages counts (grouped by node).
     *
     * @return Map containing sender nodes and respective counts.
     */
    public Map<UUID, Long> receivedMessagesByNode() {
        return collectMessagesCountByNodeId(RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME);
    }

    /**
     * Gets sent messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> sentMessagesByType() {
        return collectMessagesCountByType(SENT_MESSAGES_BY_TYPE_METRIC_NAME + SEPARATOR);
    }

    /**
     * Gets sent messages counts (grouped by node).
     *
     * @return Map containing receiver nodes and respective counts.
     */
    public Map<UUID, Long> sentMessagesByNode() {
        return collectMessagesCountByNodeId(SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME);
    }

    /** Collect messages count by type */
    protected Map<String, Long> collectMessagesCountByType(String prefix) {
        Map<String, Long> res = new HashMap<>();

        prefix = metricName(COMMUNICATION_METRICS_GROUP_NAME, prefix);

        for (Metric metric : mreg) {
            if (metric.name().startsWith(prefix)) {
                short directType = Short.parseShort(metric.name().substring(prefix.length()));

                Map<Short, String> msgTypeMap0 = msgTypeMap;

                if (msgTypeMap0 != null) {
                    String typeName = msgTypeMap0.get(directType);

                    if (typeName != null)
                        res.put(typeName, ((LongMetric)metric).value());
                }
            }
        }

        return res;
    }

    /** Collect messages count by nodeId */
    protected Map<UUID, Long> collectMessagesCountByNodeId(String metricName) {
        Map<UUID, Long> res = new HashMap<>();

        Map<String, UUID> nodesMapping = ignite.cluster().nodes().stream().collect(toMap(
            node -> node.consistentId().toString(), ClusterNode::id
        ));

        String mregPrefix = COMMUNICATION_METRICS_GROUP_NAME + SEPARATOR;

        for (ReadOnlyMetricRegistry mreg : spiCtx.metricRegistries()) {
            if (mreg.name().startsWith(mregPrefix)) {
                String nodeConsIdStr = mreg.name().substring(mregPrefix.length());

                UUID nodeId = nodesMapping.get(nodeConsIdStr);

                if (nodeId == null)
                    continue;

                res.put(nodeId, mreg.<LongMetric>findMetric(metricName).value());
            }
        }

        return res;
    }

    /**
     * Resets metrics for this instance.
     */
    public void resetMetrics() {
        rcvdMsgsMetric.reset();
        sentMsgsMetric.reset();

        sentBytesMetric.reset();
        rcvdBytesMetric.reset();

        for (Metric metric : mreg) {
            if (metric.name().startsWith(SENT_MESSAGES_BY_TYPE_METRIC_NAME))
                metric.reset();
            else if (metric.name().startsWith(RECEIVED_MESSAGES_BY_TYPE_METRIC_NAME))
                metric.reset();
        }

        for (ReadOnlyMetricRegistry mreg : spiCtx.metricRegistries()) {
            if (mreg.name().startsWith(COMMUNICATION_METRICS_GROUP_NAME + SEPARATOR)) {
                mreg.findMetric(SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME).reset();

                mreg.findMetric(RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME).reset();
            }
        }
    }

    /**
     * @param consistentId Consistent id of the node.
     */
    public void onNodeLeft(Object consistentId) {
        // Tricky part - these maps are not thread-safe. Ideally it's only required to delete one entry from each one
        // of them, but this would lead to syncs in communication worker threads. Instead, we just "clean" them so they
        // will be filled later lazily with the same data.
        for (ThreadMetrics threadMetrics : allMetrics) {
            threadMetrics.sentMsgsMetricsByConsistentId = new HashMap<>();
            threadMetrics.rcvdMsgsMetricsByConsistentId = new HashMap<>();
        }

        spiCtx.removeMetricRegistry(metricName(COMMUNICATION_METRICS_GROUP_NAME, consistentId.toString()));
    }

    /**
     * Update message type map.
     *
     * @param msg Message.
     */
    private void updateMessageTypeMap(Message msg) {
        short typeId = msg.directType();

        Map<Short, String> msgTypeMap0 = msgTypeMap;

        if (msgTypeMap0 == null || !msgTypeMap0.containsKey(typeId)) {
            synchronized (msgTypeMapMux) {
                if (msgTypeMap == null) {
                    msgTypeMap0 = new HashMap<>();

                    msgTypeMap0.put(typeId, msg.getClass().getName());

                    msgTypeMap = msgTypeMap0;
                }
                else {
                    if (!msgTypeMap.containsKey(typeId)) {
                        msgTypeMap0 = new HashMap<>(msgTypeMap);

                        msgTypeMap0.put(typeId, msg.getClass().getName());

                        msgTypeMap = msgTypeMap0;
                    }
                }
            }
        }
    }

    /**
     * Generate metric name by message direct type id.
     *
     * @param directType Direct type ID of sent message.
     * @return Metric name for sent message.
     */
    public static String sentMessagesByTypeMetricName(Short directType) {
        return metricName(SENT_MESSAGES_BY_TYPE_METRIC_NAME, directType.toString());
    }

    /**
     * Generate metric name by message direct type id.
     *
     * @param directType Direct type ID of the received message.
     * @return Metric name for the received message.
     */
    public static String receivedMessagesByTypeMetricName(Short directType) {
        return metricName(RECEIVED_MESSAGES_BY_TYPE_METRIC_NAME, directType.toString());
    }

    /**
     * Thread-local metrics.
     */
    private class ThreadMetrics {
        /** Sent messages count metrics grouped by message node consistent id. */
        volatile Map<Object, LongAdderMetric> sentMsgsMetricsByConsistentId = new HashMap<>();

        /** Received messages metrics count grouped by message node consistent id. */
        volatile Map<Object, LongAdderMetric> rcvdMsgsMetricsByConsistentId = new HashMap<>();

        /**
         * Collects statistics for message sent by SPI.
         *
         * @param msg Sent message.
         * @param consistentId Receiver node consistent id.
         */
        private void onMessageSent(Message msg, Object consistentId) {
            IgniteBiTuple<LongAdderMetric, LongAdderMetric> cnts = msgCntrsByType.get(msg.directType());

            cnts.get1().increment();

            sentMsgsMetricsByConsistentId.computeIfAbsent(consistentId, sentMsgsCntByConsistentIdMetricFactory).increment();
        }

        /**
         * Collects statistics for message received by SPI.
         * @param msg Received message.
         * @param consistentId Sender node consistent id.
         */
        private void onMessageReceived(Message msg, Object consistentId) {
            IgniteBiTuple<LongAdderMetric, LongAdderMetric> cnts = msgCntrsByType.get(msg.directType());

            cnts.get2().increment();

            rcvdMsgsMetricsByConsistentId.computeIfAbsent(consistentId, rcvdMsgsCntByConsistentIdMetricFactory).increment();
        }
    }
}
