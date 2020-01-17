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
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.metric.LongMetric;
import org.apache.ignite.spi.metric.Metric;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.SEPARATOR;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.util.nio.GridNioServer.RECEIVED_BYTES_METRIC_DESC;
import static org.apache.ignite.internal.util.nio.GridNioServer.RECEIVED_BYTES_METRIC_NAME;
import static org.apache.ignite.internal.util.nio.GridNioServer.SENT_BYTES_METRIC_DESC;
import static org.apache.ignite.internal.util.nio.GridNioServer.SENT_BYTES_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_NODE_ID_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_NODE_ID_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_TYPE_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_TYPE_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_NODE_ID_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_NODE_ID_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_TYPE_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_TYPE_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_METRIC_NAME;

/**
 * Statistics for {@link org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi}.
 */
class TcpCommunicationMetricsListener {
    /** Metrics manager. */
    private final GridMetricManager mmgr;

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

    /** Function to be used in {@link Map#computeIfAbsent(Object, Function)} of {@code sentMsgsMetricsByType}. */
    private final Function<Short, LongAdderMetric> sentMsgsCntByTypeMetricFactory;

    /** Function to be used in {@link Map#computeIfAbsent(Object, Function)} of {@code rcvdMsgsMetricsByType}. */
    private final Function<Short, LongAdderMetric> rcvdMsgsCntByTypeMetricFactory;

    /** Function to be used in {@link Map#computeIfAbsent(Object, Function)} of {@code sentMsgsMetricsByNodeId}. */
    private final Function<UUID, LongAdderMetric> sentMsgsCntByNodeIdMetricFactory;

    /** Function to be used in {@link Map#computeIfAbsent(Object, Function)} of {@code rcvdMsgsMetricsByNodeId}. */
    private final Function<UUID, LongAdderMetric> rcvdMsgsCntByNodeIdMetricFactory;

    /** Sent bytes count metric.*/
    private final LongAdderMetric sentBytesMetric;

    /** Received bytes count metric. */
    private final LongAdderMetric rcvdBytesMetric;

    /** Sent messages count metric. */
    private final LongAdderMetric sentMsgsMetric;

    /** Received messages count metric. */
    private final LongAdderMetric rcvdMsgsMetric;

    /** Method to synchronize access to message type map. */
    private final Object msgTypMapMux = new Object();

    /** Message type map. */
    private volatile Map<Short, String> msgTypMap;

    /** */
    public TcpCommunicationMetricsListener(GridMetricManager mmgr) {
        this.mmgr = mmgr;

        mreg = mmgr.registry(COMMUNICATION_METRICS_GROUP_NAME);

        sentMsgsCntByTypeMetricFactory = directType -> mreg.longAdderMetric(
            sentMessagesByTypeMetricName(directType),
            SENT_MESSAGES_BY_TYPE_METRIC_DESC
        );
        rcvdMsgsCntByTypeMetricFactory = directType -> mreg.longAdderMetric(
            receivedMessagesByTypeMetricName(directType),
            RECEIVED_MESSAGES_BY_TYPE_METRIC_DESC
        );

        sentMsgsCntByNodeIdMetricFactory = nodeId ->
            mmgr.registry(metricName(COMMUNICATION_METRICS_GROUP_NAME, nodeId.toString()))
                .findMetric(SENT_MESSAGES_BY_NODE_ID_METRIC_NAME);

        rcvdMsgsCntByNodeIdMetricFactory = nodeId ->
            mmgr.registry(metricName(COMMUNICATION_METRICS_GROUP_NAME, nodeId.toString()))
                .findMetric(RECEIVED_MESSAGES_BY_NODE_ID_METRIC_NAME);

        sentBytesMetric = mreg.longAdderMetric(SENT_BYTES_METRIC_NAME, SENT_BYTES_METRIC_DESC);
        rcvdBytesMetric = mreg.longAdderMetric(RECEIVED_BYTES_METRIC_NAME, RECEIVED_BYTES_METRIC_DESC);

        sentMsgsMetric = mreg.longAdderMetric(SENT_MESSAGES_METRIC_NAME, SENT_MESSAGES_METRIC_DESC);
        rcvdMsgsMetric = mreg.longAdderMetric(RECEIVED_MESSAGES_METRIC_NAME, RECEIVED_MESSAGES_METRIC_DESC);

        mmgr.addMetricRegistryCreationListener(mreg -> {
            // Metrics for the specific nodes.
            if (!mreg.name().startsWith(COMMUNICATION_METRICS_GROUP_NAME + SEPARATOR))
                return;

            mreg.longAdderMetric(SENT_MESSAGES_BY_NODE_ID_METRIC_NAME, SENT_MESSAGES_BY_NODE_ID_METRIC_DESC);

            mreg.longAdderMetric(RECEIVED_MESSAGES_BY_NODE_ID_METRIC_NAME, RECEIVED_MESSAGES_BY_NODE_ID_METRIC_DESC);
        });
    }

    /** Metrics registry. */
    public MetricRegistry metricRegistry() {
        return mreg;
    }

    /**
     * Collects statistics for message sent by SPI.
     *
     * @param msg Sent message.
     * @param nodeId Receiver node id.
     */
    public void onMessageSent(Message msg, UUID nodeId) {
        assert msg != null;
        assert nodeId != null;

        if (msg instanceof GridIoMessage) {
            msg = ((GridIoMessage) msg).message();

            updateMessageTypeMap(msg);

            sentMsgsMetric.increment();

            threadMetrics.get().onMessageSent(msg, nodeId);
        }
    }

    /**
     * Collects statistics for message received by SPI.
     *
     * @param msg Received message.
     * @param nodeId Sender node id.
     */
    public void onMessageReceived(Message msg, UUID nodeId) {
        assert msg != null;
        assert nodeId != null;

        if (msg instanceof GridIoMessage) {
            msg = ((GridIoMessage) msg).message();

            updateMessageTypeMap(msg);

            rcvdMsgsMetric.increment();

            threadMetrics.get().onMessageReceived(msg, nodeId);
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
        return collectMessagesCountByNodeId(RECEIVED_MESSAGES_BY_NODE_ID_METRIC_NAME);
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
        return collectMessagesCountByNodeId(SENT_MESSAGES_BY_NODE_ID_METRIC_NAME);
    }

    /** */
    protected Map<String, Long> collectMessagesCountByType(String prefix) {
        Map<String, Long> res = new HashMap<>();

        prefix = metricName(COMMUNICATION_METRICS_GROUP_NAME, prefix);

        for (Metric metric : mreg) {
            if (metric.name().startsWith(prefix)) {
                short directType = Short.parseShort(metric.name().substring(prefix.length()));

                Map<Short, String> msgTypMap0 = msgTypMap;

                if (msgTypMap0 != null) {
                    String typeName = msgTypMap0.get(directType);

                    if (typeName != null)
                        res.put(typeName, ((LongMetric)metric).value());
                }
            }
        }

        return res;
    }

    /** */
    protected Map<UUID, Long> collectMessagesCountByNodeId(String metricName) {
        Map<UUID, Long> res = new HashMap<>();

        String mregPrefix = COMMUNICATION_METRICS_GROUP_NAME + SEPARATOR;

        for (MetricRegistry mreg : mmgr) {
            if (mreg.name().startsWith(mregPrefix)) {
                String nodeIdStr = mreg.name().substring(mregPrefix.length());

                UUID nodeId = UUID.fromString(nodeIdStr);

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

        for (MetricRegistry mreg : mmgr) {
            if (mreg.name().startsWith(COMMUNICATION_METRICS_GROUP_NAME + SEPARATOR)) {
                mreg.findMetric(SENT_MESSAGES_BY_NODE_ID_METRIC_NAME).reset();

                mreg.findMetric(RECEIVED_MESSAGES_BY_NODE_ID_METRIC_NAME).reset();
            }
        }
    }

    /**
     * @param nodeId Left node id.
     */
    public void onNodeLeft(UUID nodeId) {
        for (ThreadMetrics threadMetrics : allMetrics) {
            threadMetrics.rcvdMsgsMetricsByNodeId = new HashMap<>();
            threadMetrics.sentMsgsMetricsByNodeId = new HashMap<>();
        }

        mmgr.remove(metricName(COMMUNICATION_METRICS_GROUP_NAME, nodeId.toString()));
    }

    /**
     * Update message type map.
     *
     * @param msg Message.
     */
    private void updateMessageTypeMap(Message msg) {
        short typeId = msg.directType();

        Map<Short, String> msgTypMap0 = msgTypMap;

        if (msgTypMap0 == null || !msgTypMap0.containsKey(typeId)) {
            synchronized (msgTypMapMux) {
                if (msgTypMap == null) {
                    msgTypMap0 = new HashMap<>();

                    msgTypMap0.put(typeId, msg.getClass().getName());

                    msgTypMap = msgTypMap0;
                }
                else {
                    if (!msgTypMap.containsKey(typeId)) {
                        msgTypMap0 = new HashMap<>(msgTypMap);

                        msgTypMap0.put(typeId, msg.getClass().getName());

                        msgTypMap = msgTypMap0;
                    }
                }
            }
        }
    }

    /** Generate metric name by message direct type id. */
    public static String sentMessagesByTypeMetricName(Short directType) {
        return metricName(SENT_MESSAGES_BY_TYPE_METRIC_NAME, directType.toString());
    }

    /** Generate metric name by message direct type id. */
    public static String receivedMessagesByTypeMetricName(Short directType) {
        return metricName(RECEIVED_MESSAGES_BY_TYPE_METRIC_NAME, directType.toString());
    }

    /**
     * Thread-local metrics.
     */
    private class ThreadMetrics {
        /** Sent messages count metrics grouped by message type. */
        private final Map<Short, LongAdderMetric> sentMsgsMetricsByType = new HashMap<>();

        /** Received messages count metrics grouped by message type. */
        private final Map<Short, LongAdderMetric> rcvdMsgsMetricsByType = new HashMap<>();

        /**
         * Sent messages count metrics grouped by message node id.
         */
        public volatile Map<UUID, LongAdderMetric> sentMsgsMetricsByNodeId = new HashMap<>();

        /**
         * Received messages metrics count grouped by message node id.
         */
        public volatile Map<UUID, LongAdderMetric> rcvdMsgsMetricsByNodeId = new HashMap<>();


        /**
         * Collects statistics for message sent by SPI.
         *
         * @param msg Sent message.
         * @param nodeId Receiver node id.
         */
        private void onMessageSent(Message msg, UUID nodeId) {
            sentMsgsMetricsByType.computeIfAbsent(msg.directType(), sentMsgsCntByTypeMetricFactory).increment();

            sentMsgsMetricsByNodeId.computeIfAbsent(nodeId, sentMsgsCntByNodeIdMetricFactory).increment();
        }

        /**
         * Collects statistics for message received by SPI.
         *
         * @param msg Received message.
         * @param nodeId Sender node id.
         */
        private void onMessageReceived(Message msg, UUID nodeId) {
            rcvdMsgsMetricsByType.computeIfAbsent(msg.directType(), rcvdMsgsCntByTypeMetricFactory).increment();

            rcvdMsgsMetricsByNodeId.computeIfAbsent(nodeId, rcvdMsgsCntByNodeIdMetricFactory).increment();
        }
    }
}
