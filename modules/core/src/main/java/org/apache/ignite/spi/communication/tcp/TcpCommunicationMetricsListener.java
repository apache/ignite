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

package org.apache.ignite.spi.communication.tcp;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.plugin.extensions.communication.Message;

import static org.apache.ignite.internal.util.nio.GridNioServer.RECEIVED_BYTES_METRIC_DESC;
import static org.apache.ignite.internal.util.nio.GridNioServer.RECEIVED_BYTES_METRIC_NAME;
import static org.apache.ignite.internal.util.nio.GridNioServer.SENT_BYTES_METRIC_DESC;
import static org.apache.ignite.internal.util.nio.GridNioServer.SENT_BYTES_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.COMMUNICATION_METRICS_GROUP_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_NODE_ID_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_NODE_ID_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_BY_TYPE_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.RECEIVED_MESSAGES_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_NODE_ID_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_NODE_ID_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_BY_TYPE_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_METRIC_DESC;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.SENT_MESSAGES_METRIC_NAME;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.receivedMessagesByTypeMetricName;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.sentMessagesByTypeMetricName;

/**
 * Statistics for {@link org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi}.
 */
class TcpCommunicationMetricsListener {
    /** Metrics manager. */
    private final GridMetricManager mmgr;

    /** Metrics registry. */
    private final MetricRegistry mreg;

    /** */
    private final Function<Short, LongAdderMetric> sentMsgsCntByTypeMetricFactory;

    /** */
    private final Function<Short, LongAdderMetric> rcvdMsgsCntByTypeMetricFactory;

    /** */
    private final Function<UUID, LongAdderMetric> sentMsgsCntByNodeIdMetricFactory;

    /** */
    private final Function<UUID, LongAdderMetric> rcvdMsgsCntByNodeIdMetricFactory;

    /** Sent bytes count metric.*/
    private final LongAdderMetric sentBytesMetric;

    /** Received bytes count metric. */
    private final LongAdderMetric rcvdBytesMetric;

    /** Sent messages count metric. */
    private final LongAdderMetric sentMsgsMetric;

    /** Received messages count metric. */
    private final LongAdderMetric rcvdMsgsMetric;

    /** Sent messages count metrics grouped by message type. */
    ConcurrentHashMap<Short, LongAdderMetric> sentMsgsMetricsByType = new ConcurrentHashMap<>();

    /** Received messages count metrics grouped by message type. */
    ConcurrentHashMap<Short, LongAdderMetric> rcvdMsgsMetricsByType = new ConcurrentHashMap<>();

    /** Sent messages count metrics grouped by message node id. */
    ConcurrentHashMap<UUID, LongAdderMetric> sentMsgsMetricsByNodeId = new ConcurrentHashMap<>();

    /** Received messages metrics count grouped by message node id. */
    ConcurrentHashMap<UUID, LongAdderMetric> rcvdMsgsMetricsByNodeId = new ConcurrentHashMap<>();

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

        sentMsgsCntByNodeIdMetricFactory = nodeId -> mmgr.registry(COMMUNICATION_METRICS_GROUP_NAME + "." + nodeId)
            .longAdderMetric(
                SENT_MESSAGES_BY_NODE_ID_METRIC_NAME,
                SENT_MESSAGES_BY_NODE_ID_METRIC_DESC
            );

        rcvdMsgsCntByNodeIdMetricFactory = nodeId -> mmgr.registry(COMMUNICATION_METRICS_GROUP_NAME + "." + nodeId)
            .longAdderMetric(
                RECEIVED_MESSAGES_BY_NODE_ID_METRIC_NAME,
                RECEIVED_MESSAGES_BY_NODE_ID_METRIC_DESC
            );

        sentBytesMetric = mreg.longAdderMetric(SENT_BYTES_METRIC_NAME, SENT_BYTES_METRIC_DESC);
        rcvdBytesMetric = mreg.longAdderMetric(RECEIVED_BYTES_METRIC_NAME, RECEIVED_BYTES_METRIC_DESC);

        sentMsgsMetric = mreg.longAdderMetric(SENT_MESSAGES_METRIC_NAME, SENT_MESSAGES_METRIC_DESC);
        rcvdMsgsMetric = mreg.longAdderMetric(RECEIVED_MESSAGES_METRIC_NAME, RECEIVED_MESSAGES_METRIC_DESC);
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

            sentMsgsMetricsByType.computeIfAbsent(msg.directType(), sentMsgsCntByTypeMetricFactory).increment();

            sentMsgsMetricsByNodeId.computeIfAbsent(nodeId, sentMsgsCntByNodeIdMetricFactory).increment();
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

            rcvdMsgsMetricsByType.computeIfAbsent(msg.directType(), rcvdMsgsCntByTypeMetricFactory).increment();

            rcvdMsgsMetricsByNodeId.computeIfAbsent(nodeId, rcvdMsgsCntByNodeIdMetricFactory).increment();
        }
    }

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    public int sentMessagesCount() {
        int res0 = (int)sentMsgsMetric.longValue();

        return res0 < 0 ? Integer.MAX_VALUE : res0;
    }

    /**
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    public long sentBytesCount() {
        return sentBytesMetric.longValue();
    }

    /**
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    public int receivedMessagesCount() {
        int res0 = (int)rcvdMsgsMetric.longValue();

        return res0 < 0 ? Integer.MAX_VALUE : res0;
    }

    /**
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    public long receivedBytesCount() {
        return rcvdBytesMetric.longValue();
    }

    /**
     * Convert message types.
     *
     * @param input Input map.
     * @return Result map.
     */
    private Map<String, Long> convertMessageTypes(Map<Short, LongAdderMetric> input) {
        Map<String, Long> res = new HashMap<>(input.size());

        Map<Short, String> msgTypMap0 = msgTypMap;

        if (msgTypMap0 != null) {
            for (Map.Entry<Short, LongAdderMetric> inputEntry : input.entrySet()) {
                String typeName = msgTypMap0.get(inputEntry.getKey());

                if (typeName != null)
                    res.put(typeName, inputEntry.getValue().longValue());
            }
        }

        return res;
    }

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> receivedMessagesByType() {
        return convertMessageTypes(rcvdMsgsMetricsByType);
    }

    /**
     * Gets received messages counts (grouped by node).
     *
     * @return Map containing sender nodes and respective counts.
     */
    public Map<UUID, Long> receivedMessagesByNode() {
        Map<UUID, Long> res = new HashMap<>();

        for (Map.Entry<UUID, LongAdderMetric> entry : rcvdMsgsMetricsByNodeId.entrySet())
            res.put(entry.getKey(), entry.getValue().longValue());

        return res;
    }

    /**
     * Gets sent messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> sentMessagesByType() {
        return convertMessageTypes(sentMsgsMetricsByType);
    }

    /**
     * Gets sent messages counts (grouped by node).
     *
     * @return Map containing receiver nodes and respective counts.
     */
    public Map<UUID, Long> sentMessagesByNode() {
        Map<UUID, Long> res = new HashMap<>();

        for (Map.Entry<UUID, LongAdderMetric> entry : sentMsgsMetricsByNodeId.entrySet())
            res.put(entry.getKey(), entry.getValue().longValue());

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

        for (LongAdderMetric metric : sentMsgsMetricsByType.values())
            metric.reset();

        for (LongAdderMetric metric : rcvdMsgsMetricsByType.values())
            metric.reset();

        for (LongAdderMetric metric : sentMsgsMetricsByNodeId.values())
            metric.reset();

        for (LongAdderMetric metric : rcvdMsgsMetricsByNodeId.values())
            metric.reset();
    }

    /** */
    public void onNodeLeft(UUID nodeId) {
        sentMsgsMetricsByNodeId.remove(nodeId);
        rcvdMsgsMetricsByNodeId.remove(nodeId);

        mmgr.remove(COMMUNICATION_METRICS_GROUP_NAME + "." + nodeId);
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
}
