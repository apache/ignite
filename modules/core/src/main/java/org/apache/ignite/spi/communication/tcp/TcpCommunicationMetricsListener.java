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
import java.util.function.Function;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.metric.GridMetricManager;
import org.apache.ignite.internal.processors.metric.sources.CommunicationMetricSource;
import org.apache.ignite.internal.processors.metric.sources.NodeCommunicationMetricSource;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiContext;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.metric.sources.NodeCommunicationMetricSource.NODE_COMM_METRICS;

/**
 * Statistics for {@link org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi}.
 */
public class TcpCommunicationMetricsListener {
    /** SPI context. */
    private final IgniteSpiContext spiCtx;

    /** Current ignite instance. */
    private final Ignite ignite;

    /** Communication metric source. */
    private final CommunicationMetricSource metricSrc;

    //TODO: correct javadic
    /** Function to be used in {@link Map#computeIfAbsent(Object, Function)} of {@code #sentMsgsMetricsByConsistentId}. */
    private final Function<Object, NodeCommunicationMetricSource> nodeMetricSrcProvider;

    /** Message type map. */
    private volatile Map<Short, String> msgTypeMap;

    /** */
    public TcpCommunicationMetricsListener(Ignite ignite, CommunicationMetricSource metricSrc, IgniteSpiContext spiCtx) {
        this.ignite = ignite;
        this.spiCtx = spiCtx;
        this.metricSrc = metricSrc;

        GridMetricManager metricMgr = ((IgniteKernal)ignite).context().metric();

        nodeMetricSrcProvider = consistentId ->
                metricMgr.source(metricName(NODE_COMM_METRICS, consistentId.toString()));
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
            msg = ((GridIoMessage) msg).message();

            metricSrc.incrementSentMessages(msg.directType());

            //TODO: use node metric source
            //nodeMetricSrcProvider.apply(consistentId).incrementSentMessages();
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
            msg = ((GridIoMessage) msg).message();

            metricSrc.incrementReceivedMessages(msg.directType());

            //TODO: use node metric source
            //nodeMetricSrcProvider.apply(consistentId).incrementReceivedMessages();
        }
    }

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    public int sentMessagesCount() {
        int res0 = metricSrc.sentMessagesCount();

        return res0 < 0 ? Integer.MAX_VALUE : res0;
    }

    /**
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    public long sentBytesCount() {
        return metricSrc.sentMessagesCount();
    }

    /**
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    public int receivedMessagesCount() {
        int res0 = metricSrc.receivedMessagesCount();

        return res0 < 0 ? Integer.MAX_VALUE : res0;
    }

    /**
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    public long receivedBytesCount() {
        return metricSrc.receivedBytesCount();
    }

    /**
     * Resets metrics for this instance.
     */
    public void resetMetrics() {
        metricSrc.reset();

        //TODO: reset per node metrics
/*
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
*/
    }

    /**
     * @param consistentId Consistent id of the node.
     */
    public void onNodeLeft(Object consistentId) {
        GridMetricManager metricMgr = ((IgniteKernal) ignite).context().metric();

        metricMgr.unregisterSource(metricName(NODE_COMM_METRICS, consistentId.toString()));
    }
}
