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

package org.apache.ignite.internal.processors.metric.sources;

import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.metric.sources.CommunicationMetricSource.COMM_METRICS;

/**
 * Per node communication metrics.
 */
public class NodeCommunicationMetricSource extends AbstractMetricSource<NodeCommunicationMetricSource.Holder> {
    /** Per node communication metrics registry name. */
    public static final String NODE_COMM_METRICS = metricName(COMM_METRICS, "node");

    /** Sent messages by node consistent id metric name. */
    public static final String SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME = "SentMessagesToNode";

    /** Sent messages by node consistent id metric description. */
    public static final String SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC =
            "Total number of messages sent by current node to the given node.";

    /** Received messages by node consistent id metric name. */
    public static final String RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME = "ReceivedMessagesFromNode";

    /** Received messages by node consistent id metric description. */
    public static final String RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC =
            "Total number of messages received by current node from the given node.";

    /**
     * @param consistentId Consistent node ID.
     * @param ctx Kernal context.
     */
    public NodeCommunicationMetricSource(String consistentId, GridKernalContext ctx) {
        super(metricName(NODE_COMM_METRICS, consistentId), ctx);
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder holder) {
        bldr.longAdderMetric(SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME,
                SENT_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC);

        bldr.longAdderMetric(RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_NAME,
                RECEIVED_MESSAGES_BY_NODE_CONSISTENT_ID_METRIC_DESC);

    }

    public void incrementSentMessages() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.sentMsgsCnt.increment();
    }

    public void incrementReceivedMessages() {
        Holder hldr = holder();

        if (hldr != null)
            hldr.rcvdMsgsCnt.increment();
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        private LongAdderMetric sentMsgsCnt;

        private LongAdderMetric rcvdMsgsCnt;

        private LongAdderMetric sentBytesCnt;

        private LongAdderMetric rcvdBytesCnt;
    }
}
