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
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.metric.IntMetric;
import org.apache.ignite.spi.metric.LongMetric;

import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Metrics source for communication SPI metrics.
 */
public class CommunicationMetricSource extends AbstractMetricSource<CommunicationMetricSource.Holder> {
    /** Io communication metrics registry name. */
    public static final String COMM_METRICS = metricName("io", "communication");

    /** Outbound message queue size metric name. */
    public static final String OUTBOUND_MSG_QUEUE_CNT = "OutboundMessagesQueueSize";

    /** Sent messages count metric name. */
    public static final String SENT_MSG_CNT = "SentMessagesCount";

    /** Sent bytes count metric name. */
    public static final String SENT_BYTES_CNT = "SentBytesCount";

    /** Received messages count metric name. */
    public static final String RCVD_MSGS_CNT = "ReceivedMessagesCount";

    /** Received bytes count metric name. */
    public static final String RCVD_BYTES_CNT = "ReceivedBytesCount";

    /**
     * Creates communication metric source.
     *
     * @param ctx Kernal context.
     */
    public CommunicationMetricSource(GridKernalContext ctx) {
        super(COMM_METRICS, ctx);
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder holder) {
        CommunicationSpi<?> commSpi = ctx().config().getCommunicationSpi();

        holder.outboundMsgCnt = bldr.register(OUTBOUND_MSG_QUEUE_CNT, commSpi::getOutboundMessagesQueueSize,
                "Outbound messages queue size.");

        holder.sentMsgsCnt = bldr.register(SENT_MSG_CNT, commSpi::getSentMessagesCount, "Sent messages count.");

        holder.sentBytesCnt = bldr.register(SENT_BYTES_CNT, commSpi::getSentBytesCount, "Sent bytes count.");

        holder.rcvdMsgsCnt = bldr.register(RCVD_MSGS_CNT, commSpi::getReceivedMessagesCount,
                "Received messages count.");

        holder.rcvdBytesCnt = bldr.register(RCVD_BYTES_CNT, commSpi::getReceivedBytesCount, "Received bytes count.");
    }

    /**
     * Returns sent messages count.
     *
     * @return Sent messages count.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public int sentMessagesCount() {
        Holder hldr = holder();

        return hldr != null ? hldr.sentMsgsCnt.value() : -1;
    }

    /**
     * Returns received messages count.
     *
     * @return Received messages count.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public int receivedMessagesCount() {
        Holder hldr = holder();

        return hldr != null ? hldr.rcvdMsgsCnt.value() : -1;
    }

    /**
     * Returns sent bytes count.
     *
     * @return Sent bytes count.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long sentBytesCount() {
        Holder hldr = holder();

        return hldr != null ? hldr.sentBytesCnt.value() : -1;
    }

    /**
     * Returns received bytes count.
     *
     * @return Received bytes count.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public long receivedBytesCount() {
        Holder hldr = holder();

        return hldr != null ? hldr.rcvdBytesCnt.value() : -1;
    }

    /**
     * Returns outbound messages queue size.
     *
     * @return Outbound messages queue size.
     * @deprecated Should be removed in Apache Ignite 3.0 because client MBeans will be removed.
     */
    @Deprecated
    public int outboundMessagesCount() {
        Holder hldr = holder();

        return hldr != null ? hldr.outboundMsgCnt.value() : -1;
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
    }

    /** */
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** Sent messages count metric. */
        private IntMetric sentMsgsCnt;

        /** Sent bytes count metric. */
        private LongMetric sentBytesCnt;

        /** Received messages count metric. */
        private IntMetric rcvdMsgsCnt;

        /** Received bytes count metric. */
        private LongMetric rcvdBytesCnt;

        /** Outbound message queue size metric. */
        private IntMetric outboundMsgCnt;
    }
}
