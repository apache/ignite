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
import org.apache.ignite.internal.managers.communication.IgniteMessageFactoryImpl;
import org.apache.ignite.internal.processors.metric.MetricRegistryBuilder;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.processors.metric.impl.LongAdderMetric;
import org.apache.ignite.internal.util.collection.IntHashMap;
import org.apache.ignite.internal.util.collection.IntMap;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.plugin.extensions.communication.IgniteMessageFactory;

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
    public static final String SENT_MSG_CNT = "SentMessages";

    /** Sent bytes count metric name. */
    public static final String SENT_BYTES_CNT = "SentBytes";

    /** Received messages count metric name. */
    public static final String RCVD_MSGS_CNT = "ReceivedMessages";

    /** Received bytes count metric name. */
    public static final String RCVD_BYTES_CNT = "ReceivedBytes";

    private final IgniteMessageFactory msgFactory;

    /**
     * Creates communication metric source.
     */
    public CommunicationMetricSource(GridKernalContext ctx) {
        super(COMM_METRICS, ctx);

        msgFactory = (IgniteMessageFactory)ctx.io().messageFactory();
    }

    /** {@inheritDoc} */
    @Override protected void init(MetricRegistryBuilder bldr, Holder hldr) {
        hldr.msgCntrsByType = createMessageCounters(msgFactory);

        hldr.outboundMsgCnt = bldr.intMetric(OUTBOUND_MSG_QUEUE_CNT, "Outbound messages queue size.");

        hldr.sentMsgsCnt = bldr.intMetric(SENT_MSG_CNT, "Total number of messages sent by current nodeю");

        hldr.sentBytesCnt = bldr.longAdderMetric(SENT_BYTES_CNT, "Total number of bytes sent by current node.");

        hldr.rcvdMsgsCnt = bldr.intMetric(RCVD_MSGS_CNT, "Total number of messages received by current nodeю");

        hldr.rcvdBytesCnt = bldr.longAdderMetric(RCVD_BYTES_CNT,"Total number of bytes received by current node.");
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

    public void incrementSentMessages(short directType) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.sentMsgsCnt.increment();

            IgniteBiTuple<LongAdderMetric, LongAdderMetric> cnts = hldr.msgCntrsByType.get(directType);

            cnts.get1().increment();
        }
    }

    public void incrementReceivedMessages(short directType) {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.rcvdMsgsCnt.increment();

            IgniteBiTuple<LongAdderMetric, LongAdderMetric> cnts = hldr.msgCntrsByType.get(directType);

            cnts.get2().increment();
        }
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

    public void addSentBytes(long val) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.sentBytesCnt.add(val);
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

    public void addReceivedBytes(long val) {
        Holder hldr = holder();

        if (hldr != null)
            hldr.rcvdBytesCnt.add(val);
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

    public void reset() {
        Holder hldr = holder();

        if (hldr != null) {
            hldr.rcvdMsgsCnt.reset();

            hldr.rcvdBytesCnt.reset();

            hldr.sentMsgsCnt.reset();

            hldr.sentBytesCnt.reset();
        }
    }

    /** {@inheritDoc} */
    @Override protected Holder createHolder() {
        return new Holder();
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

        //TODO: Replace by metric source
/*
        for (short type : directTypes) {
            LongAdderMetric sentCnt =
                    mreg.longAdderMetric(sentMessagesByTypeMetricName(type), SENT_MESSAGES_BY_TYPE_METRIC_DESC);

            LongAdderMetric rcvCnt =
                    mreg.longAdderMetric(receivedMessagesByTypeMetricName(type), RECEIVED_MESSAGES_BY_TYPE_METRIC_DESC);

            msgCntrsByType.put(type, new IgniteBiTuple<>(sentCnt, rcvCnt));
        }
*/

        return msgCntrsByType;
    }

    /** */
    @SuppressWarnings("ClassNameSameAsAncestorName")
    protected static class Holder implements AbstractMetricSource.Holder<Holder> {
        /** Counters of sent and received messages by direct type. */
        private IntMap<IgniteBiTuple<LongAdderMetric, LongAdderMetric>> msgCntrsByType;

        /** Sent messages count metric. */
        private IntMetricImpl sentMsgsCnt;

        /** Sent bytes count metric. */
        private LongAdderMetric sentBytesCnt;

        /** Received messages count metric. */
        private IntMetricImpl rcvdMsgsCnt;

        /** Received bytes count metric. */
        private LongAdderMetric rcvdBytesCnt;

        /** Outbound message queue size metric. */
        private IntMetricImpl outboundMsgCnt;
    }
}
