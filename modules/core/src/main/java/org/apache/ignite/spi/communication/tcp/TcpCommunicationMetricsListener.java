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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.GridConcurrentFactory;
import org.apache.ignite.internal.util.nio.GridNioMetricsListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.jsr166.LongAdder8;

/**
 * Statistics for {@link org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi}.
 */
public class TcpCommunicationMetricsListener implements GridNioMetricsListener{
    /** Received messages count. */
    private final LongAdder8 rcvdMsgsCnt = new LongAdder8();

    /** Sent messages count.*/
    private final LongAdder8 sentMsgsCnt = new LongAdder8();

    /** Received bytes count. */
    private final LongAdder8 rcvdBytesCnt = new LongAdder8();

    /** Sent bytes count.*/
    private final LongAdder8 sentBytesCnt = new LongAdder8();

    /** Counter factory. */
    private static final Callable<LongAdder8> LONG_ADDER_FACTORY = new Callable<LongAdder8>() {
        @Override public LongAdder8 call() {
            return new LongAdder8();
        }
    };

    /** Received messages count grouped by message type. */
    private final ConcurrentMap<String, LongAdder8> rcvdMsgsCntByType = new ConcurrentHashMap<>();

    /** Received messages count grouped by sender. */
    private final ConcurrentMap<String, LongAdder8> rcvdMsgsCntByNode = new ConcurrentHashMap<>();

    /** Sent messages count grouped by message type. */
    private final ConcurrentMap<String, LongAdder8> sentMsgsCntByType = new ConcurrentHashMap<>();

    /** Sent messages count grouped by receiver. */
    private final ConcurrentMap<String, LongAdder8> sentMsgsCntByNode = new ConcurrentHashMap<>();

    /** {@inheritDoc} */
    @Override public void onBytesSent(int bytesCnt) {
        sentBytesCnt.add(bytesCnt);
    }

    /** {@inheritDoc} */
    @Override public void onBytesReceived(int bytesCnt) {
        rcvdBytesCnt.add(bytesCnt);
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

        sentMsgsCnt.increment();

        if (msg instanceof GridIoMessage)
            msg = ((GridIoMessage)msg).message();

        LongAdder8 cntByType = F.addIfAbsent(sentMsgsCntByType, msg.getClass().getSimpleName(), LONG_ADDER_FACTORY);
        LongAdder8 cntByNode = F.addIfAbsent(sentMsgsCntByNode, nodeId.toString(), LONG_ADDER_FACTORY);

        cntByType.increment();
        cntByNode.increment();
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

        rcvdMsgsCnt.increment();

        if (msg instanceof GridIoMessage)
            msg = ((GridIoMessage)msg).message();

        LongAdder8 cntByType = F.addIfAbsent(rcvdMsgsCntByType, msg.getClass().getSimpleName(), LONG_ADDER_FACTORY);
        LongAdder8 cntByNode = F.addIfAbsent(rcvdMsgsCntByNode, nodeId.toString(), LONG_ADDER_FACTORY);

        cntByType.increment();
        cntByNode.increment();
    }

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    public int sentMessagesCount() {
        return sentMsgsCnt.intValue();
    }

    /**
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    public long sentBytesCount() {
        return sentBytesCnt.longValue();
    }

    /**
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    public int receivedMessagesCount() {
        return rcvdMsgsCnt.intValue();
    }

    /**
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    public long receivedBytesCount() {
        return rcvdBytesCnt.longValue();
    }

    /**
     * Converts statistics from internal representation to JMX-readable format.
     *
     * @param srcStat Internal statistics representation.
     * @return Result map.
     */
    private Map<String, Long> convertStatistics(Map<String, LongAdder8> srcStat) {
        Map<String, Long> destStat = U.newHashMap(srcStat.size());

        for (Map.Entry<String, LongAdder8> entry : srcStat.entrySet()) {
            destStat.put(entry.getKey(), entry.getValue().longValue());
        }

        return destStat;
    }

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> receivedMessagesByType() {
        return convertStatistics(rcvdMsgsCntByType);
    }

    /**
     * Gets received messages counts (grouped by node).
     *
     * @return Map containing sender nodes and respective counts.
     */
    public Map<String, Long> receivedMessagesByNode() {
        return convertStatistics(rcvdMsgsCntByNode);
    }

    /**
     * Gets sent messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> sentMessagesByType() {
        return convertStatistics(sentMsgsCntByType);
    }

    /**
     * Gets sent messages counts (grouped by node).
     *
     * @return Map containing receiver nodes and respective counts.
     */
    public Map<String, Long> sentMessagesByNode() {
        return convertStatistics(sentMsgsCntByNode);
    }

    /**
     * Resets metrics for this instance.
     */
    public void resetMetrics() {
        // Can't use 'reset' method because it is not thread-safe
        // according to javadoc.
        sentMsgsCnt.add(-sentMsgsCnt.sum());
        rcvdMsgsCnt.add(-rcvdMsgsCnt.sum());
        sentBytesCnt.add(-sentBytesCnt.sum());
        rcvdBytesCnt.add(-rcvdBytesCnt.sum());

        sentMsgsCntByType.clear();
        rcvdMsgsCntByType.clear();
        sentMsgsCntByNode.clear();
        rcvdMsgsCntByNode.clear();
    }
}
