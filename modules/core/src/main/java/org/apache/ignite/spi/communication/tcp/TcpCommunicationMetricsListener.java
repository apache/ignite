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
import java.util.concurrent.atomic.LongAdder;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.nio.GridNioMetricsListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Statistics for {@link org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi}.
 */
public class TcpCommunicationMetricsListener implements GridNioMetricsListener{
    /** Received messages count. */
    private final LongAdder rcvdMsgsCnt = new LongAdder();

    /** Sent messages count.*/
    private final LongAdder sentMsgsCnt = new LongAdder();

    /** Received bytes count. */
    private final LongAdder rcvdBytesCnt = new LongAdder();

    /** Sent bytes count.*/
    private final LongAdder sentBytesCnt = new LongAdder();

    /** Counter factory. */
    private static final Callable<LongAdder> LONG_ADDER_FACTORY = new Callable<LongAdder>() {
        @Override public LongAdder call() {
            return new LongAdder();
        }
    };

    /** Received messages count grouped by message type. */
    private final ConcurrentMap<String, LongAdder> rcvdMsgsCntByType = new ConcurrentHashMap<>();

    /** Received messages count grouped by sender. */
    private final ConcurrentMap<UUID, LongAdder> rcvdMsgsCntByNode = new ConcurrentHashMap<>();

    /** Sent messages count grouped by message type. */
    private final ConcurrentMap<String, LongAdder> sentMsgsCntByType = new ConcurrentHashMap<>();

    /** Sent messages count grouped by receiver. */
    private final ConcurrentMap<UUID, LongAdder> sentMsgsCntByNode = new ConcurrentHashMap<>();

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

        LongAdder cntByType = F.addIfAbsent(sentMsgsCntByType, msg.getClass().getSimpleName(), LONG_ADDER_FACTORY);
        LongAdder cntByNode = F.addIfAbsent(sentMsgsCntByNode, nodeId, LONG_ADDER_FACTORY);

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

        LongAdder cntByType = F.addIfAbsent(rcvdMsgsCntByType, msg.getClass().getSimpleName(), LONG_ADDER_FACTORY);
        LongAdder cntByNode = F.addIfAbsent(rcvdMsgsCntByNode, nodeId, LONG_ADDER_FACTORY);

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
    private <T> Map<T, Long> convertStatistics(Map<T, LongAdder> srcStat) {
        Map<T, Long> destStat = U.newHashMap(srcStat.size());

        for (Map.Entry<T, LongAdder> entry : srcStat.entrySet())
            destStat.put(entry.getKey(), entry.getValue().longValue());

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
    public Map<UUID, Long> receivedMessagesByNode() {
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
    public Map<UUID, Long> sentMessagesByNode() {
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
