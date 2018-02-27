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

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.nio.GridNioMetricsListener;
import org.apache.ignite.internal.util.typedef.F;
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
    private static final Callable<MessageCounter> MSGS_CNT_FACTORY = new Callable<MessageCounter>() {
        @Override public MessageCounter call() {
            return new MessageCounter();
        }
    };

    /**
     * Received/sent message counter.
     */
    private static class MessageCounter {
        /** Received messages count. */
        volatile long rcvdMsgs;

        /** Sent messages count. */
        volatile long sentMsgs;

        /**
         * Increment received messages count.
         */
        public void incrementRcvdCount() {
            rcvdMsgs++;
        }

        /**
         * Increment sent messages count.
         */
        public void incrementSentCount() {
            sentMsgs++;
        }

        /**
         * @return Received messages count.
         */
        public long rcvdMsgs() {
            return rcvdMsgs;
        }

        /**
         * @return Sent messages count.
         */
        public long sentMsgs() {
            return sentMsgs;
        }
    }

    /**
     * Thread local counters map.
     *
     * @param <T> grouping key class.
     */
    private class ThreadLocalCountersMap<T> extends ThreadLocal<Map<T, MessageCounter>> {
        /** Counter maps for all threads. */
        private final Collection<Map<T, MessageCounter>> cntrAggregator;

        /**
         * @param aggregator Counter maps for all threads.
         */
        private ThreadLocalCountersMap(Collection<Map<T, MessageCounter>> aggregator) {
            cntrAggregator = aggregator;
        }

        /** {@inheritDoc} */
        @Override protected Map<T, MessageCounter> initialValue() {
            Map<T, MessageCounter> cntrMap = new ConcurrentHashMap<>();

            cntrAggregator.add(cntrMap);

            return cntrMap;
        }
    }

    /** Message counters by node for all threads. */
    Set<Map<UUID, MessageCounter>> msgsCntrByNodeAll = new GridConcurrentHashSet<>();

    /** Message counter by node for local thread. */
    ThreadLocal<Map<UUID, MessageCounter>> msgsCntrByNode = new ThreadLocalCountersMap<>(msgsCntrByNodeAll);

    /** Message counters by type for all threads. */
    Set<Map<String, MessageCounter>> msgsCntrByTypeAll = new GridConcurrentHashSet<>();

    /** Message counter by type for local thread. */
    ThreadLocal<Map<String, MessageCounter>> msgsCntrByType = new ThreadLocalCountersMap<>(msgsCntrByTypeAll);

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

        Map<String, MessageCounter> cntrMapByType = msgsCntrByType.get();
        Map<UUID, MessageCounter> cntrMapByNode = msgsCntrByNode.get();

        MessageCounter cntrByType = F.addIfAbsent(cntrMapByType, msg.getClass().getSimpleName(), MSGS_CNT_FACTORY);
        MessageCounter cntrByNode = F.addIfAbsent(cntrMapByNode, nodeId, MSGS_CNT_FACTORY);

        cntrByType.incrementSentCount();
        cntrByNode.incrementSentCount();
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

        Map<String, MessageCounter> cntrMapByType = msgsCntrByType.get();
        Map<UUID, MessageCounter> cntrMapByNode = msgsCntrByNode.get();

        MessageCounter cntrByType = F.addIfAbsent(cntrMapByType, msg.getClass().getSimpleName(), MSGS_CNT_FACTORY);
        MessageCounter cntrByNode = F.addIfAbsent(cntrMapByNode, nodeId, MSGS_CNT_FACTORY);

        cntrByType.incrementRcvdCount();
        cntrByNode.incrementRcvdCount();
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
    private <T> Map<T, Long> convertStatistics(Set<Map<T, MessageCounter>> srcStat, ToLongFunction<MessageCounter> func) {
        return srcStat.stream()
            .flatMap(m -> m.entrySet().stream())
            .collect(
                Collectors.groupingBy(
                    Map.Entry::getKey,
                    Collectors.summarizingLong(e -> func.applyAsLong(e.getValue()))
                )
            ).entrySet().stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    e -> e.getValue().getSum()
                )
            );
    }

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> receivedMessagesByType() {
        return convertStatistics(msgsCntrByTypeAll, c -> c.rcvdMsgs());
    }

    /**
     * Gets received messages counts (grouped by node).
     *
     * @return Map containing sender nodes and respective counts.
     */
    public Map<UUID, Long> receivedMessagesByNode() {
        return convertStatistics(msgsCntrByNodeAll, c -> c.rcvdMsgs());
    }

    /**
     * Gets sent messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> sentMessagesByType() {
        return convertStatistics(msgsCntrByTypeAll, c -> c.sentMsgs());
    }

    /**
     * Gets sent messages counts (grouped by node).
     *
     * @return Map containing receiver nodes and respective counts.
     */
    public Map<UUID, Long> sentMessagesByNode() {
        return convertStatistics(msgsCntrByNodeAll, c -> c.sentMsgs());
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

        for (Map<UUID, MessageCounter> map : msgsCntrByNodeAll)
            map.clear();

        for (Map<String, MessageCounter> map : msgsCntrByTypeAll)
            map.clear();
    }
}
