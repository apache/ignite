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

import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
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

    /** Received messages count grouped by message type. */
    private final GroupingCounter<String> rcvdMsgsCntByType = new GroupingCounter<>();

    /** Received messages count grouped by sender. */
    private final GroupingCounter<UUID> rcvdMsgsCntByNode = new GroupingCounter<>();

    /** Sent messages count grouped by message type. */
    private final GroupingCounter<String> sentMsgsCntByType = new GroupingCounter<>();

    /** Sent messages count grouped by receiver. */
    private final GroupingCounter<UUID> sentMsgsCntByNode = new GroupingCounter<>();

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

        sentMsgsCntByType.increment(msg.getClass().getSimpleName());
        sentMsgsCntByNode.increment(nodeId);
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

        rcvdMsgsCntByType.increment(msg.getClass().getSimpleName());
        rcvdMsgsCntByNode.increment(nodeId);
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
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> receivedMessagesByType() {
        return rcvdMsgsCntByType.toMap();
    }

    /**
     * Gets received messages counts (grouped by node).
     *
     * @return Map containing sender nodes and respective counts.
     */
    public Map<UUID, Long> receivedMessagesByNode() {
        return rcvdMsgsCntByNode.toMap();
    }

    /**
     * Gets sent messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> sentMessagesByType() {
        return sentMsgsCntByType.toMap();
    }

    /**
     * Gets sent messages counts (grouped by node).
     *
     * @return Map containing receiver nodes and respective counts.
     */
    public Map<UUID, Long> sentMessagesByNode() {
        return sentMsgsCntByNode.toMap();
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

        sentMsgsCntByType.reset();
        rcvdMsgsCntByType.reset();
        sentMsgsCntByNode.reset();
        rcvdMsgsCntByNode.reset();
    }

    /**
     * Grouping counter.
     *
     * @param <T> Key class.
     */
    private static class GroupingCounter<T> {
        /** Concurrency level. */
        private static final int CONCURRENCY_LEVEL = 32;

        /** Grouping maps. */
        private final ConcurrentMap<T, LongAdder> maps[] = new ConcurrentMap[CONCURRENCY_LEVEL];

        /**
         * Default constructor.
         */
        public GroupingCounter() {
            for (int i = 0; i < CONCURRENCY_LEVEL; i++)
                maps[i] = new ConcurrentHashMap<>();
        }

        /**
         * Increment counter for key.
         *
         * @param key Key.
         */
        public void increment(T key) {
            ConcurrentMap<T, LongAdder> map = maps[ThreadLocalRandom.current().nextInt(CONCURRENCY_LEVEL)];

            LongAdder adder = F.addIfAbsent(map, key, (Callable<LongAdder>)LongAdder::new);

            adder.increment();
        }

        /**
         * Reset counters.
         */
        public void reset() {
            for (Map<T, LongAdder> map : maps)
                map.clear();
        }

        /**
         * Convert statistics from internal representation to map.
         *
         * @return Result map.
         */
        public  Map<T, Long> toMap() {
            return Arrays.stream(maps)
                .flatMap(m -> m.entrySet().stream())
                .collect(
                    Collectors.groupingBy(
                        Map.Entry::getKey,
                        Collectors.summarizingLong(e -> e.getValue().longValue())
                    )
                ).entrySet().stream()
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        e -> e.getValue().getSum()
                    )
                );
        }
    }
}
