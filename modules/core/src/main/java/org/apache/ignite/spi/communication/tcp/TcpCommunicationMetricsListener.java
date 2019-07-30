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
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;

import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.nio.GridNioMetricsListener;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.plugin.extensions.communication.Message;

/**
 * Statistics for {@link org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi}.
 */
public class TcpCommunicationMetricsListener implements GridNioMetricsListener{
    /** Counter factory. */
    private static final Callable<LongHolder> HOLDER_FACTORY = new Callable<LongHolder>() {
        @Override public LongHolder call() {
            return new LongHolder();
        }
    };

    /** Received bytes count. */
    private final LongAdder rcvdBytesCnt = new LongAdder();

    /** Sent bytes count.*/
    private final LongAdder sentBytesCnt = new LongAdder();

    /** All registered metrics. */
    private final Set<ThreadMetrics> allMetrics = Collections.newSetFromMap(new ConcurrentHashMap<>());

    /** Thread-local metrics. */
    private final ThreadLocal<ThreadMetrics> threadMetrics = new ThreadLocal<ThreadMetrics>() {
        @Override protected ThreadMetrics initialValue() {
            ThreadMetrics metrics = new ThreadMetrics();

            allMetrics.add(metrics);

            return metrics;
        }
    };

    /** Method to synchronize access to message type map. */
    private final Object msgTypMapMux = new Object();

    /** Message type map. */
    private volatile Map<Short, String> msgTypMap;

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

        if (msg instanceof GridIoMessage) {
            msg = ((GridIoMessage) msg).message();

            updateMessageTypeMap(msg);

            ThreadMetrics metrics = threadMetrics.get();

            metrics.onMessageSent(msg, nodeId);
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

            ThreadMetrics metrics = threadMetrics.get();

            metrics.onMessageReceived(msg, nodeId);
        }
    }

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    public int sentMessagesCount() {
        long res = 0;

        for (ThreadMetrics metrics : allMetrics)
            res += metrics.sentMsgsCnt;

        int res0 = (int)res;

        if (res0 < 0)
            res0 = Integer.MAX_VALUE;

        return res0;
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
        long res = 0;

        for (ThreadMetrics metrics : allMetrics)
            res += metrics.rcvdMsgsCnt;

        int res0 = (int)res;

        if (res0 < 0)
            res0 = Integer.MAX_VALUE;

        return res0;
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
        Map<Short, Long> res = new HashMap<>();

        for (ThreadMetrics metrics : allMetrics)
            addMetrics(res, metrics.rcvdMsgsCntByType);

        return convertMessageTypes(res);
    }

    /**
     * Convert message types.
     *
     * @param input Input map.
     * @return Result map.
     */
    private Map<String, Long> convertMessageTypes(Map<Short, Long> input) {
        Map<String, Long> res = new HashMap<>(input.size());

        Map<Short, String> msgTypMap0 = msgTypMap;

        if (msgTypMap0 != null) {
            for (Map.Entry<Short, Long> inputEntry : input.entrySet()) {
                String typeName = msgTypMap0.get(inputEntry.getKey());

                if (typeName != null)
                    res.put(typeName, inputEntry.getValue());
            }
        }

        return res;
    }

    /**
     * Gets received messages counts (grouped by node).
     *
     * @return Map containing sender nodes and respective counts.
     */
    public Map<UUID, Long> receivedMessagesByNode() {
        Map<UUID, Long> res = new HashMap<>();

        for (ThreadMetrics metrics : allMetrics)
            addMetrics(res, metrics.rcvdMsgsCntByNode);

        return res;
    }

    /**
     * Gets sent messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public Map<String, Long> sentMessagesByType() {
        Map<Short, Long> res = new HashMap<>();

        for (ThreadMetrics metrics : allMetrics)
            addMetrics(res, metrics.sentMsgsCntByType);

        return convertMessageTypes(res);
    }

    /**
     * Gets sent messages counts (grouped by node).
     *
     * @return Map containing receiver nodes and respective counts.
     */
    public Map<UUID, Long> sentMessagesByNode() {
        Map<UUID, Long> res = new HashMap<>();

        for (ThreadMetrics metrics : allMetrics)
            addMetrics(res, metrics.sentMsgsCntByNode);

        return res;
    }

    /**
     * Resets metrics for this instance.
     */
    public void resetMetrics() {
        for (ThreadMetrics metrics : allMetrics)
            metrics.reset();

        sentBytesCnt.reset();
        rcvdBytesCnt.reset();
    }

    /**
     * Add single metrics to the total.
     *
     * @param total Total.
     * @param current Current metrics.
     */
    private <T> void addMetrics(Map<T, Long> total, Map<T, LongHolder> current) {
        for (Map.Entry<T, LongHolder> entry : current.entrySet()) {
            T key = entry.getKey();
            long val = entry.getValue().val;

            Long prevVal = total.get(key);

            total.put(key, prevVal == null ? val : prevVal + val);
        }
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

    /**
     * Long value holder.
     */
    private static class LongHolder {
        /** Value. */
        private long val;

        /**
         * Increment value.
         */
        private void increment() {
            val++;
        }
    }

    /**
     * Thread-local metrics.
     */
    private static class ThreadMetrics {
        /** Received messages count. */
        private long rcvdMsgsCnt;

        /** Sent messages count.*/
        private long sentMsgsCnt;

        /** Received messages count grouped by message type. */
        private final HashMap<Short, LongHolder> rcvdMsgsCntByType = new HashMap<>();

        /** Received messages count grouped by sender. */
        private final HashMap<UUID, LongHolder> rcvdMsgsCntByNode = new HashMap<>();

        /** Sent messages count grouped by message type. */
        private final HashMap<Short, LongHolder> sentMsgsCntByType = new HashMap<>();

        /** Sent messages count grouped by receiver. */
        private final HashMap<UUID, LongHolder> sentMsgsCntByNode = new HashMap<>();

        /**
         * Collects statistics for message sent by SPI.
         *
         * @param msg Sent message.
         * @param nodeId Receiver node id.
         */
        private void onMessageSent(Message msg, UUID nodeId) {
            sentMsgsCnt++;

            LongHolder cntByType = F.addIfAbsent(sentMsgsCntByType, msg.directType(), HOLDER_FACTORY);
            LongHolder cntByNode = F.addIfAbsent(sentMsgsCntByNode, nodeId, HOLDER_FACTORY);

            assert cntByType != null;
            assert cntByNode != null;

            cntByType.increment();
            cntByNode.increment();
        }

        /**
         * Collects statistics for message received by SPI.
         *
         * @param msg Received message.
         * @param nodeId Sender node id.
         */
        private void onMessageReceived(Message msg, UUID nodeId) {
            rcvdMsgsCnt++;

            LongHolder cntByType = F.addIfAbsent(rcvdMsgsCntByType, msg.directType(), HOLDER_FACTORY);
            LongHolder cntByNode = F.addIfAbsent(rcvdMsgsCntByNode, nodeId, HOLDER_FACTORY);

            assert cntByType != null;
            assert cntByNode != null;

            cntByType.increment();
            cntByNode.increment();
        }

        /**
         * Reset metrics.
         */
        private void reset() {
            rcvdMsgsCnt = 0;
            sentMsgsCnt = 0;

            sentMsgsCntByType.clear();
            sentMsgsCntByNode.clear();

            rcvdMsgsCntByType.clear();
            rcvdMsgsCntByNode.clear();
        }
    }
}
