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

package org.apache.ignite.spi.discovery.tcp.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.metric.impl.AtomicLongMetric;
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.util.GridBoundedLinkedHashMap;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryCustomEventMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddFinishedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeAddedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeFailedMessage;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryNodeLeftMessage;

/**
 * Statistics for {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}.
 */
public class TcpDiscoveryStatistics {
    /** Join started timestamp. */
    private final AtomicLongMetric joinStartedTs;

    /** Join finished timestamp. */
    private final AtomicLongMetric joinFinishedTs;

    /** Coordinator since timestamp. */
    private final AtomicLong crdSinceTs = new AtomicLong();

    /** Joined nodes count. */
    private final IntMetricImpl joinedNodesCnt;

    /** Failed nodes count. */
    private final IntMetricImpl failedNodesCnt;

    /** Left nodes count. */
    private final IntMetricImpl leftNodesCnt;

    /** Ack timeouts count. */
    private final IntMetricImpl ackTimeoutsCnt;

    /** Socket timeouts count. */
    private final IntMetricImpl sockTimeoutsCnt;

    /** Received messages. */
    @GridToStringInclude
    private final Map<String, Integer> rcvdMsgs = new HashMap<>();

    /** Processed messages. */
    @GridToStringInclude
    private final Map<String, Integer> procMsgs = new HashMap<>();

    /** Average time taken to serialize messages. */
    @GridToStringInclude
    private final Map<String, Long> avgMsgsSndTimes = new HashMap<>();

    /** Average time taken to serialize messages. */
    @GridToStringInclude
    private final Map<String, Long> maxMsgsSndTimes = new HashMap<>();

    /** Sent messages. */
    @GridToStringInclude
    private final Map<String, Integer> sentMsgs = new HashMap<>();

    /** Messages receive timestamps. */
    private final Map<IgniteUuid, Long> msgsRcvTs = new GridBoundedLinkedHashMap<>(1024);

    /** Messages processing start timestamps. */
    private final Map<IgniteUuid, Long> msgsProcStartTs = new GridBoundedLinkedHashMap<>(1024);

    /** Ring messages sent timestamps. */
    private final Map<IgniteUuid, Long> ringMsgsSndTs = new GridBoundedLinkedHashMap<>(1024);

    /** */
    @GridToStringInclude
    private final Map<String, Long> avgMsgsAckTimes = new HashMap<>();

    /** */
    @GridToStringInclude
    private final Map<String, Long> maxMsgsAckTimes = new HashMap<>();

    /** Average time messages is in queue. */
    private long avgMsgQueueTime;

    /** Max time messages is in queue. */
    private long maxMsgQueueTime;

    /** Total number of ring messages sent. */
    private final IntMetricImpl ringMsgsSent;

    /** Average time it takes for messages to pass the full ring. */
    private long avgRingMsgTime;

    /** Max time it takes for messages to pass the full ring. */
    private long maxRingMsgTime;

    /** Class name of ring message that required the biggest time for full ring traverse. */
    private String maxRingTimeMsgCls;

    /** Average message processing time. */
    private long avgMsgProcTime;

    /** Max message processing time. */
    private long maxMsgProcTime;

    /** Class name of the message that required the biggest time to process. */
    private String maxProcTimeMsgCls;

    /** Socket readers created count. */
    private final IntMetricImpl sockReadersCreated;

    /** Socket readers removed count. */
    private final IntMetricImpl sockReadersRmv;

    /** Average time it takes to initialize connection from another node. */
    private long avgSrvSockInitTime;

    /** Max time it takes to initialize connection from another node. */
    private long maxSrvSockInitTime;

    /** Number of outgoing connections established. */
    private final IntMetricImpl clientSockCreatedCnt;

    /** Average time it takes to connect to another node. */
    private long avgClientSockInitTime;

    /** Max time it takes to connect to another node. */
    private long maxClientSockInitTime;

    /** Pending messages registered count. */
    private final IntMetricImpl pendingMsgsRegistered;

    /** Pending messages discarded count. */
    private final IntMetricImpl pendingMsgsDiscarded;

    /**
     * @param discoReg Discovery metric registry.
     */
    public TcpDiscoveryStatistics(MetricRegistry discoReg) {
        joinStartedTs = discoReg.longMetric("JoinStarted", "Join started timestamp");

        joinFinishedTs = discoReg.longMetric("JoinFinished", "Join finished timestamp");

        discoReg.register("CoordinatorSince", crdSinceTs::get, "Coordinator since timestamp");

        joinedNodesCnt = discoReg.intMetric("JoinedNodesCount", "Joined nodes count");

        failedNodesCnt = discoReg.intMetric("FailedNodesCount", "Failed nodes count");

        leftNodesCnt = discoReg.intMetric("LeftNodesCount", "Left nodes count");

        ackTimeoutsCnt = discoReg.intMetric("AckTimeoutsCount", "Ack timeouts count");

        sockTimeoutsCnt = discoReg.intMetric("SockTimeoutsCount", "Socket timeouts count");

        ringMsgsSent = discoReg.intMetric("RingMessagesSent", "Total number of ring messages sent");

        sockReadersCreated = discoReg.intMetric("SocketReadersCreated", "Socket readers created count");

        sockReadersRmv = discoReg.intMetric("SocketReadersRemoved", "Socket readers removed count");

        clientSockCreatedCnt = discoReg.intMetric("ClientSocketCreatedCount",
            "Number of outgoing connections established");

        pendingMsgsRegistered = discoReg.intMetric("PendingMessagesRegistered", "Pending messages registered count");

        pendingMsgsDiscarded = discoReg.intMetric("PendingMessagesDiscarded", "Pending messages discarded count");
    }

    /**
     * Increments joined nodes count.
     */
    public void onNodeJoined() {
        joinedNodesCnt.increment();
    }

    /**
     * Increments left nodes count.
     */
    public void onNodeLeft() {
        leftNodesCnt.increment();
    }

    /**
     * Increments failed nodes count.
     */
    public void onNodeFailed() {
        failedNodesCnt.increment();
    }

    /**
     * Increments ack timeouts count.
     */
    public void onAckTimeout() {
        ackTimeoutsCnt.increment();
    }

    /**
     * Increments socket timeouts count.
     */
    public void onSocketTimeout() {
        sockTimeoutsCnt.increment();
    }

    /**
     * Initializes coordinator since date (if needed).
     */
    public void onBecomingCoordinator() {
        crdSinceTs.compareAndSet(0, U.currentTimeMillis());
    }

    /**
     * Initializes join started timestamp.
     */
    public void onJoinStarted() {
        joinStartedTs.value(U.currentTimeMillis());
    }

    /**
     * Initializes join finished timestamp.
     */
    public void onJoinFinished() {
        joinFinishedTs.value(U.currentTimeMillis());
    }

    /**
     * @return Join started timestamp.
     */
    public long joinStarted() {
        return joinStartedTs.value();
    }

    /**
     * @return Join finished timestamp.
     */
    public long joinFinished() {
        return joinFinishedTs.value();
    }

    /**
     * Collects necessary stats for message received by SPI.
     *
     * @param msg Received message.
     */
    public synchronized void onMessageReceived(TcpDiscoveryAbstractMessage msg) {
        assert msg != null;

        Integer cnt = F.addIfAbsent(rcvdMsgs, msg.getClass().getSimpleName(),  0);

        rcvdMsgs.put(msg.getClass().getSimpleName(), ++cnt);

        msgsRcvTs.put(msg.id(), U.currentTimeMillis());
    }

    /**
     * Collects necessary stats for message processed by SPI.
     *
     * @param msg Processed message.
     */
    public synchronized void onMessageProcessingStarted(TcpDiscoveryAbstractMessage msg) {
        assert msg != null;

        Integer cnt = F.addIfAbsent(procMsgs, msg.getClass().getSimpleName(), 0);

        procMsgs.put(msg.getClass().getSimpleName(), ++cnt);

        Long rcvdTs = msgsRcvTs.remove(msg.id());

        if (rcvdTs != null) {
            long duration = U.currentTimeMillis() - rcvdTs;

            if (maxMsgQueueTime < duration)
                maxMsgQueueTime = duration;

            int totalProcMsgs = totalProcessedMessages();

            if (totalProcMsgs != 0)
                avgMsgQueueTime = (avgMsgQueueTime * (totalProcMsgs - 1)) / totalProcMsgs;
        }

        msgsProcStartTs.put(msg.id(), U.currentTimeMillis());
    }

    /**
     * Collects necessary stats for message processed by SPI.
     *
     * @param msg Processed message.
     */
    public synchronized void onMessageProcessingFinished(TcpDiscoveryAbstractMessage msg) {
        assert msg != null;

        Long startTs = msgsProcStartTs.get(msg.id());

        if (startTs != null) {
            long duration = U.currentTimeMillis() - startTs;

            int totalProcMsgs = totalProcessedMessages();

            if (totalProcMsgs != 0)
                avgMsgProcTime = (avgMsgProcTime * (totalProcMsgs - 1) + duration) / totalProcMsgs;

            if (duration > maxMsgProcTime) {
                maxMsgProcTime = duration;

                maxProcTimeMsgCls = msg.getClass().getSimpleName();
            }

            msgsProcStartTs.remove(msg.id());
        }
    }

    /**
     * Called by coordinator when ring message is sent.
     *  @param msg Sent message.
     * @param time Time taken to serialize message.
     */
    public synchronized void onMessageSent(TcpDiscoveryAbstractMessage msg, long time) {
        assert msg != null;
        assert time >= 0 : time;

        if (crdSinceTs.get() > 0 &&
            (msg instanceof TcpDiscoveryCustomEventMessage) ||
            (msg instanceof TcpDiscoveryNodeAddedMessage) ||
            (msg instanceof TcpDiscoveryNodeAddFinishedMessage) ||
            (msg instanceof TcpDiscoveryNodeLeftMessage) ||
            (msg instanceof TcpDiscoveryNodeFailedMessage)) {
            ringMsgsSndTs.put(msg.id(), U.currentTimeMillis());

            ringMsgsSent.increment();
        }

        Integer cnt = F.addIfAbsent(sentMsgs, msg.getClass().getSimpleName(), 0);

        sentMsgs.put(msg.getClass().getSimpleName(), ++cnt);

        addTimeInfo(avgMsgsSndTimes, maxMsgsSndTimes, msg, cnt, time);

        addTimeInfo(avgMsgsAckTimes, maxMsgsAckTimes, msg, cnt, time);
    }

    /**
     * @param avgTimes Average times.
     * @param maxTimes Max times.
     * @param msg Message.
     * @param cnt Total message count.
     * @param time Time.
     */
    private void addTimeInfo(Map<String, Long> avgTimes,
        Map<String, Long> maxTimes,
        TcpDiscoveryAbstractMessage msg,
        int cnt,
        long time) {

        Long avgTime = F.addIfAbsent(avgTimes, msg.getClass().getSimpleName(), 0L);

        avgTime = (avgTime * (cnt - 1) + time) / cnt;

        avgTimes.put(msg.getClass().getSimpleName(), avgTime);

        Long maxTime = F.addIfAbsent(maxTimes, msg.getClass().getSimpleName(), 0L);

        if (time > maxTime)
            maxTimes.put(msg.getClass().getSimpleName(), time);
    }

    /**
     * Called by coordinator when ring message makes full pass.
     *
     * @param msg Message.
     */
    public synchronized void onRingMessageReceived(TcpDiscoveryAbstractMessage msg) {
        assert msg != null;

        Long sentTs = ringMsgsSndTs.get(msg.id());

        if (sentTs != null) {
            long duration = U.currentTimeMillis() - sentTs;

            if (maxRingMsgTime < duration) {
                maxRingMsgTime = duration;

                maxRingTimeMsgCls = msg.getClass().getSimpleName();
            }

            if (ringMsgsSent.value() != 0) {
                int val = ringMsgsSent.value();

                avgRingMsgTime = (avgRingMsgTime * (val - 1) + duration) / val;
            }
        }
    }

    /**
     * Gets max time for ring message to make full pass.
     *
     * @return Max full pass time.
     */
    public synchronized long maxRingMessageTime() {
        return maxRingMsgTime;
    }

    /**
     * Gets class name of the message that took max time to make full pass.
     *
     * @return Message class name.
     */
    public synchronized String maxRingDurationMessageClass() {
        return maxRingTimeMsgCls;
    }

    /**
     * Gets class name of the message took max time to process.
     *
     * @return Message class name.
     */
    public synchronized String maxProcessingTimeMessageClass() {
        return maxProcTimeMsgCls;
    }

    /**
     * @param initTime Time socket was initialized in.
     */
    public synchronized void onServerSocketInitialized(long initTime) {
        if (initTime < 0)
            initTime = 0;

        if (maxSrvSockInitTime < initTime)
            maxSrvSockInitTime = initTime;

        int val = sockReadersCreated.value();

        avgSrvSockInitTime = (avgSrvSockInitTime * (val - 1) + initTime) / val;
    }

    /**
     * @param initTime Time socket was initialized in.
     */
    public synchronized void onClientSocketInitialized(long initTime) {
        if (initTime < 0)
            initTime = 0;

        clientSockCreatedCnt.increment();

        if (maxClientSockInitTime < initTime)
            maxClientSockInitTime = initTime;

        int val = clientSockCreatedCnt.value();

        avgClientSockInitTime = (avgClientSockInitTime * (val - 1) + initTime) / val;
    }

    /**
     * Increments pending messages registered count.
     */
    public void onPendingMessageRegistered() {
        pendingMsgsRegistered.increment();
    }

    /**
     * Increments pending messages discarded count.
     */
    public void onPendingMessageDiscarded() {
        pendingMsgsDiscarded.increment();
    }

    /**
     * Increments socket readers created count.
     */
    public void onSocketReaderCreated() {
        sockReadersCreated.increment();
    }

    /**
     * Increments socket readers removed count.
     */
    public void onSocketReaderRemoved() {
        sockReadersRmv.increment();
    }

    /**
     * Gets processed messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public synchronized Map<String, Integer> processedMessages() {
        return new HashMap<>(procMsgs);
    }

    /**
     * Gets received messages counts (grouped by type).
     *
     * @return Map containing message types and respective counts.
     */
    public synchronized Map<String, Integer> receivedMessages() {
        return new HashMap<>(rcvdMsgs);
    }

    /**
     * @return Sent messages counts (grouped by type).
     */
    public synchronized Map<String, Integer> sentMessages() {
        return new HashMap<>(sentMsgs);
    }

    /**
     * Gets max messages send time (grouped by type).
     *
     * @return Map containing messages types and max send times.
     */
    public synchronized Map<String, Long> maxMessagesSendTimes() {
        return new HashMap<>(maxMsgsSndTimes);
    }

    /**
     * Gets average messages send time (grouped by type).
     *
     * @return Map containing messages types and average send times.
     */
    public synchronized Map<String, Long> avgMessagesSendTimes() {
        return new HashMap<>(avgMsgsSndTimes);
    }

    /**
     * Gets total received messages count.
     *
     * @return Total received messages count.
     */
    public synchronized int totalReceivedMessages() {
        return F.sumInt(rcvdMsgs.values());
    }

    /**
     * Gets total processed messages count.
     *
     * @return Total processed messages count.
     */
    public synchronized int totalProcessedMessages() {
        return F.sumInt(procMsgs.values());
    }

    /**
     * Gets max message processing time.
     *
     * @return Max message processing time.
     */
    public synchronized long maxMessageProcessingTime() {
        return maxMsgProcTime;
    }

    /**
     * Gets average message processing time.
     *
     * @return Average message processing time.
     */
    public synchronized long avgMessageProcessingTime() {
        return avgMsgProcTime;
    }

    /**
     * Gets pending messages registered count.
     *
     * @return Pending messages registered count.
     */
    public long pendingMessagesRegistered() {
        return pendingMsgsRegistered.value();
    }

    /**
     * Gets pending messages discarded count.
     *
     * @return Pending messages registered count.
     */
    public long pendingMessagesDiscarded() {
        return pendingMsgsDiscarded.value();
    }

    /**
     * Gets nodes joined count.
     *
     * @return Nodes joined count.
     */
    public int joinedNodesCount() {
        return joinedNodesCnt.value();
    }

    /**
     * Gets nodes left count.
     *
     * @return Nodes left count.
     */
    public int leftNodesCount() {
        return leftNodesCnt.value();
    }

    /**
     * Gets failed nodes count.
     *
     * @return Failed nodes count.
     */
    public int failedNodesCount() {
        return failedNodesCnt.value();
    }

    /**
     * @return Ack timeouts count.
     */
    public int ackTimeoutsCount() {
        return ackTimeoutsCnt.value();
    }

    /**
     * @return Socket timeouts count.
     */
    public int socketTimeoutsCount() {
        return sockTimeoutsCnt.value();
    }

    /**
     * Gets socket readers created count.
     *
     * @return Socket readers created count.
     */
    public int socketReadersCreated() {
        return sockReadersCreated.value();
    }

    /**
     * Gets socket readers removed count.
     *
     * @return Socket readers removed count.
     */
    public int socketReadersRemoved() {
        return sockReadersRmv.value();
    }

    /**
     * Gets time local node has been coordinator since.
     *
     * @return Coordinator since timestamp.
     */
    public long coordinatorSinceTimestamp() {
        return crdSinceTs.get();
    }

    /**
     * Clears statistics.
     */
    public synchronized void clear() {
        ackTimeoutsCnt.reset();
        avgClientSockInitTime = 0;
        avgMsgProcTime = 0;
        avgMsgQueueTime = 0;
        avgMsgsAckTimes.clear();
        avgMsgsSndTimes.clear();
        avgRingMsgTime = 0;
        avgSrvSockInitTime = 0;
        clientSockCreatedCnt.reset();
        crdSinceTs.set(0);
        failedNodesCnt.reset();
        joinedNodesCnt.reset();
        joinFinishedTs.reset();
        joinStartedTs.reset();
        leftNodesCnt.reset();
        maxClientSockInitTime = 0;
        maxMsgProcTime = 0;
        maxMsgQueueTime = 0;
        maxMsgsAckTimes.clear();
        maxMsgsSndTimes.clear();
        maxProcTimeMsgCls = null;
        maxRingMsgTime = 0;
        maxRingTimeMsgCls = null;
        maxSrvSockInitTime = 0;
        pendingMsgsDiscarded.reset();
        pendingMsgsRegistered.reset();
        procMsgs.clear();
        rcvdMsgs.clear();
        ringMsgsSent.reset();
        sentMsgs.clear();
        sockReadersCreated.reset();
        sockReadersRmv.reset();
        sockTimeoutsCnt.reset();
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(TcpDiscoveryStatistics.class, this);
    }
}
