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
import org.apache.ignite.internal.processors.metric.impl.IntMetricImpl;
import org.apache.ignite.internal.util.GridBoundedLinkedHashMap;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.spi.discovery.tcp.messages.TcpDiscoveryAbstractMessage;

import static org.apache.ignite.internal.managers.discovery.GridDiscoveryManager.DISCO_METRICS;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;

/**
 * Statistics for {@link org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi}.
 */
public class TcpDiscoveryStatistics {
    /** Coordinator since timestamp. */
    private final AtomicLong crdSinceTs = new AtomicLong();

    /** Joined nodes count. */
    private final IntMetricImpl joinedNodesCnt;

    /** Failed nodes count. */
    private final IntMetricImpl failedNodesCnt;

    /** Left nodes count. */
    private final IntMetricImpl leftNodesCnt;

    /** Received messages. */
    @GridToStringInclude
    private final Map<String, Integer> rcvdMsgs = new HashMap<>();

    /** Processed messages. */
    @GridToStringInclude
    private final Map<String, Integer> procMsgs = new HashMap<>();

    /** Sent messages. */
    @GridToStringInclude
    private final Map<String, Integer> sentMsgs = new HashMap<>();

    /** Messages processing start timestamps. */
    private final Map<IgniteUuid, Long> msgsProcStartTs = new GridBoundedLinkedHashMap<>(1024);

    /** Average message processing time. */
    private long avgMsgProcTime;

    /** Max message processing time. */
    private long maxMsgProcTime;

    /** Pending messages registered count. */
    private final IntMetricImpl pendingMsgsRegistered;

    /** Metric that indicates connections count that were rejected due to SSL errors. */
    private final IntMetricImpl rejectedSslConnectionsCnt;

    /** */
    public TcpDiscoveryStatistics() {
        joinedNodesCnt = new IntMetricImpl(metricName(DISCO_METRICS, "JoinedNodes"), "Joined nodes count");

        failedNodesCnt = new IntMetricImpl(metricName(DISCO_METRICS, "FailedNodes"), "Failed nodes count");

        leftNodesCnt = new IntMetricImpl(metricName(DISCO_METRICS, "LeftNodes"), "Left nodes count");

        pendingMsgsRegistered = new IntMetricImpl(metricName(DISCO_METRICS, "PendingMessagesRegistered"),
            "Pending messages registered count");

        rejectedSslConnectionsCnt = new IntMetricImpl(
            metricName(DISCO_METRICS, "RejectedSslConnectionsCount"),
            "TCP discovery connections count that were rejected due to SSL errors."
        );
    }

    /**
     * @param discoReg Discovery metric registry.
     */
    public void registerMetrics(MetricRegistry discoReg) {
        discoReg.register("TotalProcessedMessages", this::totalProcessedMessages, "Total processed messages count");

        discoReg.register("TotalReceivedMessages", this::totalReceivedMessages, "Total received messages count");

        discoReg.register(joinedNodesCnt);
        discoReg.register(failedNodesCnt);
        discoReg.register(leftNodesCnt);
        discoReg.register(pendingMsgsRegistered);
        discoReg.register(rejectedSslConnectionsCnt);
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
     * Initializes coordinator since date (if needed).
     */
    public void onBecomingCoordinator() {
        crdSinceTs.compareAndSet(0, U.currentTimeMillis());
    }

    /** Increments connections count that were rejected due to SSL errors. */
    public void onSslConnectionRejected() {
        rejectedSslConnectionsCnt.increment();
    }

    /**
     * Collects necessary stats for message received by SPI.
     *
     * @param msg Received message.
     */
    public synchronized void onMessageReceived(TcpDiscoveryAbstractMessage msg) {
        assert msg != null;

        Integer cnt = F.addIfAbsent(rcvdMsgs, msg.getClass().getSimpleName(), 0);

        rcvdMsgs.put(msg.getClass().getSimpleName(), ++cnt);
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

        msgsProcStartTs.put(msg.id(), U.currentTimeMillis());
    }

    /**
     * Collects necessary stats for message processed by SPI.
     *
     * @param msg Processed message.
     */
    public synchronized void onMessageProcessingFinished(TcpDiscoveryAbstractMessage msg) {
        assert msg != null;

        Long startTs = msgsProcStartTs.remove(msg.id());

        if (startTs != null) {
            long duration = U.currentTimeMillis() - startTs;

            int totalProcMsgs = totalProcessedMessages();

            if (totalProcMsgs != 0)
                avgMsgProcTime = (avgMsgProcTime * (totalProcMsgs - 1) + duration) / totalProcMsgs;

            if (duration > maxMsgProcTime)
                maxMsgProcTime = duration;
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

        Integer cnt = F.addIfAbsent(sentMsgs, msg.getClass().getSimpleName(), 0);

        sentMsgs.put(msg.getClass().getSimpleName(), ++cnt);
    }

    /**
     * Increments pending messages registered count.
     */
    public void onPendingMessageRegistered() {
        pendingMsgsRegistered.increment();
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
        avgMsgProcTime = 0;
        crdSinceTs.set(0);
        failedNodesCnt.reset();
        joinedNodesCnt.reset();
        leftNodesCnt.reset();
        maxMsgProcTime = 0;
        pendingMsgsRegistered.reset();
        procMsgs.clear();
        rcvdMsgs.clear();
        sentMsgs.clear();
        rejectedSslConnectionsCnt.reset();
    }

    /** {@inheritDoc} */
    @Override public synchronized String toString() {
        return S.toString(TcpDiscoveryStatistics.class, this);
    }
}
