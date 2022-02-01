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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionDemandMessage;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsSingleMessage;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME;

/**
 *
 */
public class TestRecordingCommunicationSpi extends TcpCommunicationSpi {
    /** */
    private Set<Class<?>> recordClasses;

    /** */
    private IgniteBiPredicate<ClusterNode, Message> recordP;

    /** */
    private List<Object> recordedMsgs = new ArrayList<>();

    /** */
    private List<BlockedMessageDescriptor> blockedMsgs = new ArrayList<>();

    /** */
    private Map<Class<?>, Set<String>> blockCls = new HashMap<>();

    /** */
    private IgniteBiPredicate<ClusterNode, Message> blockP;

    /** */
    private volatile IgniteBiInClosure<ClusterNode, Message> c;

    /**
     * @param node Node.
     * @return Test SPI.
     */
    public static TestRecordingCommunicationSpi spi(Ignite node) {
        return (TestRecordingCommunicationSpi)node.configuration().getCommunicationSpi();
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {
        // All ignite code expects that 'send' fails after discovery listener for node fail finished.
        if (getSpiContext().node(node.id()) == null) {
            throw new IgniteSpiException(new ClusterTopologyCheckedException("Failed to send message" +
                " (node left topology): " + node));
        }

        if (msg instanceof GridIoMessage) {
            GridIoMessage ioMsg = (GridIoMessage)msg;

            Message msg0 = ioMsg.message();

            if (c != null)
                c.apply(node, msg0);

            synchronized (this) {
                boolean record = (recordClasses != null && recordClasses.contains(msg0.getClass())) ||
                    (recordP != null && recordP.apply(node, msg0));

                if (record)
                    recordedMsgs.add(msg0);

                boolean block = false;

                if (blockP != null && blockP.apply(node, msg0))
                    block = true;
                else {
                    Set<String> blockNodes = blockCls.get(msg0.getClass());

                    if (blockNodes != null) {
                        String nodeName = (String)node.attributes().get(ATTR_IGNITE_INSTANCE_NAME);

                        block = blockNodes.contains(nodeName);
                    }
                }

                if (block) {
                    ignite.log().info("Block message [node=" + node.id() + ", order=" + node.order() +
                        ", msg=" + ioMsg.message() + ']');

                    blockedMsgs.add(new BlockedMessageDescriptor(node, ioMsg, MTC.span()));

                    notifyAll();

                    return;
                }
                else if (record)
                    notifyAll();
            }
        }

        super.sendMessage(node, msg, ackC);
    }

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg) throws IgniteSpiException {
        sendMessage(node, msg, null);
    }

    /**
     * @param recordP Record predicate.
     */
    public void record(IgniteBiPredicate<ClusterNode, Message> recordP) {
        synchronized (this) {
            this.recordP = recordP;
        }
    }

    /**
     * @param recordClasses Message classes to record.
     */
    public void record(Class<?>... recordClasses) {
        synchronized (this) {
            if (this.recordClasses == null)
                this.recordClasses = new HashSet<>();

            Collections.addAll(this.recordClasses, recordClasses);

            recordedMsgs = new ArrayList<>();
        }
    }

    /**
     * @param stopRecord Stop record flag.
     * @return Recorded messages.
     */
    public List<Object> recordedMessages(boolean stopRecord) {
        synchronized (this) {
            List<Object> msgs = recordedMsgs;

            recordedMsgs = new ArrayList<>();

            if (stopRecord)
                recordClasses = null;

            return msgs;
        }
    }

    /**
     * @return {@code True} if there are blocked messages.
     */
    public boolean hasBlockedMessages() {
        synchronized (this) {
            return !blockedMsgs.isEmpty();
        }
    }

    /**
     * @param cls Message class.
     * @param nodeName Node name.
     * @throws InterruptedException If interrupted.
     */
    public void waitForBlocked(Class<?> cls, String nodeName) throws InterruptedException {
        synchronized (this) {
            while (!hasMessage(cls, nodeName))
                wait();
        }
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    public void waitForBlocked() throws InterruptedException {
        waitForBlocked(1);
    }

    /**
     * @param size Number of messages to wait for.
     * @throws InterruptedException If interrupted.
     */
    public void waitForBlocked(int size) throws InterruptedException {
        synchronized (this) {
            while (blockedMsgs.size() < size)
                wait();
        }
    }

    /**
     * @param size Size
     * @param timeout Timeout.
     * @throws InterruptedException
     */
    public boolean waitForBlocked(int size, long timeout) throws InterruptedException {
        long t0 = U.currentTimeMillis() + timeout;

        synchronized (this) {
            while (blockedMsgs.size() < size) {
                wait(1000);

                if (U.currentTimeMillis() >= t0)
                    return false;
            }
        }

        return true;
    }

    /**
     * @throws InterruptedException If interrupted.
     */
    public void waitForRecorded() throws InterruptedException {
        synchronized (this) {
            while (recordedMsgs.isEmpty())
                wait();
        }
    }

    /**
     * @param cls Message class.
     * @param nodeName Node name.
     * @return {@code True} if has blocked message.
     */
    private boolean hasMessage(Class<?> cls, String nodeName) {
        for (BlockedMessageDescriptor blockedMsg : blockedMsgs) {
            if (blockedMsg.ioMessage().message().getClass() == cls &&
                nodeName.equals(blockedMsg.destinationNode().attribute(ATTR_IGNITE_INSTANCE_NAME)))
                return true;
        }

        return false;
    }

    /**
     * @param c Message closure.
     */
    public void closure(IgniteBiInClosure<ClusterNode, Message> c) {
        this.c = c;
    }

    /**
     * @param blockP Message block predicate.
     */
    public void blockMessages(IgniteBiPredicate<ClusterNode, Message> blockP) {
        synchronized (this) {
            this.blockP = blockP;
        }
    }

    /**
     * @param cls Message class.
     * @param nodeName Name of the node where message is sent to.
     */
    public void blockMessages(Class<?> cls, String nodeName) {
        synchronized (this) {
            Set<String> set = blockCls.get(cls);

            if (set == null) {
                set = new HashSet<>();

                blockCls.put(cls, set);
            }

            set.add(nodeName);
        }
    }

    /**
     * Stops block messages and sends all already blocked messages.
     */
    public void stopBlock() {
        stopBlock(true, null, true, true);
    }

    /**
     * Stops block messages and sends all already blocked messages if sndMsgs is 'true'.
     *
     * @param sndMsgs {@code True} to send blocked messages.
     */
    public void stopBlock(boolean sndMsgs) {
        stopBlock(sndMsgs, null, true, true);
    }

    /**
     * Stops block messages and sends all already blocked messages if sndMsgs is 'true' optionally filtered
     * by unblockPred.
     *
     * @param sndMsgs If {@code true} sends blocked messages.
     * @param unblockPred If not null unblocks only messages allowed by predicate.
     */
    public void stopBlock(boolean sndMsgs, @Nullable IgnitePredicate<BlockedMessageDescriptor> unblockPred) {
        stopBlock(sndMsgs, unblockPred, true, true);
    }

    /**
     * Stops block messages and sends all already blocked messages if sndMsgs is 'true' optionally filtered by
     * unblockPred.
     *
     * @param sndMsgs If {@code true} sends blocked messages.
     * @param unblockPred If not null unblocks only messages allowed by predicate.
     * @param clearFilters {@code true} to clear filters.
     * @param rmvBlockedMsgs {@code true} to remove blocked messages. Sometimes useful in conjunction with {@code
     * sndMsgs=false}.
     */
    public void stopBlock(boolean sndMsgs, @Nullable IgnitePredicate<BlockedMessageDescriptor> unblockPred,
        boolean clearFilters, boolean rmvBlockedMsgs) {
        synchronized (this) {
            if (clearFilters) {
                blockCls.clear();
                blockP = null;
            }

            Iterator<BlockedMessageDescriptor> iter = blockedMsgs.iterator();

            while (iter.hasNext()) {
                BlockedMessageDescriptor blockedMsg = iter.next();

                // It is important what predicate if called only once for each message.
                if (unblockPred != null && !unblockPred.apply(blockedMsg))
                    continue;

                if (sndMsgs) {
                    try (TraceSurroundings ignored = MTC.supportContinual(blockedMsg.span())) {
                        ignite.log().info("Send blocked message " + blockedMsg);

                        super.sendMessage(blockedMsg.destinationNode(), blockedMsg.ioMessage());
                    }
                    catch (Throwable e) {
                        U.error(ignite.log(), "Failed to send blocked message: " + blockedMsg, e);
                    }
                }

                if (rmvBlockedMsgs)
                    iter.remove();
            }
        }
    }

    /**
     * Stop blocking all messages.
     */
    public static void stopBlockAll() {
        for (Ignite ignite : G.allGrids())
            spi(ignite).stopBlock(true);
    }

    /**
     * @param grpId Group id.
     */
    public static IgniteBiPredicate<ClusterNode, Message> blockDemandMessageForGroup(int grpId) {
        return new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionDemandMessage) {
                    GridDhtPartitionDemandMessage msg0 = (GridDhtPartitionDemandMessage)msg;

                    return msg0.groupId() == grpId;
                }

                return false;
            }
        };
    }

    /** */
    public static IgniteBiPredicate<ClusterNode, Message> blockSingleExhangeMessage() {
        return new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionsSingleMessage) {
                    GridDhtPartitionsSingleMessage msg0 = (GridDhtPartitionsSingleMessage)msg;

                    return msg0.exchangeId() != null;
                }

                return false;
            }
        };
    }

    /** */
    public static IgniteBiPredicate<ClusterNode, Message> blockSinglePartitionStateMessage() {
        return new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode clusterNode, Message msg) {
                if (msg instanceof GridDhtPartitionsSingleMessage) {
                    GridDhtPartitionsSingleMessage msg0 = (GridDhtPartitionsSingleMessage)msg;

                    return msg0.exchangeId() == null;
                }

                return false;
            }
        };
    }

    /**
     * Description of the blocked message.
     */
    public static class BlockedMessageDescriptor {
        /** Destination node for the blocked message. */
        private final ClusterNode destNode;

        /** Blocked message. */
        private final GridIoMessage msg;

        /** Span in which context sending must be done. */
        private final Span span;

        /**
         *
         */
        public BlockedMessageDescriptor(ClusterNode destNode, GridIoMessage msg, Span span) {
            this.destNode = destNode;
            this.msg = msg;
            this.span = span;
        }

        /**
         * @return Destination node for the blocked message.
         */
        public ClusterNode destinationNode() {
            return destNode;
        }

        /**
         * @return Blocked message.
         */
        public GridIoMessage ioMessage() {
            return msg;
        }

        /**
         * @return Span in which context sending must be done.
         */
        public Span span() {
            return span;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "[node=" + destNode.id() + ", order=" + destNode.order() + ", msg=" + msg.message() + ']';
        }
    }
}
