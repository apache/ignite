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
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
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
    private List<T2<ClusterNode, GridIoMessage>> blockedMsgs = new ArrayList<>();

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

                    blockedMsgs.add(new T2<>(node, ioMsg));

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
        for (T2<ClusterNode, GridIoMessage> msg : blockedMsgs) {
            if (msg.get2().message().getClass() == cls &&
                nodeName.equals(msg.get1().attribute(ATTR_IGNITE_INSTANCE_NAME)))
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
     * @param nodeName Node name.
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
        stopBlock(true, null);
    }

    /**
     * Stops block messages and sends all already blocked messages if sndMsgs is 'true'.
     *
     * @param sndMsgs {@code True} to send blocked messages.
     */
    public void stopBlock(boolean sndMsgs) {
        stopBlock(sndMsgs, null);
    }

    /**
     * Stops block messages and sends all already blocked messages if sndMsgs is 'true' optionally filtered
     * by unblockPred.
     *
     * @param sndMsgs If {@code true} sends blocked messages.
     * @param unblockPred If not null unblocks only messages allowed by predicate.
     */
    public void stopBlock(boolean sndMsgs, @Nullable IgnitePredicate<T2<ClusterNode, GridIoMessage>> unblockPred) {
        synchronized (this) {
            blockCls.clear();
            blockP = null;

            Collection<T2<ClusterNode, GridIoMessage>> msgs =
                unblockPred == null ? blockedMsgs : F.view(blockedMsgs, unblockPred);

            if (sndMsgs) {
                for (T2<ClusterNode, GridIoMessage> msg : msgs) {
                    try {
                        ignite.log().info("Send blocked message [node=" + msg.get1().id() +
                            ", order=" + msg.get1().order() +
                            ", msg=" + msg.get2().message() + ']');

                        super.sendMessage(msg.get1(), msg.get2());
                    }
                    catch (Throwable e) {
                        U.error(ignite.log(), "Failed to send blocked message: " + msg, e);
                    }
                }
            }

            msgs.clear();
        }
    }
}
