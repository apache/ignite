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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.managers.communication.GridIoMessage;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_GRID_NAME;

/**
 *
 */
public class TestRecordingCommunicationSpi extends TcpCommunicationSpi {
    /** */
    private Class<?> recordCls;

    /** */
    private List<Object> recordedMsgs = new ArrayList<>();

    /** */
    private List<T2<ClusterNode, GridIoMessage>> blockedMsgs = new ArrayList<>();

    /** */
    private Map<Class<?>, Set<String>> blockCls = new HashMap<>();

    /** */
    private IgnitePredicate<GridIoMessage> blockP;

    /** */
    @LoggerResource
    private IgniteLogger log;

    /** {@inheritDoc} */
    @Override public void sendMessage(ClusterNode node, Message msg, IgniteInClosure<IgniteException> ackC)
        throws IgniteSpiException {
        if (msg instanceof GridIoMessage) {
            GridIoMessage ioMsg = (GridIoMessage)msg;

            Object msg0 = ioMsg.message();

            synchronized (this) {
                if (recordCls != null && msg0.getClass().equals(recordCls))
                    recordedMsgs.add(msg0);

                boolean block = false;

                if (blockP != null && blockP.apply(ioMsg))
                    block = true;
                else {
                    Set<String> blockNodes = blockCls.get(msg0.getClass());

                    if (blockNodes != null) {
                        String nodeName = (String)node.attributes().get(ATTR_GRID_NAME);

                        block = blockNodes.contains(nodeName);
                    }
                }

                if (block) {
                    log.info("Block message: " + node.id() + ", msg=" + msg0);

                    blockedMsgs.add(new T2<>(node, ioMsg));

                    return;
                }
            }
        }

        super.sendMessage(node, msg, ackC);
    }

    /**
     * @param recordCls Message class to record.
     */
    public void record(@Nullable Class<?> recordCls) {
        synchronized (this) {
            this.recordCls = recordCls;
        }
    }

    /**
     * @return Recorded messages.
     */
    public List<Object> recordedMessages() {
        synchronized (this) {
            List<Object> msgs = recordedMsgs;

            recordedMsgs = new ArrayList<>();

            return msgs;
        }
    }

    /**
     * @param blockP Message block predicate.
     */
    public void blockMessages(IgnitePredicate<GridIoMessage> blockP) {
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
        synchronized (this) {
            blockCls.clear();

            blockP = null;

            for (T2<ClusterNode, GridIoMessage> msg : blockedMsgs) {
                log.info("Send blocked message: " + msg.get1().id() + ", msg=" + msg.get2() +']');

                super.sendMessage(msg.get1(), msg.get2());
            }

            blockedMsgs.clear();
        }
    }
}
