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

package org.apache.ignite.internal.processors.query.calcite.message;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.ignite.internal.processors.query.calcite.exec.QueryTaskExecutor;
import org.apache.ignite.lang.IgniteInternalCheckedException;
import org.apache.ignite.lang.IgniteInternalException;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.MessagingService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TopologyService;

import static org.apache.ignite.internal.processors.query.calcite.message.SqlQueryMessageGroup.GROUP_TYPE;

/**
 *
 */
public class MessageServiceImpl implements MessageService {
    private static final UUID QUERY_ID_STUB = UUID.randomUUID();

    private static final IgniteLogger LOG = IgniteLogger.forClass(MessageServiceImpl.class);

    private final TopologyService topSrvc;

    private final MessagingService messagingSrvc;

    /** */
    private final String locNodeId;

    /** */
    private final QueryTaskExecutor taskExecutor;

    /** */
    private Map<Short, MessageListener> lsnrs;

    /** */
    public MessageServiceImpl(
        TopologyService topSrvc,
        MessagingService messagingSrvc,
        QueryTaskExecutor taskExecutor
    ) {
        this.topSrvc = topSrvc;
        this.messagingSrvc = messagingSrvc;
        this.taskExecutor = taskExecutor;

        locNodeId = topSrvc.localMember().id();

        messagingSrvc.addMessageHandler(SqlQueryMessageGroup.class, this::onMessage);
    }

    /** {@inheritDoc} */
    @Override public void send(String nodeId, NetworkMessage msg) throws IgniteInternalCheckedException {
        if (locNodeId.equals(nodeId))
            onMessage(nodeId, msg);
        else {
            ClusterNode node = topSrvc.allMembers().stream()
                .filter(cn -> nodeId.equals(cn.id()))
                .findFirst()
                .orElseThrow(() -> new IgniteInternalException("Failed to send message to node (has node left grid?): " + nodeId));

            try {
                messagingSrvc.send(node, msg).get();
            } catch (Exception ex) {
                if (ex instanceof IgniteInternalCheckedException)
                    throw (IgniteInternalCheckedException)ex;

                throw new IgniteInternalCheckedException(ex);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void register(MessageListener lsnr, short type) {
        if (lsnrs == null)
            lsnrs = new HashMap<>();

        MessageListener old = lsnrs.put(type, lsnr);

        assert old == null : old;
    }

    /** {@inheritDoc} */
    @Override public boolean alive(String nodeId) {
        return topSrvc.allMembers().stream()
            .map(ClusterNode::id)
            .anyMatch(id -> id.equals(nodeId));
    }

    /** */
    protected void onMessage(String nodeId, NetworkMessage msg) {
        if (msg instanceof ExecutionContextAwareMessage) {
            ExecutionContextAwareMessage msg0 = (ExecutionContextAwareMessage) msg;
            taskExecutor.execute(msg0.queryId(), msg0.fragmentId(), () -> onMessageInternal(nodeId, msg));
        }
        else {
            taskExecutor.execute(
                QUERY_ID_STUB,
                ThreadLocalRandom.current().nextLong(1024),
                () -> onMessageInternal(nodeId, msg)
            );
        }
    }

    /** */
    private void onMessage(NetworkMessage msg, NetworkAddress addr, String correlationId) {
        assert msg.groupType() == GROUP_TYPE : "unexpected message group grpType=" + msg.groupType();

        ClusterNode node = topSrvc.getByAddress(addr);
        if (node == null) {
            LOG.warn("Received a message from a node that has not yet" +
                    " joined the cluster: addr={}, msg={}", addr, msg);

            return;
        }

        onMessage(node.id(), msg);
    }

    /** */
    private void onMessageInternal(String nodeId, NetworkMessage msg) {
        MessageListener lsnr = Objects.requireNonNull(
                lsnrs.get(msg.messageType()),
                "there is no listener for msgType=" + msg.messageType()
        );

        lsnr.onMessage(nodeId, msg);
    }
}
