/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.raft.server.impl;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.message.ActionRequest;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.RaftErrorResponse;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.jetbrains.annotations.Nullable;

/**
 * A single node service implementation.
 * @deprecated TODO asch Replace with jraft server https://issues.apache.org/jira/browse/IGNITE-14848
 */
public class RaftServerImpl implements RaftServer {
    /** */
    private static final int QUEUE_SIZE = 1000;

    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(RaftServerImpl.class);

    /** */
    private final RaftClientMessagesFactory clientMsgFactory;

    /** */
    private final ClusterService service;

    /** */
    private final ConcurrentMap<String, RaftGroupListener> listeners = new ConcurrentHashMap<>();

    /** */
    private final BlockingQueue<CommandClosureEx<ReadCommand>> readQueue;

    /** */
    private final BlockingQueue<CommandClosureEx<WriteCommand>> writeQueue;

    /** */
    private final Thread readWorker;

    /** */
    private final Thread writeWorker;

    /**
     * @param service Network service.
     * @param clientMsgFactory Client message factory.
     */
    public RaftServerImpl(ClusterService service, RaftClientMessagesFactory clientMsgFactory) {
        Objects.requireNonNull(service);
        Objects.requireNonNull(clientMsgFactory);

        this.service = service;
        this.clientMsgFactory = clientMsgFactory;

        readQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
        writeQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);

        service.messagingService().addMessageHandler((message, senderAddr, correlationId) -> {
            if (message instanceof GetLeaderRequest) {
                GetLeaderResponse resp = clientMsgFactory.getLeaderResponse().leader(new Peer(service.topologyService().localMember().address())).build();

                service.messagingService().send(senderAddr, resp, correlationId);
            }
            else if (message instanceof ActionRequest) {
                ActionRequest req0 = (ActionRequest)message;

                RaftGroupListener lsnr = listeners.get(req0.groupId());

                if (lsnr == null) {
                    sendError(senderAddr, correlationId, RaftErrorCode.ILLEGAL_STATE);

                    return;
                }

                if (req0.command() instanceof ReadCommand)
                    handleActionRequest(senderAddr, req0, correlationId, readQueue, lsnr);
                else
                    handleActionRequest(senderAddr, req0, correlationId, writeQueue, lsnr);
            }
            // TODO https://issues.apache.org/jira/browse/IGNITE-14775
        });

        readWorker = new Thread(() -> processQueue(readQueue, RaftGroupListener::onRead), "read-cmd-worker#" + service.topologyService().localMember().toString());
        readWorker.setDaemon(true);
        readWorker.start();

        writeWorker = new Thread(() -> processQueue(writeQueue, RaftGroupListener::onWrite), "write-cmd-worker#" + service.topologyService().localMember().toString());
        writeWorker.setDaemon(true);
        writeWorker.start();

        LOG.info("Started replication server [node=" + service + ']');
    }

    /** {@inheritDoc} */
    @Override public ClusterService clusterService() {
        return service;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean startRaftGroup(String groupId, RaftGroupListener lsnr,
        List<Peer> initialConf) {
        if (listeners.containsKey(groupId))
            return false;

        listeners.put(groupId, lsnr);

        return true;
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean stopRaftGroup(String groupId) {
        return listeners.remove(groupId) != null;
    }

    /** {@inheritDoc} */
    @Override public @Nullable Peer localPeer(String groupId) {
        return new Peer(service.topologyService().localMember().address());
    }

    /** {@inheritDoc} */
    @Override public synchronized void shutdown() throws Exception {
        readWorker.interrupt();
        readWorker.join();

        writeWorker.interrupt();
        writeWorker.join();

        LOG.info("Stopped replication server [node=" + service.toString() + ']');
    }

    /**
     * @param sender The sender.
     * @param req The request.
     * @param corellationId Corellation id.
     * @param queue The queue.
     * @param lsnr The listener.
     * @param <T> Command type.
     */
    private <T extends Command> void handleActionRequest(
        String sender,
        ActionRequest req,
        String corellationId,
        BlockingQueue<CommandClosureEx<T>> queue,
        RaftGroupListener lsnr
    ) {
        if (!queue.offer(new CommandClosureEx<>() {
            @Override public RaftGroupListener listener() {
                return lsnr;
            }

            @Override public T command() {
                return (T) req.command();
            }

            @Override public void result(Serializable res) {
                var msg = clientMsgFactory.actionResponse().result(res).build();
                service.messagingService().send(sender, msg, corellationId);
            }
        })) {
            // Queue out of capacity.
            sendError(sender, corellationId, RaftErrorCode.BUSY);
        }
    }

    /**
     * @param queue The queue.
     * @param clo The closure.
     * @param <T> Command type.
     */
    private <T extends Command> void processQueue(
        BlockingQueue<CommandClosureEx<T>> queue,
        BiConsumer<RaftGroupListener, Iterator<CommandClosure<T>>> clo
    ) {
        while (!Thread.interrupted()) {
            try {
                CommandClosureEx<T> cmdClo = queue.take();

                RaftGroupListener lsnr = cmdClo.listener();

                clo.accept(lsnr, List.<CommandClosure<T>>of(cmdClo).iterator());
            }
            catch (InterruptedException e0) {
                return;
            }
            catch (Exception e) {
                LOG.error("Failed to process the command", e);
            }
        }
    }

    private void sendError(String sender, String corellationId, RaftErrorCode errorCode) {
        RaftErrorResponse resp = clientMsgFactory.raftErrorResponse().errorCode(errorCode).build();

        service.messagingService().send(sender, resp, corellationId);
    }

    /**
     *
     */
    private interface CommandClosureEx<T extends Command> extends CommandClosure<T> {
        /**
         * @return The listener.
         */
        RaftGroupListener listener();
    }
}
