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
import org.apache.ignite.lang.LoggerMessageHelper;
import org.apache.ignite.lang.NodeStoppingException;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.jraft.RaftMessageGroup;
import org.apache.ignite.raft.jraft.RaftMessagesFactory;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.rpc.ActionRequest;
import org.apache.ignite.raft.jraft.rpc.CliRequests;
import org.apache.ignite.raft.jraft.rpc.RpcRequests;
import org.jetbrains.annotations.Nullable;

/**
 * A single node service implementation.
 * Only for test purposes.
 */
public class RaftServerImpl implements RaftServer {
    /** */
    private static final int QUEUE_SIZE = 1000;

    /** The logger. */
    private static final IgniteLogger LOG = IgniteLogger.forClass(RaftServerImpl.class);

    /** */
    private final RaftMessagesFactory clientMsgFactory;

    /** */
    private final ClusterService service;

    /** */
    private final ConcurrentMap<String, RaftGroupListener> listeners = new ConcurrentHashMap<>();

    /** */
    private final BlockingQueue<CommandClosureEx<ReadCommand>> readQueue;

    /** */
    private final BlockingQueue<CommandClosureEx<WriteCommand>> writeQueue;

    /** */
    private volatile Thread readWorker;

    /** */
    private volatile Thread writeWorker;

    /**
     * @param service Network service.
     * @param clientMsgFactory Client message factory.
     */
    public RaftServerImpl(ClusterService service, RaftMessagesFactory clientMsgFactory) {
        Objects.requireNonNull(service);
        Objects.requireNonNull(clientMsgFactory);

        this.service = service;
        this.clientMsgFactory = clientMsgFactory;

        readQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
        writeQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);
    }

    /** {@inheritDoc} */
    @Override public void start() {
        service.messagingService().addMessageHandler(
            RaftMessageGroup.class,
            (message, senderAddr, correlationId) -> {
                if (message instanceof CliRequests.GetLeaderRequest) {
                    var localPeer = new Peer(service.topologyService().localMember().address());

                    CliRequests.GetLeaderResponse resp = clientMsgFactory.getLeaderResponse().leaderId(PeerId.fromPeer(localPeer).toString()).build();

                    service.messagingService().send(senderAddr, resp, correlationId);
                }
                else if (message instanceof ActionRequest) {
                    ActionRequest req0 = (ActionRequest)message;

                    RaftGroupListener lsnr = listeners.get(req0.groupId());

                    if (lsnr == null) {
                        sendError(senderAddr, correlationId, RaftError.UNKNOWN);

                        return;
                    }

                    if (req0.command() instanceof ReadCommand)
                        handleActionRequest(senderAddr, req0, correlationId, readQueue, lsnr);
                    else
                        handleActionRequest(senderAddr, req0, correlationId, writeQueue, lsnr);
                }
                // TODO https://issues.apache.org/jira/browse/IGNITE-14775
            }
        );

        readWorker = new Thread(() -> processQueue(readQueue, RaftGroupListener::onRead), "read-cmd-worker#" + service.topologyService().localMember().toString());
        readWorker.setDaemon(true);
        readWorker.start();

        writeWorker = new Thread(() -> processQueue(writeQueue, RaftGroupListener::onWrite), "write-cmd-worker#" + service.topologyService().localMember().toString());
        writeWorker.setDaemon(true);
        writeWorker.start();

        LOG.info("Started replication server [node={}]", service);
    }

    /** {@inheritDoc} */
    @Override public void stop() throws NodeStoppingException {
        assert listeners.isEmpty() : LoggerMessageHelper.format("Raft groups are still running {}", listeners.keySet());

        if (readWorker != null) {
            readWorker.interrupt();
            try {
                readWorker.join();
            }
            catch (InterruptedException e) {
                throw new NodeStoppingException("Unable to stop read worker.", e);
            }
        }

        if (writeWorker != null) {
            writeWorker.interrupt();
            try {
                writeWorker.join();
            }
            catch (InterruptedException e) {
                throw new NodeStoppingException("Unable to stop write worker.", e);
            }
        }

        LOG.info("Stopped replication server [node={}]", service);
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

    /**
     * @param sender The sender.
     * @param req The request.
     * @param corellationId Corellation id.
     * @param queue The queue.
     * @param lsnr The listener.
     * @param <T> Command type.
     */
    private <T extends Command> void handleActionRequest(
        NetworkAddress sender,
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
            sendError(sender, corellationId, RaftError.EBUSY);
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

    private void sendError(NetworkAddress sender, String corellationId, RaftError error) {
        RpcRequests.ErrorResponse resp = clientMsgFactory.errorResponse().errorCode(error.getNumber()).build();

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
