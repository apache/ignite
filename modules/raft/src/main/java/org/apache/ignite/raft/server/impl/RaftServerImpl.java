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

package org.apache.ignite.raft.server.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;
import org.apache.ignite.lang.IgniteLogger;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.ClusterLocalConfiguration;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.message.MessageSerializationRegistry;
import org.apache.ignite.network.scalecube.ScaleCubeClusterServiceFactory;
import org.apache.ignite.raft.client.Command;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.RaftErrorCode;
import org.apache.ignite.raft.client.ReadCommand;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.exception.RaftException;
import org.apache.ignite.raft.client.message.ActionRequest;
import org.apache.ignite.raft.client.message.GetLeaderRequest;
import org.apache.ignite.raft.client.message.GetLeaderResponse;
import org.apache.ignite.raft.client.message.RaftClientMessageFactory;
import org.apache.ignite.raft.client.message.RaftErrorResponse;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupCommandListener;
import org.apache.ignite.raft.server.RaftServer;
import org.jetbrains.annotations.NotNull;

/**
 * A single node server implementation.
 */
public class RaftServerImpl implements RaftServer {
    /** */
    private static final IgniteLogger LOG = IgniteLogger.forClass(RaftServerImpl.class);

    /** */
    private final String id;

    /** */
    private final int localPort;

    /** */
    private final RaftClientMessageFactory clientMsgFactory;

    /** */
    private final ClusterService server;

    /** */
    private final ConcurrentMap<String, RaftGroupCommandListener> listeners = new ConcurrentHashMap<>();

    /** */
    private final BlockingQueue<CommandClosureEx<ReadCommand>> readQueue;

    /** */
    private final BlockingQueue<CommandClosureEx<WriteCommand>> writeQueue;

    /** */
    private final Thread readWorker;

    /** */
    private final Thread writeWorker;

    /**
     * @param id Server id.
     * @param localPort Local port.
     * @param clientMsgFactory Client message factory.
     * @param queueSize Queue size.
     * @param listeners Command listeners.
     */
    public RaftServerImpl(
        @NotNull String id,
        int localPort,
        @NotNull RaftClientMessageFactory clientMsgFactory,
        int queueSize,
        Map<String, RaftGroupCommandListener> listeners
    ) {
        Objects.requireNonNull(id);
        Objects.requireNonNull(clientMsgFactory);

        this.id = id;
        this.localPort = localPort;
        this.clientMsgFactory = clientMsgFactory;

        if (listeners != null)
            this.listeners.putAll(listeners);

        readQueue = new ArrayBlockingQueue<>(queueSize);
        writeQueue = new ArrayBlockingQueue<>(queueSize);

        // TODO: IGNITE-14088: Uncomment and use real serializer factory
        var serializationRegistry = new MessageSerializationRegistry();
//            .registerFactory((short)1000, ???)
//            .registerFactory((short)1001, ???)
//            .registerFactory((short)1005, ???)
//            .registerFactory((short)1006, ???)
//            .registerFactory((short)1009, ???);

        var context = new ClusterLocalConfiguration(id, localPort, List.of(), serializationRegistry);
        var factory = new ScaleCubeClusterServiceFactory();

        server = factory.createClusterService(context);

        server.messagingService().addMessageHandler((message, sender, correlationId) -> {
            if (message instanceof GetLeaderRequest) {
                GetLeaderResponse resp = clientMsgFactory.getLeaderResponse().leader(new Peer(server.topologyService().localMember())).build();

                server.messagingService().send(sender, resp, correlationId);
            }
            else if (message instanceof ActionRequest) {
                ActionRequest<?> req0 = (ActionRequest<?>) message;

                RaftGroupCommandListener lsnr = listeners.get(req0.groupId());

                if (lsnr == null) {
                    sendError(sender, correlationId, RaftErrorCode.ILLEGAL_STATE);

                    return;
                }

                if (req0.command() instanceof ReadCommand) {
                    handleActionRequest(sender, req0, correlationId, readQueue, lsnr);
                }
                else {
                    handleActionRequest(sender, req0, correlationId, writeQueue, lsnr);
                }
            }
            else {
                LOG.warn("Unsupported message class " + message.getClass().getName());
            }
        });

        server.start();

        readWorker = new Thread(() -> processQueue(readQueue, RaftGroupCommandListener::onRead), "read-cmd-worker#" + id);
        readWorker.setDaemon(true);
        readWorker.start();

        writeWorker = new Thread(() -> processQueue(writeQueue, RaftGroupCommandListener::onWrite), "write-cmd-worker#" + id);
        writeWorker.setDaemon(true);
        writeWorker.start();

        LOG.info("Started replication server [id=" + id + ", localPort=" + localPort + ']');
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localMember() {
        return server.topologyService().localMember();
    }

    /** {@inheritDoc} */
    @Override public void setListener(String groupId, RaftGroupCommandListener lsnr) {
        listeners.put(groupId, lsnr);
    }

    /** {@inheritDoc} */
    @Override public void clearListener(String groupId) {
        listeners.remove(groupId);
    }

    /** {@inheritDoc} */
    @Override public synchronized void shutdown() throws Exception {
        server.shutdown();

        readWorker.interrupt();
        readWorker.join();

        writeWorker.interrupt();
        writeWorker.join();

        LOG.info("Stopped replication server [id=" + id + ", localPort=" + localPort + ']');
    }

    private <T extends Command> void handleActionRequest(
        ClusterNode sender,
        ActionRequest<?> req,
        String corellationId,
        BlockingQueue<CommandClosureEx<T>> queue,
        RaftGroupCommandListener lsnr
    ) {
        if (!queue.offer(new CommandClosureEx<>() {
            @Override public RaftGroupCommandListener listener() {
                return lsnr;
            }

            @Override public T command() {
                return (T)req.command();
            }

            @Override public void success(Object res) {
                var msg = clientMsgFactory.actionResponse().result(res).build();
                server.messagingService().send(sender, msg, corellationId);
            }

            @Override public void failure(Throwable t) {
                sendError(sender, corellationId, RaftErrorCode.ILLEGAL_STATE);
            }
        })) {
            // Queue out of capacity.
            sendError(sender, corellationId, RaftErrorCode.BUSY);
        }
    }

    private <T extends Command> void processQueue(
        BlockingQueue<CommandClosureEx<T>> queue,
        BiConsumer<RaftGroupCommandListener, Iterator<CommandClosure<T>>> clo
    ) {
        while (!Thread.interrupted()) {
            try {
                CommandClosureEx<T> cmdClo = queue.take();

                RaftGroupCommandListener lsnr = cmdClo.listener();

                if (lsnr == null)
                    cmdClo.failure(new RaftException(RaftErrorCode.ILLEGAL_STATE));
                else
                    clo.accept(lsnr, List.<CommandClosure<T>>of(cmdClo).iterator());
            }
            catch (InterruptedException e) {
                return;
            }
        }
    }

    private void sendError(ClusterNode sender, String corellationId, RaftErrorCode errorCode) {
        RaftErrorResponse resp = clientMsgFactory.raftErrorResponse().errorCode(errorCode).build();

        server.messagingService().send(sender, resp, corellationId);
    }

    private interface CommandClosureEx<T extends Command> extends CommandClosure<T> {
        RaftGroupCommandListener listener();
    }
}
