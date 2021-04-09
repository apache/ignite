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
import org.apache.ignite.network.Network;
import org.apache.ignite.network.NetworkCluster;
import org.apache.ignite.network.NetworkHandlersProvider;
import org.apache.ignite.network.NetworkMember;
import org.apache.ignite.network.NetworkMessageHandler;
import org.apache.ignite.network.message.NetworkMessage;
import org.apache.ignite.network.scalecube.ScaleCubeMemberResolver;
import org.apache.ignite.network.scalecube.ScaleCubeNetworkClusterFactory;
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
    private final NetworkCluster server;

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

        readQueue = new ArrayBlockingQueue<CommandClosureEx<ReadCommand>>(queueSize);
        writeQueue = new ArrayBlockingQueue<CommandClosureEx<WriteCommand>>(queueSize);

        Network network = new Network(
            new ScaleCubeNetworkClusterFactory(id, localPort, List.of(), new ScaleCubeMemberResolver())
        );

        // TODO: IGNITE-14088: Uncomment and use real serializer provider
//        network.registerMessageMapper((short)1000, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1001, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1005, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1006, new DefaultMessageMapperProvider());
//        network.registerMessageMapper((short)1009, new DefaultMessageMapperProvider());

        server = network.start();

        server.addHandlersProvider(new NetworkHandlersProvider() {
            @Override public NetworkMessageHandler messageHandler() {
                return new NetworkMessageHandler() {
                    @Override public void onReceived(NetworkMessage req, NetworkMember sender, String corellationId) {

                        if (req instanceof GetLeaderRequest) {
                            GetLeaderResponse resp = clientMsgFactory.getLeaderResponse().leader(new Peer(server.localMember())).build();

                            server.send(sender, resp, corellationId);
                        }
                        else if (req instanceof ActionRequest) {
                            ActionRequest req0 = (ActionRequest) req;

                            RaftGroupCommandListener lsnr = listeners.get(req0.groupId());

                            if (lsnr == null) {
                                sendError(sender, corellationId, RaftErrorCode.ILLEGAL_STATE);

                                return;
                            }

                            if (req0.command() instanceof ReadCommand) {
                                handleActionRequest(sender, req0, corellationId, readQueue, lsnr);
                            }
                            else {
                                handleActionRequest(sender, req0, corellationId, writeQueue, lsnr);
                            }
                        }
                        else {
                            LOG.warn("Unsupported message class " + req.getClass().getName());
                        }
                    }
                };
            }
        });

        readWorker = new Thread(() -> processQueue(readQueue, (l, i) -> l.onRead(i)), "read-cmd-worker#" + id);
        readWorker.setDaemon(true);
        readWorker.start();

        writeWorker = new Thread(() -> processQueue(writeQueue, (l, i) -> l.onWrite(i)), "write-cmd-worker#" + id);
        writeWorker.setDaemon(true);
        writeWorker.start();

        LOG.info("Started replication server [id=" + id + ", localPort=" + localPort + ']');
    }

    /** {@inheritDoc} */
    @Override public NetworkMember localMember() {
        return server.localMember();
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
        NetworkMember sender,
        ActionRequest req,
        String corellationId,
        BlockingQueue<CommandClosureEx<T>> queue,
        RaftGroupCommandListener lsnr
    ) {
        if (!queue.offer(new CommandClosureEx<T>() {
            @Override public RaftGroupCommandListener listener() {
                return lsnr;
            }

            @Override public T command() {
                return (T) req.command();
            }

            @Override public void success(Object res) {
                server.send(sender, clientMsgFactory.actionResponse().result(res).build(), corellationId);
            }

            @Override public void failure(Throwable t) {
                sendError(sender, corellationId, RaftErrorCode.ILLEGAL_STATE);
            }
        })) {
            // Queue out of capacity.
            sendError(sender, corellationId, RaftErrorCode.BUSY);

            return;
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

    private void sendError(NetworkMember sender, String corellationId, RaftErrorCode errorCode) {
        RaftErrorResponse resp = clientMsgFactory.raftErrorResponse().errorCode(errorCode).build();

        server.send(sender, resp, corellationId);
    }

    private interface CommandClosureEx<T extends Command> extends CommandClosure<T> {
        RaftGroupCommandListener listener();
    }
}
