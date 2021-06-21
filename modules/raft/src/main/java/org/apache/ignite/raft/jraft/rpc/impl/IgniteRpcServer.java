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
package org.apache.ignite.raft.jraft.rpc.impl;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkMessage;
import org.apache.ignite.network.TopologyEventHandler;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.rpc.RpcContext;
import org.apache.ignite.raft.jraft.rpc.RpcProcessor;
import org.apache.ignite.raft.jraft.rpc.RpcServer;
import org.apache.ignite.raft.jraft.rpc.impl.cli.AddLearnersRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.AddPeerRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.ChangePeersRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.GetLeaderRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.GetPeersRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.RemoveLearnersRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.RemovePeerRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.ResetLearnersRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.ResetPeerRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.SnapshotRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.cli.TransferLeaderRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.client.ActionRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.AppendEntriesRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.GetFileRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.InstallSnapshotRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.ReadIndexRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.RequestVoteRequestProcessor;
import org.apache.ignite.raft.jraft.rpc.impl.core.TimeoutNowRequestProcessor;
import org.jetbrains.annotations.Nullable;

/**
 * TODO https://issues.apache.org/jira/browse/IGNITE-14519 Unsubscribe on shutdown
 */
public class IgniteRpcServer implements RpcServer<Void> {
    /** Factory. */
    private static final RaftClientMessagesFactory FACTORY = new RaftClientMessagesFactory();

    private final ClusterService service;

    private final List<ConnectionClosedEventListener> listeners = new CopyOnWriteArrayList<>();

    private final Map<String, RpcProcessor> processors = new ConcurrentHashMap<>();

    /**
     * @param service The cluster service.
     * @param nodeManager The node manager.
     * @param factory Message factory.
     * @param rpcExecutor The executor for RPC requests.
     */
    public IgniteRpcServer(
        ClusterService service,
        NodeManager nodeManager,
        RaftClientMessagesFactory factory,
        @Nullable Executor rpcExecutor
    ) {
        this.service = service;

        // raft server RPC
        AppendEntriesRequestProcessor appendEntriesRequestProcessor = new AppendEntriesRequestProcessor(rpcExecutor);
        registerConnectionClosedEventListener(appendEntriesRequestProcessor);
        registerProcessor(appendEntriesRequestProcessor);
        registerProcessor(new GetFileRequestProcessor(rpcExecutor));
        registerProcessor(new InstallSnapshotRequestProcessor(rpcExecutor));
        registerProcessor(new RequestVoteRequestProcessor(rpcExecutor));
        registerProcessor(new PingRequestProcessor(rpcExecutor)); // TODO asch this should go last.
        registerProcessor(new TimeoutNowRequestProcessor(rpcExecutor));
        registerProcessor(new ReadIndexRequestProcessor(rpcExecutor));
        // raft native cli service
        registerProcessor(new AddPeerRequestProcessor(rpcExecutor));
        registerProcessor(new RemovePeerRequestProcessor(rpcExecutor));
        registerProcessor(new ResetPeerRequestProcessor(rpcExecutor));
        registerProcessor(new ChangePeersRequestProcessor(rpcExecutor));
        registerProcessor(new GetLeaderRequestProcessor(rpcExecutor));
        registerProcessor(new SnapshotRequestProcessor(rpcExecutor));
        registerProcessor(new TransferLeaderRequestProcessor(rpcExecutor));
        registerProcessor(new GetPeersRequestProcessor(rpcExecutor));
        registerProcessor(new AddLearnersRequestProcessor(rpcExecutor));
        registerProcessor(new RemoveLearnersRequestProcessor(rpcExecutor));
        registerProcessor(new ResetLearnersRequestProcessor(rpcExecutor));
        // common client integration
        registerProcessor(new org.apache.ignite.raft.jraft.rpc.impl.client.GetLeaderRequestProcessor(rpcExecutor, FACTORY));
        registerProcessor(new ActionRequestProcessor(rpcExecutor, FACTORY));
        registerProcessor(new org.apache.ignite.raft.jraft.rpc.impl.client.SnapshotRequestProcessor(rpcExecutor, FACTORY));

        service.messagingService().addMessageHandler((msg, senderAddr, corellationId) -> {
            Class<? extends NetworkMessage> cls = msg.getClass();
            RpcProcessor<NetworkMessage> prc = processors.get(cls.getName());

            // TODO asch cache mapping https://issues.apache.org/jira/browse/IGNITE-14832
            if (prc == null) {
                for (Class<?> iface : cls.getInterfaces()) {
                    prc = processors.get(iface.getName());

                    if (prc != null)
                        break;
                }
            }

            if (prc == null)
                return;

            RpcProcessor.ExecutorSelector selector = prc.executorSelector();

            Executor executor = null;

            if (selector != null)
                executor = selector.select(prc.getClass().getName(), msg, nodeManager);

            if (executor == null)
                executor = prc.executor();

            if (executor == null)
                executor = rpcExecutor;

            RpcProcessor<NetworkMessage> finalPrc = prc;

            executor.execute(() -> {
                var context = new RpcContext() {
                    @Override public NodeManager getNodeManager() {
                        return nodeManager;
                    }

                    @Override public void sendResponse(Object responseObj) {
                        service.messagingService().send(senderAddr, (NetworkMessage) responseObj, corellationId);
                    }

                    @Override public String getRemoteAddress() {
                        return senderAddr;
                    }

                    @Override public String getLocalAddress() {
                        return service.topologyService().localMember().address();
                    }
                };

                finalPrc.handleRequest(context, msg);
            });
        });

        service.topologyService().addEventHandler(new TopologyEventHandler() {
            @Override public void onAppeared(ClusterNode member) {
                // TODO asch optimize start replicator https://issues.apache.org/jira/browse/IGNITE-14843
            }

            @Override public void onDisappeared(ClusterNode member) {
                for (ConnectionClosedEventListener listener : listeners)
                    listener.onClosed(service.topologyService().localMember().name(), member.name());
            }
        });
    }

    /** {@inheritDoc} */
    @Override public void registerConnectionClosedEventListener(ConnectionClosedEventListener listener) {
        if (!listeners.contains(listener))
            listeners.add(listener);
    }

    /** {@inheritDoc} */
    @Override public void registerProcessor(RpcProcessor<?> processor) {
        processors.put(processor.interest(), processor);
    }

    /** {@inheritDoc} */
    @Override public int boundPort() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean init(Void opts) {
        return true;
    }

    public ClusterService clusterService() {
        return service;
    }

    /** {@inheritDoc} */
    @Override public void shutdown() {
    }
}
