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

import java.io.File;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;
import org.apache.ignite.internal.raft.server.RaftServer;
import org.apache.ignite.network.ClusterNode;
import org.apache.ignite.network.ClusterService;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.ElectionPriority;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.client.WriteCommand;
import org.apache.ignite.raft.client.message.RaftClientMessagesFactory;
import org.apache.ignite.raft.client.service.CommandClosure;
import org.apache.ignite.raft.client.service.RaftGroupListener;
import org.apache.ignite.raft.jraft.Closure;
import org.apache.ignite.raft.jraft.Iterator;
import org.apache.ignite.raft.jraft.JRaftUtils;
import org.apache.ignite.raft.jraft.NodeManager;
import org.apache.ignite.raft.jraft.RaftGroupService;
import org.apache.ignite.raft.jraft.Status;
import org.apache.ignite.raft.jraft.conf.Configuration;
import org.apache.ignite.raft.jraft.core.StateMachineAdapter;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.apache.ignite.raft.jraft.error.RaftError;
import org.apache.ignite.raft.jraft.option.NodeOptions;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcClient;
import org.apache.ignite.raft.jraft.rpc.impl.IgniteRpcServer;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotReader;
import org.apache.ignite.raft.jraft.storage.snapshot.SnapshotWriter;
import org.apache.ignite.raft.jraft.util.JDKMarshaller;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.raft.jraft.JRaftUtils.addressFromEndpoint;

/**
 * Raft server implementation on top of forked JRaft library.
 */
public class JRaftServerImpl implements RaftServer {
    /** Cluster service. */
    private final ClusterService service;

    /** Data path. */
    private final String dataPath;

    /** Server instance. */
    private IgniteRpcServer rpcServer;

    /** Started groups. */
    private ConcurrentMap<String, RaftGroupService> groups = new ConcurrentHashMap<>();

    /** Node manager. */
    private final NodeManager nodeManager;

    /** Options. */
    private final NodeOptions opts;

    /**
     * @param service Cluster service.
     * @param dataPath Data path.
     * @param factory The factory.
     */
    public JRaftServerImpl(ClusterService service, String dataPath, RaftClientMessagesFactory factory) {
        this(service, dataPath, factory, new NodeOptions());
    }

    /**
     * @param service Cluster service.
     * @param dataPath Data path.
     * @param factory The factory.
     * @param opts Default node options.
     */
    public JRaftServerImpl(
        ClusterService service,
        String dataPath,
        RaftClientMessagesFactory factory,
        NodeOptions opts
    ) {
        this.service = service;
        this.dataPath = dataPath;
        this.nodeManager = new NodeManager();
        this.opts = opts;

        assert service.topologyService().localMember() != null;

        if (opts.getServerName() == null)
            opts.setServerName(service.localConfiguration().getName());

        if (opts.getCommonExecutor() == null)
            opts.setCommonExecutor(JRaftUtils.createCommonExecutor(opts));

        if (opts.getStripedExecutor() == null)
            opts.setStripedExecutor(JRaftUtils.createAppendEntriesExecutor(opts));

        if (opts.getScheduler() == null)
            opts.setScheduler(JRaftUtils.createScheduler(opts));

        if (opts.getClientExecutor() == null)
            opts.setClientExecutor(JRaftUtils.createClientExecutor(opts, opts.getServerName()));

        rpcServer = new IgniteRpcServer(service, nodeManager, factory, JRaftUtils.createRequestExecutor(opts));

        rpcServer.init(null);
    }

    /** {@inheritDoc} */
    @Override public ClusterService clusterService() {
        return service;
    }

    /**
     * @param groupId Group id.
     * @return The path to persistence folder.
     */
    public String getServerDataPath(String groupId) {
        ClusterNode clusterNode = service.topologyService().localMember();

        return this.dataPath + File.separator + groupId + "_" + clusterNode.address().toString().replace(':', '_');
    }

    /** {@inheritDoc} */
    @Override public synchronized boolean startRaftGroup(String groupId, RaftGroupListener lsnr,
        @Nullable List<Peer> initialConf) {
        if (groups.containsKey(groupId))
            return false;

        // Thread pools are shared by all raft groups.
        final NodeOptions nodeOptions = opts.copy();

        final String serverDataPath = getServerDataPath(groupId);
        new File(serverDataPath).mkdirs();

        nodeOptions.setLogUri(serverDataPath + File.separator + "logs");
        nodeOptions.setRaftMetaUri(serverDataPath + File.separator + "meta");
        nodeOptions.setSnapshotUri(serverDataPath + File.separator + "snapshot");

        nodeOptions.setFsm(new DelegatingStateMachine(lsnr));

        if (initialConf != null) {
            List<PeerId> mapped = initialConf.stream().map(PeerId::fromPeer).collect(Collectors.toList());

            nodeOptions.setInitialConf(new Configuration(mapped, null));
        }

        IgniteRpcClient client = new IgniteRpcClient(service);

        nodeOptions.setRpcClient(client);

        NetworkAddress addr = service.topologyService().localMember().address();

        var peerId = new PeerId(addr.host(), addr.port(), 0, ElectionPriority.DISABLED);

        var server = new RaftGroupService(groupId, peerId, nodeOptions, rpcServer, nodeManager);

        server.start();

        groups.put(groupId, server);

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean stopRaftGroup(String groupId) {
        RaftGroupService svc = groups.remove(groupId);

        boolean stopped = svc != null;

        if (stopped)
            svc.shutdown();

        return stopped;
    }

    /** {@inheritDoc} */
    @Override public Peer localPeer(String groupId) {
        RaftGroupService service = groups.get(groupId);

        if (service == null)
            return null;

        PeerId peerId = service.getRaftNode().getNodeId().getPeerId();

        return new Peer(addressFromEndpoint(peerId.getEndpoint()), peerId.getPriority());
    }

    /**
     * @param groupId Group id.
     * @return Service group.
     */
    public RaftGroupService raftGroupService(String groupId) {
        return groups.get(groupId);
    }

    /** {@inheritDoc} */
    @Override public void shutdown() throws Exception {
        for (RaftGroupService groupService : groups.values())
            groupService.shutdown();

        rpcServer.shutdown();
    }

    /**
     *
     */
    public static class DelegatingStateMachine extends StateMachineAdapter {
        private final RaftGroupListener listener;

        /**
         * @param listener The listener.
         */
        DelegatingStateMachine(RaftGroupListener listener) {
            this.listener = listener;
        }

        public RaftGroupListener getListener() {
            return listener;
        }

        /** {@inheritDoc} */
        @Override public void onApply(Iterator iter) {
            try {
                listener.onWrite(new java.util.Iterator<>() {
                    @Override public boolean hasNext() {
                        return iter.hasNext();
                    }

                    @Override public CommandClosure<WriteCommand> next() {
                        @Nullable CommandClosure<WriteCommand> done = (CommandClosure<WriteCommand>)iter.done();
                        ByteBuffer data = iter.getData();

                        return new CommandClosure<>() {
                            @Override public WriteCommand command() {
                                return JDKMarshaller.DEFAULT.unmarshall(data.array());
                            }

                            @Override public void result(Serializable res) {
                                if (done != null)
                                    done.result(res);

                                iter.next();
                            }
                        };
                    }
                });
            }
            catch (Exception err) {
                Status st = new Status(RaftError.ESTATEMACHINE, err.getMessage());

                if (iter.done() != null)
                    iter.done().run(st);

                iter.setErrorAndRollback(1, st);
            }
        }

        /** {@inheritDoc} */
        @Override public void onSnapshotSave(SnapshotWriter writer, Closure done) {
            try {
                listener.onSnapshotSave(writer.getPath(), res -> {
                    if (res == null) {
                        File file = new File(writer.getPath());

                        for (File file0 : file.listFiles()) {
                            if (file0.isFile())
                                writer.addFile(file0.getName(), null);
                        }

                        done.run(Status.OK());
                    }
                    else {
                        done.run(new Status(RaftError.EIO, "Fail to save snapshot to %s, reason %s",
                            writer.getPath(), res.getMessage()));
                    }
                });
            }
            catch (Exception e) {
                done.run(new Status(RaftError.EIO, "Fail to save snapshot %s", e.getMessage()));
            }
        }

        /** {@inheritDoc} */
        @Override public boolean onSnapshotLoad(SnapshotReader reader) {
            return listener.onSnapshotLoad(reader.getPath());
        }
    }
}
