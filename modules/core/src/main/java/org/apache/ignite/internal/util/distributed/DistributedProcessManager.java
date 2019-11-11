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

package org.apache.ignite.internal.util.distributed;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * TODO Update javadocs
 * Provides logic for a distributed process:
 * <ul>
 *  <li>1. Initial discovery message starts process.</li>
 *  <li>2. Each server node process it and send the result via the communication message to the coordinator.
 *  See {@link DistributedProcess#execute}.</li>
 *  <li>3. The coordinator processes all single nodes results and sends the action message via discovery.</li>
 * </ul>
 * Several processes can be started at the same time. Processes are identified by request id.
 * Follow methods used to manage process:
 * {@link #onAllReceived}
 * {@link #onAllServersLeft}

 */
public class DistributedProcessManager {
    /** */
    private final ConcurrentHashMap<Integer, DistributedProcess> dps = new ConcurrentHashMap<>(1);

    /** Map of all active processes. */
    private final ConcurrentHashMap</*processId*/UUID, Process> processes = new ConcurrentHashMap<>(1);

    /** Synchronization mutex for coordinator initializing and the remaining collection operations. */
    private final Object mux = new Object();

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Logger. */
    private IgniteLogger log;

    /**
     * @param ctx Kernal context.
     */
    public DistributedProcessManager(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        ctx.discovery().setCustomEventListener(ProcessInitMessage.class, (topVer, snd, msg) -> {
            Process proc = processes.computeIfAbsent(msg.requestId(), id -> new Process(msg.requestId()));

            if (proc.initFut.isDone())
                return;

            proc.typeId = msg.processTypeId();

            ClusterNode crd = coordinator();

            if (crd == null) {
                proc.initFut.onDone();

                onAllServersLeft(msg.requestId());

                return;
            }

            proc.crdId = crd.id();

            if (crd.isLocal())
                initCoordinator(topVer, proc);

            DistributedProcess dp = dps.get(proc.typeId);

            IgniteInternalFuture<Serializable> fut = dp.execute(msg.request());

            fut.listen(f -> {
                try {
                    Serializable res = f.get();

                    proc.singleResFut.onDone(res);

                    if (res == null)
                        return;

                    ClusterNode crdNode = coordinator();

                    SingleNodeMessage singleMsg = new SingleNodeMessage(proc.id, res);

                    if (crdNode != null) {
                        if (crdNode.isLocal())
                            onSingleNodeMessageReceived(singleMsg, crdNode.id());
                        else
                            ctx.io().sendToGridTopic(crdNode, GridTopic.TOPIC_DISTRIBUTED_PROCESS, singleMsg, SYSTEM_POOL);
                    }
                    else
                        onAllServersLeft(msg.requestId());
                }
                catch (IgniteCheckedException e) {
                    log.warning("Unable to send result.", e);
                }
            });

            proc.initFut.onDone();
        });

        ctx.discovery().setCustomEventListener(ProcessFinishMessage.class, (topVer, snd, msg) -> {
            Process proc = processes.computeIfAbsent(msg.requestId(), id -> new Process(msg.requestId()));

            DistributedProcess dp = dps.get(proc.typeId);

            dp.onResult(msg.result());

            processes.remove(msg.requestId());
        });

        ctx.io().addMessageListener(GridTopic.TOPIC_DISTRIBUTED_PROCESS, (nodeId, msg0, plc) -> {
            if (msg0 instanceof SingleNodeMessage) {
                SingleNodeMessage msg = (SingleNodeMessage)msg0;

                onSingleNodeMessageReceived(msg, nodeId);
            }
        });

        ctx.event().addDiscoveryEventListener((evt, discoCache) -> {
            UUID leftNodeId = evt.eventNode().id();

            for (Process proc : processes.values()) {
                proc.initFut.listen(fut -> {
                    boolean crdChanged = org.apache.ignite.internal.util.typedef.F.eq(leftNodeId, proc.crdId);

                    if (crdChanged) {
                        ClusterNode crd = coordinator();

                        if (crd != null) {
                            proc.crdId = crd.id();

                            if (crd.isLocal())
                                initCoordinator(discoCache.version(), proc);

                            proc.singleResFut.listen(f -> {
                                try {
                                    Serializable res = f.get();

                                    if (res == null)
                                        return;

                                    SingleNodeMessage singleMsg = new SingleNodeMessage(proc.id, res);

                                    if (crd.isLocal())
                                        onSingleNodeMessageReceived(singleMsg, crd.id());
                                    else
                                        ctx.io().sendToGridTopic(crd, GridTopic.TOPIC_DISTRIBUTED_PROCESS, singleMsg, SYSTEM_POOL);
                                }
                                catch (IgniteCheckedException e) {
                                    log.warning("Unable to send result.", e);
                                }
                            });
                        }
                        else
                            onAllServersLeft(proc.id);
                    }
                    else if (ctx.localNodeId().equals(proc.crdId)) {
                        boolean rmvd, isEmpty;

                        synchronized (mux) {
                            rmvd = proc.remaining.remove(leftNodeId);

                            isEmpty = proc.remaining.isEmpty();
                        }

                        if (rmvd) {
                            proc.results.remove(leftNodeId);

                            if (isEmpty)
                                onAllReceived(proc);
                        }
                    }
                });
            }
        }, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /** */
    public void register(DistributedProcesses proc, DistributedProcess dp) {
        dps.put(proc.processTypeId(), dp);
    }

    /** */
    public void start(DistributedProcesses proc, Serializable req) {
        try {
            ProcessInitMessage msg = new ProcessInitMessage(UUID.randomUUID(), proc.processTypeId(), req);

            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.warning("Unable to start process.", e);
        }
    }

    /** */
    protected void onAllReceived(Process proc) {
        DistributedProcess dp = dps.get(proc.typeId);

        Serializable res = dp.buildResult(proc.results);

        try {
            ProcessFinishMessage msg = new ProcessFinishMessage(proc.id, res);

            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.warning("Unable to send action message.", e);
        }
    }

    /**
     * Handles case when all server nodes have left the grid.
     *
     * @param id Request id.
     */
    protected void onAllServersLeft(UUID id) {
        // No-op.
    }

    /**
     * Processes the received single node message.
     *
     * @param msg Message.
     * @param nodeId Node id.
     */
    private void onSingleNodeMessageReceived(SingleNodeMessage msg, UUID nodeId) {
        Process proc = processes.computeIfAbsent(msg.requestId(), id -> new Process(msg.requestId()));

        proc.initCrdFut.listen(f -> {
            boolean rmvd, isEmpty;

            synchronized (mux) {
                rmvd = proc.remaining.remove(nodeId);

                isEmpty = proc.remaining.isEmpty();
            }

            if (rmvd) {
                proc.results.put(nodeId, msg.response());

                if (isEmpty)
                    onAllReceived(proc);
            }
        });
    }

    /**
     * Initiates process coordinator.
     *
     * @param topVer Topology version.
     * @param proc Process.
     */
    private void initCoordinator(AffinityTopologyVersion topVer, Process proc) {
        Set<UUID> aliveSrvNodesIds = new HashSet<>();

        for (ClusterNode node : ctx.discovery().serverNodes(topVer)) {
            if (ctx.discovery().alive(node))
                aliveSrvNodesIds.add(node.id());
        }

        synchronized (mux) {
            if (proc.initCrdFut.isDone())
                return;

            assert proc.remaining.isEmpty();

            proc.remaining.addAll(aliveSrvNodesIds);

            proc.initCrdFut.onDone();
        }
    }

    /** @return Cluster coordinator, {@code null} if failed to determine. */
    private @Nullable ClusterNode coordinator() {
        return U.oldest(ctx.discovery().aliveServerNodes(), null);
    }

    /** */
    private class Process {
        /** Process id. */
        private final UUID id;

        /** Init coordinator future. */
        private final GridFutureAdapter<Void> initCrdFut = new GridFutureAdapter<>();

        /** Coordinator id. */
        private volatile UUID crdId;

        /** Process name. */
        private volatile int typeId = -1;

        /** Init process future. */
        private final GridFutureAdapter<Void> initFut = new GridFutureAdapter<>();

        /** Future of single local node result. */
        private final GridFutureAdapter<Serializable> singleResFut = new GridFutureAdapter<>();

        /** Remaining nodes to received single result message. */
        private final Set</*nodeId*/UUID> remaining = new GridConcurrentHashSet<>();

        /** Single nodes results. */
        private final ConcurrentHashMap</*nodeId*/UUID, Serializable> results = new ConcurrentHashMap<>();

        /** @param id Process id. */
        private Process(UUID id) {
            this.id = id;
        }
    }
}
