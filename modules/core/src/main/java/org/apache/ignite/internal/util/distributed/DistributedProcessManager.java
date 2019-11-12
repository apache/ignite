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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Optional;
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
 * Distributed processes manager.
 * <p>
 * Each distributed process should be registered via {@link #register} before discovery manager started.
 * The method {@link #start} starts process.
 * <p>
 * Several processes of one type can be started at the same time.
 *
 * @see DistributedProcess
 * @see DistributedProcesses
 */
@SuppressWarnings("unchecked")
public class DistributedProcessManager {
    /** Map of registered processes. */
    private final ConcurrentHashMap<Integer, DistributedProcessFactory> registered = new ConcurrentHashMap<>(1);

    /** Map of all active processes. */
    private final ConcurrentHashMap</*processId*/UUID, Process> processes = new ConcurrentHashMap<>(1);

    /** Synchronization mutex for coordinator initializing and the remaining collection operations. */
    private final Object mux = new Object();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /** @param ctx Kernal context. */
    public DistributedProcessManager(GridKernalContext ctx) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        ctx.discovery().setCustomEventListener(InitMessage.class, (topVer, snd, msg) -> {
            Process proc = processes.computeIfAbsent(msg.requestId(), id -> new Process(msg.requestId()));

            if (proc.initFut.isDone())
                return;

            ClusterNode crd = coordinator();

            if (crd == null) {
                proc.initFut.onDone();

                onAllServersLeft();

                return;
            }

            proc.crdId = crd.id();

            if (crd.isLocal())
                initCoordinator(topVer, proc);

            proc.instance = registered.get(msg.processTypeId()).create();

            IgniteInternalFuture<Serializable> fut = proc.instance.execute(msg.request());

            fut.listen(f -> {
                if (f.error() != null)
                    proc.singleResFut.onDone(f.error());
                else
                    proc.singleResFut.onDone(f.result());

                ClusterNode crdNode = coordinator();

                if (crdNode != null)
                    sendSingleSingleMessage(proc, crdNode);
            });

            proc.initFut.onDone();
        });

        ctx.discovery().setCustomEventListener(FinishMessage.class, (topVer, snd, msg) -> {
            Process proc = processes.computeIfAbsent(msg.requestId(), id -> new Process(msg.requestId()));

            if (msg.hasError())
                proc.instance.cancel(msg.error());
            else
                proc.instance.finish(msg.result());

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

                            proc.singleResFut.listen(f -> sendSingleSingleMessage(proc, crd));
                        }
                        else
                            onAllServersLeft();
                    }
                    else if (ctx.localNodeId().equals(proc.crdId)) {
                        boolean rmvd, isEmpty;

                        synchronized (mux) {
                            rmvd = proc.remaining.remove(leftNodeId);

                            isEmpty = proc.remaining.isEmpty();
                        }

                        if (rmvd) {
                            proc.singleMsgs.remove(leftNodeId);

                            if (isEmpty)
                                onAllReceived(proc);
                        }
                    }
                });
            }
        }, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /**
     * Registers distributed process.
     * <p>
     * Note: Processes should be registered before the discovery manager started.
     *
     * @param p Distibuted process.
     * @param factory Distributed process factory.
     */
    public void register(DistributedProcesses p, DistributedProcessFactory factory) {
        registered.put(p.processTypeId(), factory);
    }

    /**
     * Starts distributed process.
     *
     * @param p Distibuted process.
     * @param req Initial request.
     */
    public void start(DistributedProcesses p, Serializable req) {
        try {
            InitMessage msg = new InitMessage(UUID.randomUUID(), p.processTypeId(), req);

            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.warning("Unable to start process.", e);
        }
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

    /**
     * Creates and sends finish message when all single nodes result received.
     *
     * @param p Process.
     */
    private void onAllReceived(Process p) {
        Optional<SingleNodeMessage> errMsg = p.singleMsgs.values().stream().filter(SingleNodeMessage::hasError).findFirst();

        FinishMessage msg;

        if (errMsg.isPresent())
            msg = new FinishMessage(p.id, errMsg.get().error());
        else {
            HashMap<UUID, Serializable> map = new HashMap<>(p.singleMsgs.size());

            p.singleMsgs.forEach((uuid, m) -> map.put(uuid, m.response()));

            Serializable res = p.instance.buildResult(map);

            msg = new FinishMessage(p.id, res);
        }

        try {
            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.warning("Unable to send action message.", e);
        }
    }

    /**
     * Sends single node message to coordinator.
     *
     * @param p Process.
     * @param crd Coordinator node to send message.
     */
    private void sendSingleSingleMessage(Process p, ClusterNode crd) {
        assert p.singleResFut.isDone();

        SingleNodeMessage singleMsg = new SingleNodeMessage(p.id, p.singleResFut.result(),
            (Exception)p.singleResFut.error());

        if (crd.isLocal())
            onSingleNodeMessageReceived(singleMsg, crd.id());
        else {
            try {
                ctx.io().sendToGridTopic(crd, GridTopic.TOPIC_DISTRIBUTED_PROCESS, singleMsg, SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                log.warning("Unable to send message.", e);
            }
        }
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
                proc.singleMsgs.put(nodeId, msg);

                if (isEmpty)
                    onAllReceived(proc);
            }
        });
    }

    /**
     * Handles case when all server nodes have left the grid.
     */
    private void onAllServersLeft() {
        processes.clear();
    }

    /** @return Cluster coordinator, {@code null} if failed to determine. */
    private @Nullable ClusterNode coordinator() {
        return U.oldest(ctx.discovery().aliveServerNodes(), null);
    }

    /** */
    private static class Process {
        /** Process id. */
        private final UUID id;

        /** Init coordinator future. */
        private final GridFutureAdapter<Void> initCrdFut = new GridFutureAdapter<>();

        /** Coordinator id. */
        private volatile UUID crdId;

        /** Local instance of process. */
        private volatile DistributedProcess instance;

        /** Init process future. */
        private final GridFutureAdapter<Void> initFut = new GridFutureAdapter<>();

        /** Remaining nodes to received single nodes result. */
        private final Set</*nodeId*/UUID> remaining = new GridConcurrentHashSet<>();

        /** Future for a single local node result. */
        private final GridFutureAdapter<Serializable> singleResFut = new GridFutureAdapter<>();

        /** Single nodes results. */
        private final ConcurrentHashMap</*nodeId*/UUID, SingleNodeMessage> singleMsgs = new ConcurrentHashMap<>();

        /** @param id Process id. */
        private Process(UUID id) {
            this.id = id;
        }
    }
}
