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
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;

/**
 * Distributed process is a cluster-wide process that accumulates single nodes results to finish itself.
 * <p>
 * The process consists of the following phases:
 * <ol>
 *  <li>Initial request starts process. (Sent via discovery)</li>
 *  <li>Each server node process an initial request and send the single node result to the coordinator. (Sent via
 *  communication)</li>
 *  <li>The coordinator accumulate all single nodes results and finish process. (Sent via discovery)</li>
 * </ol>
 * <p>
 * Several processes of one type can be started at the same time.
 *
 * @param <I> Request type.
 * @param <R> Result type.
 *
 * @see DistributedProcessAction
 * @see DistributedProcesses
 */
public class DistributedProcess<I extends Serializable, R extends Serializable> {
    /** Kernal context. */
    private final DistributedProcesses type;

    /** Map of all active processes. */
    private final ConcurrentHashMap</*processId*/UUID, Process<I, R>> processes = new ConcurrentHashMap<>(1);

    /** Synchronization mutex for coordinator initializing and the remaining collection operations. */
    private final Object mux = new Object();

    /** Kernal context. */
    private final GridKernalContext ctx;

    /** Logger. */
    private final IgniteLogger log;

    /**
     * @param ctx Kernal context.
     * @param type Process type.
     */
    public DistributedProcess(GridKernalContext ctx, DistributedProcesses type, DistributedProcessFactory<I, R> factory) {
        this.ctx = ctx;
        this.type = type;

        log = ctx.log(getClass());

        ctx.discovery().setCustomEventListener(InitMessage.class, (topVer, snd, msg) -> {
            if (msg.type() != type)
                return;

            Process<I, R> p = processes.computeIfAbsent(msg.processId(), id -> new Process<>(msg.processId()));

            // May be completed in case of double delivering.
            if (p.initFut.isDone())
                return;

            ClusterNode crd = coordinator();

            if (crd == null) {
                p.initFut.onDone();

                onAllServersLeft();

                return;
            }

            p.crdId = crd.id();

            if (crd.isLocal())
                initCoordinator(p, topVer);

            p.instance = factory.create();

            IgniteInternalFuture<R> fut = p.instance.execute((I)msg.request());

            fut.listen(f -> {
                if (f.error() != null)
                    p.singleResFut.onDone(f.error());
                else
                    p.singleResFut.onDone(f.result());

                ClusterNode crdNode = coordinator();

                if (crdNode != null && !ctx.clientNode())
                    sendSingleSingleMessage(p, crdNode);
            });

            p.initFut.onDone();
        });

        ctx.discovery().setCustomEventListener(FullMessage.class, (topVer, snd, msg0) -> {
            if (msg0.type() != type)
                return;

            FullMessage<R> msg = (FullMessage<R>)msg0;

            Process<I, R> p = processes.get(msg.processId());

            if (p == null) {
                log.warning("Received finish distributed process message for an uninitialized process [msg=" +
                    msg + ']');

                return;
            }

            if (p.completeFut.isDone())
                return;

            p.instance.finish(msg.result(), msg.error());

            p.completeFut.onDone();

            processes.remove(msg.processId());
        });

        ctx.io().addMessageListener(GridTopic.TOPIC_DISTRIBUTED_PROCESS, (nodeId, msg0, plc) -> {
            if (msg0 instanceof SingleNodeMessage && ((SingleNodeMessage)msg0).type() == type) {
                SingleNodeMessage<R> msg = (SingleNodeMessage<R>)msg0;

                if (msg.type() == type)
                    onSingleNodeMessageReceived(msg, nodeId);
            }
        });

        ctx.event().addDiscoveryEventListener((evt, discoCache) -> {
            UUID leftNodeId = evt.eventNode().id();

            for (Process<I, R> p : processes.values()) {
                p.initFut.listen(fut -> {
                    boolean crdChanged = F.eq(leftNodeId, p.crdId);

                    if (crdChanged) {
                        ClusterNode crd = coordinator();

                        if (crd != null) {
                            p.crdId = crd.id();

                            if (crd.isLocal())
                                initCoordinator(p, discoCache.version());

                            if (!ctx.clientNode())
                                p.singleResFut.listen(f -> sendSingleSingleMessage(p, crd));
                        }
                        else
                            onAllServersLeft();
                    }
                    else if (ctx.localNodeId().equals(p.crdId)) {
                        boolean rmvd, isEmpty;

                        synchronized (mux) {
                            rmvd = p.remaining.remove(leftNodeId);

                            isEmpty = p.remaining.isEmpty();
                        }

                        if (rmvd) {
                            assert !p.singleMsgs.containsKey(leftNodeId);

                            if (isEmpty)
                                finishProcess(p);
                        }
                    }
                });
            }
        }, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /**
     * Starts distributed process.
     *
     * @param req Initial request.
     */
    public void start(I req) {
        try {
            InitMessage msg = new InitMessage<>(UUID.randomUUID(), type, req);

            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.warning("Unable to start process.", e);
        }
    }

    /**
     * Initiates process coordinator.
     *
     * @param p Process.
     * @param topVer Topology version.
     */
    private void initCoordinator(Process<I, R> p, AffinityTopologyVersion topVer) {
        Set<UUID> aliveSrvNodesIds = new HashSet<>();

        for (ClusterNode node : ctx.discovery().serverNodes(topVer)) {
            if (ctx.discovery().alive(node))
                aliveSrvNodesIds.add(node.id());
        }

        synchronized (mux) {
            if (p.initCrdFut.isDone())
                return;

            assert p.remaining.isEmpty();

            p.remaining.addAll(aliveSrvNodesIds);

            p.initCrdFut.onDone();
        }
    }

    /**
     * Sends single node message to coordinator.
     *
     * @param p Process.
     * @param crd Coordinator node to send message.
     */
    private void sendSingleSingleMessage(Process<I, R> p, ClusterNode crd) {
        assert p.singleResFut.isDone();

        SingleNodeMessage<R> singleMsg = new SingleNodeMessage<>(p.id, type, p.singleResFut.result(),
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
    private void onSingleNodeMessageReceived(SingleNodeMessage<R> msg, UUID nodeId) {
        Process<I, R> p = processes.computeIfAbsent(msg.processId(), id -> new Process<>(msg.processId()));

        p.initCrdFut.listen(f -> {
            boolean rmvd, isEmpty;

            synchronized (mux) {
                rmvd = p.remaining.remove(nodeId);

                isEmpty = p.remaining.isEmpty();
            }

            if (rmvd) {
                p.singleMsgs.put(nodeId, msg);

                if (isEmpty)
                    finishProcess(p);
            }
        });
    }

    /**
     * Creates and sends finish message when all single nodes result received.
     *
     * @param p Process.
     */
    private void finishProcess(Process<I, R> p) {
        HashMap<UUID, R> res = new HashMap<>();

        HashMap<UUID, Exception> err = new HashMap<>();

        p.singleMsgs.forEach((uuid, msg) -> {
            if (msg.hasError())
                err.put(uuid, msg.error());
            else
                res.put(uuid, msg.response());
        });

        FullMessage msg = new FullMessage<>(p.id, type, res, err);

        try {
            ctx.discovery().sendCustomEvent(msg);
        }
        catch (IgniteCheckedException e) {
            log.warning("Unable to send action message.", e);
        }
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
    private static class Process<I extends Serializable, R extends Serializable> {
        /** Process id. */
        private final UUID id;

        /** Init coordinator future. */
        private final GridFutureAdapter<Void> initCrdFut = new GridFutureAdapter<>();

        /** Coordinator id. */
        private volatile UUID crdId;

        /** Local instance of process. */
        private volatile DistributedProcessAction<I, R> instance;

        /** Init process future. */
        private final GridFutureAdapter<Void> initFut = new GridFutureAdapter<>();

        /** Remaining nodes to received single nodes result. */
        private final Set</*nodeId*/UUID> remaining = new GridConcurrentHashSet<>();

        /** Future for a single local node result. */
        private final GridFutureAdapter<R> singleResFut = new GridFutureAdapter<>();

        /** Single nodes results. */
        private final ConcurrentHashMap</*nodeId*/UUID, SingleNodeMessage<R>> singleMsgs = new ConcurrentHashMap<>();

        /** Process completion future. */
        private final GridFutureAdapter<Void> completeFut = new GridFutureAdapter<>();

        /** @param id Process id. */
        private Process(UUID id) {
            this.id = id;
        }
    }
}
