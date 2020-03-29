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

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteDiagnosticAware;
import org.apache.ignite.internal.IgniteDiagnosticPrepareContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager.exchangeProtocolVersion;

/**
 *
 */
public class InitNewCoordinatorFuture extends GridCompoundFuture implements IgniteDiagnosticAware {
    /** */
    private final ClusterNode locNode;

    /** */
    private GridDhtPartitionsFullMessage fullMsg;

    /** */
    private Set<UUID> awaited = new HashSet<>();

    /** */
    private Map<ClusterNode, GridDhtPartitionsSingleMessage> msgs = new HashMap<>();

    /** */
    private Map<UUID, GridDhtPartitionsSingleMessage> joinExchMsgs;

    /** */
    private GridFutureAdapter restoreStateFut;

    /** */
    private final IgniteLogger log;

    /** */
    private AffinityTopologyVersion initTopVer;

    /** */
    private AffinityTopologyVersion resTopVer;

    /** */
    private Map<UUID, GridDhtPartitionExchangeId> joinedNodes;

    /** */
    private boolean restoreState;

    /**
     * @param cctx Context.
     */
    InitNewCoordinatorFuture(GridCacheSharedContext cctx) {
        this.log = cctx.logger(getClass());
        this.locNode = cctx.localNode();
    }

    /**
     * @param exchFut Current future.
     * @throws IgniteCheckedException If failed.
     */
    public void init(GridDhtPartitionsExchangeFuture exchFut) throws IgniteCheckedException {
        initTopVer = exchFut.initialVersion();

        GridCacheSharedContext cctx = exchFut.sharedContext();

        restoreState = exchangeProtocolVersion(exchFut.context().events().discoveryCache().minimumNodeVersion()) > 1;

        boolean newAff = exchFut.localJoinExchange();

        IgniteInternalFuture<?> fut = cctx.affinity().initCoordinatorCaches(exchFut, newAff);

        if (fut != null)
            add(fut);

        if (restoreState) {
            DiscoCache curDiscoCache = cctx.discovery().discoCache();

            DiscoCache discoCache = exchFut.events().discoveryCache();

            List<ClusterNode> nodes = new ArrayList<>();

            synchronized (this) {
                for (ClusterNode node : discoCache.allNodes()) {
                    if (!node.isLocal() && cctx.discovery().alive(node)) {
                        awaited.add(node.id());

                        nodes.add(node);
                    }
                    else if (!node.isLocal()) {
                        if (log.isInfoEnabled())
                            log.info("Init new coordinator future will skip remote node: " + node);
                    }
                }

                if (exchFut.context().mergeExchanges() && !curDiscoCache.version().equals(discoCache.version())) {
                    for (ClusterNode node : curDiscoCache.allNodes()) {
                        if (discoCache.node(node.id()) == null) {
                            if (exchangeProtocolVersion(node.version()) == 1)
                                break;

                            awaited.add(node.id());

                            nodes.add(node);

                            if (joinedNodes == null)
                                joinedNodes = new HashMap<>();

                            GridDhtPartitionExchangeId exchId = new GridDhtPartitionExchangeId(node.id(),
                                EVT_NODE_JOINED,
                                new AffinityTopologyVersion(node.order()));

                            joinedNodes.put(node.id(), exchId);
                        }
                    }
                }

                if (joinedNodes == null)
                    joinedNodes = Collections.emptyMap();

                if (!awaited.isEmpty()) {
                    restoreStateFut = new GridFutureAdapter();

                    add(restoreStateFut);
                }
            }

            if (log.isInfoEnabled()) {
                log.info("Try restore exchange result [awaited=" + awaited +
                    ", joined=" + joinedNodes.keySet() +
                    ", nodes=" + U.nodeIds(nodes) +
                    ", discoAllNodes=" + U.nodeIds(discoCache.allNodes()) + ']');
            }

            if (!nodes.isEmpty()) {
                GridDhtPartitionsSingleRequest req = GridDhtPartitionsSingleRequest.restoreStateRequest(exchFut.exchangeId(),
                    exchFut.exchangeId());

                for (ClusterNode node : nodes) {
                    try {
                        GridDhtPartitionsSingleRequest sndReq = req;

                        if (joinedNodes.containsKey(node.id())) {
                            sndReq = GridDhtPartitionsSingleRequest.restoreStateRequest(
                                joinedNodes.get(node.id()),
                                exchFut.exchangeId());
                        }

                        cctx.io().send(node, sndReq, GridIoPolicy.SYSTEM_POOL);
                    }
                    catch (ClusterTopologyCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send partitions request, node failed: " + node);

                        onNodeLeft(node.id());
                    }
                }
            }
        }

        markInitialized();
    }

    /**
     * @return {@code True} if new coordinator tried to restore exchange state.
     */
    boolean restoreState() {
        return restoreState;
    }

    /**
     * @return Received messages.
     */
    Map<ClusterNode, GridDhtPartitionsSingleMessage> messages() {
        return msgs;
    }

    /**
     * @return Messages for merged join exchanges.
     */
    Map<UUID, GridDhtPartitionsSingleMessage> mergedJoinExchangeMessages() {
        return joinExchMsgs;
    }

    /**
     * @return Full message is some of nodes received it from previous coordinator.
     */
    GridDhtPartitionsFullMessage fullMessage() {
        return fullMsg;
    }

    /**
     * @return Result topology version from nodes that already finished this exchange.
     */
    AffinityTopologyVersion resultTopologyVersion() {
        synchronized (this) {
            return resTopVer;
        }
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    public void onMessage(ClusterNode node, GridDhtPartitionsSingleMessage msg) {
        if (log.isInfoEnabled()) {
            log.info("Init new coordinator, received response [node=" + node.id() +
                ", fullMsg=" + (msg.finishMessage() != null) +
                ", affReq=" + !F.isEmpty(msg.cacheGroupsAffinityRequest()) + ']');
        }

        assert msg.restoreState() : msg;

        boolean done = false;

        synchronized (this) {
            if (awaited.remove(node.id())) {
                GridDhtPartitionsFullMessage fullMsg0 = msg.finishMessage();

                if (fullMsg0 != null && fullMsg0.resultTopologyVersion() != null) {
                    if (node.isClient() || node.isDaemon()) {
                        assert resTopVer == null || resTopVer.equals(fullMsg0.resultTopologyVersion());

                        resTopVer = fullMsg0.resultTopologyVersion();
                    }
                    else {
                        assert fullMsg == null || fullMsg.resultTopologyVersion().equals(fullMsg0.resultTopologyVersion());

                        fullMsg = fullMsg0;
                    }
                }
                else
                    msgs.put(node, msg);

                done = awaited.isEmpty();
            }

            if (done)
                onAllReceived();
        }

        if (done)
            restoreStateFut.onDone();
    }

    /**
     *
     */
    private void onAllReceived() {
        if (fullMsg != null) {
            AffinityTopologyVersion resVer = fullMsg.resultTopologyVersion();

            for (Iterator<Map.Entry<ClusterNode, GridDhtPartitionsSingleMessage>> it = msgs.entrySet().iterator();
                it.hasNext(); ) {
                Map.Entry<ClusterNode, GridDhtPartitionsSingleMessage> e = it.next();

                GridDhtPartitionExchangeId msgVer = joinedNodes.get(e.getKey().id());

                if (msgVer != null) {
                    assert msgVer.topologyVersion().compareTo(initTopVer) > 0 : msgVer;

                    if (log.isInfoEnabled()) {
                        log.info("Process joined node message [resVer=" + resVer +
                            ", initTopVer=" + initTopVer +
                            ", msgVer=" + msgVer.topologyVersion() + ']');
                    }

                    if (msgVer.topologyVersion().compareTo(resVer) > 0)
                        it.remove();
                    else {
                        GridDhtPartitionsSingleMessage msg = e.getValue();

                        msg.exchangeId(msgVer);

                        if (joinExchMsgs == null)
                            joinExchMsgs = new HashMap<>();

                        joinExchMsgs.put(e.getKey().id(), msg);
                    }
                }
            }
        }
        else {
            for (Iterator<Map.Entry<ClusterNode, GridDhtPartitionsSingleMessage>> it = msgs.entrySet().iterator(); it.hasNext(); ) {
                Map.Entry<ClusterNode, GridDhtPartitionsSingleMessage> e = it.next();

                GridDhtPartitionExchangeId msgVer = joinedNodes.get(e.getKey().id());

                if (msgVer != null) {
                    it.remove();

                    assert msgVer.topologyVersion().compareTo(initTopVer) > 0 : msgVer;

                    if (log.isInfoEnabled()) {
                        log.info("Process joined node message [initTopVer=" + initTopVer +
                            ", msgVer=" + msgVer.topologyVersion() + ']');
                    }

                    if (joinExchMsgs == null)
                        joinExchMsgs = new HashMap<>();

                    e.getValue().exchangeId(msgVer);

                    joinExchMsgs.put(e.getKey().id(), e.getValue());
                }
            }

        }
    }

    /**
     * @param nodeId Node ID.
     * @return Single message for node joined after exchange start.
     */
    @Nullable GridDhtPartitionsSingleMessage joinExchangeMessage(UUID nodeId) {
        return joinExchMsgs != null ? joinExchMsgs.get(nodeId) : null;
    }

    /**
     * @param nodeId Failed node ID.
     */
    public void onNodeLeft(UUID nodeId) {
        if (log.isInfoEnabled())
            log.info("Init new coordinator, node left [node=" + nodeId + ']');

        boolean done;

        synchronized (this) {
            done = awaited.remove(nodeId) && awaited.isEmpty();

            if (done)
                onAllReceived();
        }

        if (done)
            restoreStateFut.onDone();
    }

    /** {@inheritDoc} */
    @Override public void addDiagnosticRequest(IgniteDiagnosticPrepareContext diagCtx) {
        if (!isDone()) {
            synchronized (this) {
                diagCtx.exchangeInfo(locNode.id(), initTopVer, "InitNewCoordinatorFuture waiting for " +
                    "GridDhtPartitionsSingleMessages from nodes=" + awaited);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "InitNewCoordinatorFuture [" +
            "initTopVer=" + initTopVer +
            ", awaited=" + awaited +
            ", joinedNodes=" + joinedNodes +
            ']';
    }
}
