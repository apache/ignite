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
package org.apache.ignite.internal.processors.cache.distributed.dht.preloader.latch;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Class is responsible to create and manage instances of distributed latches {@link Latch}.
 */
public class ExchangeLatchManager {
    /** Version since latch management is available. */
    private static final IgniteProductVersion VERSION_SINCE = IgniteProductVersion.fromString("2.5.0");

    /**
     * Exchange latch V2 protocol introduces following optimization: Joining nodes are explicitly excluded from possible
     * latch participants.
     */
    public static final IgniteProductVersion PROTOCOL_V2_VERSION_SINCE = IgniteProductVersion.fromString("2.5.3");

    /** Logger. */
    private final IgniteLogger log;

    /** Context. */
    private final GridKernalContext ctx;

    /** Discovery manager. */
    @GridToStringExclude
    private final GridDiscoveryManager discovery;

    /** IO manager. */
    @GridToStringExclude
    private final GridIoManager io;

    /** Current coordinator. */
    @GridToStringExclude
    private volatile ClusterNode crd;

    /** Pending acks collection. */
    private final ConcurrentMap<CompletableLatchUid, Set<UUID>> pendingAcks = new ConcurrentHashMap<>();

    /** Server latches collection. */
    @GridToStringInclude
    private final ConcurrentMap<CompletableLatchUid, ServerLatch> serverLatches = new ConcurrentHashMap<>();

    /** Client latches collection. */
    @GridToStringInclude
    private final ConcurrentMap<CompletableLatchUid, ClientLatch> clientLatches = new ConcurrentHashMap<>();

    /**
     * Map (topology version -> joined node on this version). This map is needed to exclude joined nodes from latch
     * participants.
     */
    @GridToStringExclude
    private final ConcurrentMap<AffinityTopologyVersion, ClusterNode> joinedNodes = new ConcurrentHashMap<>();

    /** Lock. */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Constructor.
     *
     * @param ctx Kernal context.
     */
    public ExchangeLatchManager(GridKernalContext ctx) {
        this.ctx = ctx;
        this.log = ctx.log(getClass());
        this.discovery = ctx.discovery();
        this.io = ctx.io();

        if (!ctx.clientNode() && !ctx.isDaemon()) {
            ctx.io().addMessageListener(GridTopic.TOPIC_EXCHANGE, (nodeId, msg, plc) -> {
                if (msg instanceof LatchAckMessage)
                    processAck(nodeId, (LatchAckMessage)msg);
            });

            // First coordinator initialization.
            ctx.discovery().localJoinFuture().listen(f -> {
                if (f.error() == null)
                    this.crd = getLatchCoordinator(AffinityTopologyVersion.NONE);
            });

            ctx.event().addDiscoveryEventListener((e, cache) -> {
                assert e != null;
                assert e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED : this;

                // Do not process from discovery thread.
                // TODO: Should use queue to guarantee the order of processing left nodes.
                ctx.closure().runLocalSafe(() -> processNodeLeft(cache.version(), e.eventNode()));
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);

            ctx.event().addDiscoveryEventListener((e, cache) -> {
                assert e != null;
                assert e.type() == EVT_NODE_JOINED;

                joinedNodes.put(cache.version(), e.eventNode());
            }, EVT_NODE_JOINED);
        }
    }

    /**
     * Creates server latch with given {@code id} and {@code topVer}. Adds corresponding pending acks to it.
     *
     * @param latchUid Latch uid.
     * @param participants Participant nodes.
     * @return Server latch instance.
     */
    private Latch createServerLatch(CompletableLatchUid latchUid, Collection<ClusterNode> participants) {
        assert !serverLatches.containsKey(latchUid);

        ServerLatch latch = new ServerLatch(latchUid, participants);

        serverLatches.put(latchUid, latch);

        if (log.isDebugEnabled())
            log.debug("Server latch is created [latch=" + latchUid + ", participantsSize=" + participants.size() + "]");

        if (pendingAcks.containsKey(latchUid)) {
            Set<UUID> acks = pendingAcks.get(latchUid);

            for (UUID node : acks)
                if (latch.hasParticipant(node) && !latch.hasAck(node))
                    latch.ack(node);

            pendingAcks.remove(latchUid);
        }

        return latch;
    }

    /**
     * Creates client latch. If there is final ack corresponds to given {@code id} and {@code topVer}, latch will be
     * completed immediately.
     *
     * @param latchUid Latch uid.
     * @param coordinator Coordinator node.
     * @param participants Participant nodes.
     * @return Client latch instance.
     */
    private Latch createClientLatch(CompletableLatchUid latchUid, ClusterNode coordinator,
        Collection<ClusterNode> participants) {
        assert !serverLatches.containsKey(latchUid);
        assert !clientLatches.containsKey(latchUid);

        ClientLatch latch = new ClientLatch(latchUid, coordinator, participants);

        if (log.isDebugEnabled())
            log.debug("Client latch is created [latch=" + latchUid
                + ", crd=" + coordinator
                + ", participantsSize=" + participants.size() + "]");

        clientLatches.put(latchUid, latch);

        return latch;
    }

    /**
     * Creates new latch with specified {@code id} and {@code topVer} or returns existing latch.
     *
     * Participants of latch are calculated from given {@code topVer} as alive server nodes. If local node is
     * coordinator {@code ServerLatch} instance will be created, otherwise {@code ClientLatch} instance.
     *
     * @param id Latch id.
     * @param topVer Latch topology version.
     * @return Latch instance.
     */
    public Latch getOrCreate(String id, AffinityTopologyVersion topVer) {
        lock.lock();

        try {
            final CompletableLatchUid latchUid = new CompletableLatchUid(id, topVer);

            CompletableLatch latch = clientLatches.containsKey(latchUid) ?
                clientLatches.get(latchUid) : serverLatches.get(latchUid);

            if (latch != null)
                return latch;

            ClusterNode coordinator = getLatchCoordinator(topVer);

            if (coordinator == null)
                return null;

            Collection<ClusterNode> participants = getLatchParticipants(topVer);

            return coordinator.isLocal()
                ? createServerLatch(latchUid, participants)
                : createClientLatch(latchUid, coordinator, participants);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Drops client latches created by {@link #getOrCreate(String, AffinityTopologyVersion)}. The corresponding
     * latches should be created before this method is invoked.
     * <p>
     * This method must be called when it is guaranteed that all nodes have processed the latches messages. In
     * the context of partitions map exchange this can be done when exchange future is completed.
     *
     * @param topVer Latch topology version.
     */
    public void dropClientLatches(AffinityTopologyVersion topVer) {
        lock.lock();

        try {
            for (CompletableLatchUid latchUid : clientLatches.keySet()) {
                if (latchUid.topVer.equals(topVer)) {
                    ClientLatch latch = clientLatches.remove(latchUid);

                    if (log.isDebugEnabled())
                        log.debug("Dropping client latch [id=" + latchUid + ", latch=" + latch + ']');

                    pendingAcks.remove(latchUid);
                }
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Gets alive server nodes from disco cache for provided AffinityTopologyVersion.
     *
     * @param topVer Topology version.
     * @return Collection of nodes with at least one cache configured.
     * @throws IgniteException If nodes for the given {@code topVer} cannot be found in the discovery history.
     */
    private Collection<ClusterNode> aliveNodesForTopologyVer(AffinityTopologyVersion topVer) {
        if (topVer == AffinityTopologyVersion.NONE)
            return discovery.aliveServerNodes();
        else {
            Collection<ClusterNode> histNodes = discovery.topology(topVer.topologyVersion());

            if (histNodes != null)
                return histNodes.stream().filter(n -> !n.isClient() && !n.isDaemon() && discovery.alive(n))
                    .collect(Collectors.toList());
            else
                throw new IgniteException("Topology " + topVer + " not found in discovery history. "
                        + "Consider increasing IGNITE_DISCOVERY_HISTORY_SIZE property. Current value is "
                        + IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_DISCOVERY_HISTORY_SIZE, -1));
        }
    }

    /**
     * @param topVer Latch topology version.
     * @return Collection of alive server nodes with latch functionality.
     */
    private Collection<ClusterNode> getLatchParticipants(AffinityTopologyVersion topVer) {
        Collection<ClusterNode> aliveNodes = aliveNodesForTopologyVer(topVer);

        List<ClusterNode> participantNodes = aliveNodes
            .stream()
            .filter(node -> node.version().compareTo(VERSION_SINCE) >= 0)
            .collect(Collectors.toList());

        if (canSkipJoiningNodes(topVer))
            return excludeJoinedNodes(participantNodes, topVer);

        return participantNodes;
    }

    /**
     * Excludes a node that was joined on given {@code topVer} from participant nodes.
     *
     * @param participantNodes Participant nodes.
     * @param topVer Topology version.
     */
    private List<ClusterNode> excludeJoinedNodes(List<ClusterNode> participantNodes, AffinityTopologyVersion topVer) {
        ClusterNode joinedNode = joinedNodes.get(topVer);

        if (joinedNode != null)
            participantNodes.remove(joinedNode);

        return participantNodes;
    }

    /**
     * @param topVer Latch topology version.
     * @return Oldest alive server node with latch functionality.
     */
    @Nullable private ClusterNode getLatchCoordinator(AffinityTopologyVersion topVer) {
        Collection<ClusterNode> aliveNodes = aliveNodesForTopologyVer(topVer);

        List<ClusterNode> applicableNodes = aliveNodes
            .stream()
            .filter(node -> node.version().compareTo(VERSION_SINCE) >= 0)
            .sorted(Comparator.comparing(ClusterNode::order))
            .collect(Collectors.toList());

        if (applicableNodes.isEmpty())
            return null;

        if (canSkipJoiningNodes(topVer))
            applicableNodes = excludeJoinedNodes(applicableNodes, topVer);

        return applicableNodes.get(0);
    }

    /**
     * Checks that latch manager can use V2 protocol and skip joining nodes from latch participants.
     *
     * @param topVer Topology version.
     * @throws IgniteException If nodes for the given {@code topVer} cannot be found in the discovery history.
     */
    public boolean canSkipJoiningNodes(AffinityTopologyVersion topVer) {
        Collection<ClusterNode> applicableNodes = aliveNodesForTopologyVer(topVer);

        return applicableNodes.stream()
            .allMatch(node -> node.version().compareTo(PROTOCOL_V2_VERSION_SINCE) >= 0);
    }

    /**
     * Processes ack message from given {@code from} node.
     *
     * Completes client latch in case of final ack message.
     *
     * If no latch is associated with message, ack is placed to {@link #pendingAcks} set.
     *
     * @param from Node sent ack.
     * @param message Ack message.
     */
    private void processAck(UUID from, LatchAckMessage message) {
        lock.lock();

        try {
            CompletableLatchUid latchUid = new CompletableLatchUid(message.latchId(), message.topVer());

            if (discovery.topologyVersionEx().compareTo(message.topVer()) < 0) {
                // It means that this node doesn't receive changed topology version message yet
                // but received ack message from client latch.
                // It can happen when we don't have guarantees of received message order for example in ZookeeperSpi.
                pendingAcks.computeIfAbsent(latchUid, id -> new GridConcurrentHashSet<>()).add(from);

                return;
            }

            ClusterNode coordinator = getLatchCoordinator(message.topVer());

            if (coordinator == null)
                return;

            if (message.isFinal()) {
                if (log.isDebugEnabled())
                    log.debug("Process final ack [latch=" + latchUid + ", from=" + from + "]");

                assert serverLatches.containsKey(latchUid) || clientLatches.containsKey(latchUid);

                if (clientLatches.containsKey(latchUid)) {
                    ClientLatch latch = clientLatches.get(latchUid);

                    latch.complete();
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Process ack [latch=" + latchUid + ", from=" + from + "]");

                if (serverLatches.containsKey(latchUid)) {
                    ServerLatch latch = serverLatches.get(latchUid);

                    if (latch.hasParticipant(from) && !latch.hasAck(from))
                        latch.ack(from);
                }
                else {
                    ClientLatch clientLatch = clientLatches.get(latchUid);

                    if (clientLatch != null && clientLatch.isCompleted())
                        sendAck(from, clientLatch.id, true);
                    else
                        pendingAcks.computeIfAbsent(latchUid, id -> new GridConcurrentHashSet<>()).add(from);
                }
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Changes coordinator to current local node. Restores all server latches from pending acks and own client latches.
     */
    private void becomeNewCoordinator() {
        if (log.isInfoEnabled())
            log.info("Become new coordinator " + crd.id());

        Set<CompletableLatchUid> latchesToRestore = new HashSet<>();

        latchesToRestore.addAll(pendingAcks.keySet());
        latchesToRestore.addAll(clientLatches.keySet());

        for (CompletableLatchUid latchUid : latchesToRestore) {
            AffinityTopologyVersion topVer = latchUid.topVer;

            ClientLatch clientLatch = clientLatches.get(latchUid);

            if (clientLatch == null || !clientLatch.isCompleted()) {
                Collection<ClusterNode> participants = getLatchParticipants(topVer);

                if (!participants.isEmpty())
                    createServerLatch(latchUid, participants);
            }
        }
    }

    /**
     * Handles node left discovery event.
     *
     * Summary:
     * Removes pending acks corresponds to the left node.
     * Adds fake acknowledgements to server latches where such node was participant.
     * Changes client latches coordinator to oldest available server node where such node was coordinator.
     * Detects coordinator change.
     *
     * @param left Left node.
     */
    private void processNodeLeft(AffinityTopologyVersion topVer, ClusterNode left) {
        assert this.crd != null : "Coordinator is not initialized";

        lock.lock();

        try {
            if (log.isDebugEnabled())
                log.debug("Process node left " + left.id());

            ClusterNode coordinator = getLatchCoordinator(topVer);

            if (coordinator == null)
                return;

            // Removed node from joined nodes map.
            joinedNodes.entrySet().stream()
                .filter(e -> e.getValue().equals(left))
                .map(e -> e.getKey()) // Map to topology version when node has joined.
                .forEach(joinedNodes::remove);

            // Clear pending acks.
            for (Map.Entry<CompletableLatchUid, Set<UUID>> ackEntry : pendingAcks.entrySet())
                if (ackEntry.getValue().contains(left.id()))
                    pendingAcks.get(ackEntry.getKey()).remove(left.id());

            // Change coordinator for client latches.
            for (Map.Entry<CompletableLatchUid, ClientLatch> latchEntry : clientLatches.entrySet()) {
                ClientLatch latch = latchEntry.getValue();

                if (latch.hasCoordinator(left.id())) {
                    // Change coordinator for latch and re-send ack if necessary.
                    if (latch.hasParticipant(coordinator.id()))
                        latch.newCoordinator(coordinator);
                    else {
                        /* If new coordinator is not able to take control on the latch,
                           it means that all other latch participants are left from topology
                           and there is no reason to track such latch. */
                        AffinityTopologyVersion latchTopVer = latchEntry.getKey().topVer;

                        assert getLatchParticipants(latchTopVer).isEmpty();

                        latch.complete(new IgniteCheckedException("All latch participants are left from topology."));
                    }
                }
            }

            // Add acknowledgements from left node.
            for (Map.Entry<CompletableLatchUid, ServerLatch> latchEntry : serverLatches.entrySet()) {
                ServerLatch latch = latchEntry.getValue();

                if (latch.hasParticipant(left.id()) && !latch.hasAck(left.id())) {
                    if (log.isDebugEnabled())
                        log.debug("Process node left [latch=" + latchEntry.getKey() + ", left=" + left.id() + "]");

                    latch.ack(left.id());
                }
            }

            // Coordinator is changed.
            if (coordinator.isLocal() && this.crd.id() != coordinator.id()) {
                this.crd = coordinator;

                becomeNewCoordinator();
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Sends ack message to the given node ID with the given latch ID.
     *
     * @param nodeId Node ID to send ack to.
     * @param latchUid Latch ID.
     * @param finalAck If {@code true}, the final (completing) ack message will be sent, otherwise the initial ack
     *      (sent from participants to coordinator) will be sent.
     */
    private void sendAck(UUID nodeId, CompletableLatchUid latchUid, boolean finalAck) {
        try {
            if (discovery.alive(nodeId)) {
                io.sendToGridTopic(
                    nodeId,
                    GridTopic.TOPIC_EXCHANGE,
                    new LatchAckMessage(latchUid.id, latchUid.topVer, finalAck),
                    GridIoPolicy.SYSTEM_POOL
                );

                if (log.isDebugEnabled())
                    log.debug("Ack has sent [latch=" + latchUid + ", final=" + finalAck + ", to=" + nodeId + "]");
            }
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send ack [latch=" + latchUid + ", final=" + finalAck + ", to=" + nodeId +
                    ", err=" + e.getMessage() + ']');
        }
    }

    /**
     * Latch creating on coordinator node.
     * Latch collects acks from participants: non-coordinator nodes and current local node.
     * Latch completes when all acks from all participants are received.
     *
     * After latch completion final ack is sent to all participants.
     */
    class ServerLatch extends CompletableLatch {
        /** Number of latch permits. This is needed to track number of countDown invocations. */
        private final AtomicInteger permits;

        /** Set of received acks. */
        private final Set<UUID> acks = new GridConcurrentHashSet<>();

        /**
         * Constructor.
         *
         * @param latchUid Latch uid.
         * @param participants Participant nodes.
         */
        ServerLatch(CompletableLatchUid latchUid, Collection<ClusterNode> participants) {
            super(latchUid, participants);

            permits = new AtomicInteger(participants.size());

            // Send final acks when latch is completed.
            complete.listen(f -> {
                for (ClusterNode node : participants)
                    sendAck(node.id(), latchId(), true);
            });
        }

        /**
         * Checks if latch has ack from given node.
         *
         * @param from Node.
         * @return {@code true} if latch has ack from given node.
         */
        private boolean hasAck(UUID from) {
            return acks.contains(from);
        }

        /**
         * Receives ack from given node. Count downs latch if ack was not already processed.
         *
         * @param from Node.
         */
        private void ack(UUID from) {
            if (log.isDebugEnabled())
                log.debug("Ack is accepted [latch=" + latchId() + ", from=" + from + "]");

            countDown0(from);
        }

        /**
         * Count down latch from ack of given node. Completes latch if all acks are received.
         *
         * @param node Node.
         */
        private void countDown0(UUID node) {
            if (isCompleted() || acks.contains(node))
                return;

            acks.add(node);

            int remaining = permits.decrementAndGet();

            if (log.isDebugEnabled())
                log.debug("Count down [latch=" + latchId() + ", remaining=" + remaining + "]");

            if (remaining == 0) {
                complete();

                serverLatches.remove(id);

                if (log.isDebugEnabled())
                    log.debug("Dropping server latch [id=" + id + ", latch=" + this + ']');
            }
        }

        /** {@inheritDoc} */
        @Override public void countDown() {
            countDown0(ctx.localNodeId());
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            Set<UUID> pendingAcks = participants.stream().filter(ack -> !acks.contains(ack)).collect(Collectors.toSet());

            return S.toString(ServerLatch.class, this,
                "pendingAcks", pendingAcks,
                "super", super.toString());
        }
    }

    /**
     * Latch creating on non-coordinator node. Latch completes when final ack from coordinator is received.
     */
    class ClientLatch extends CompletableLatch {
        /** Latch coordinator node. Can be changed if coordinator is left from topology. */
        private volatile ClusterNode coordinator;

        /** Flag indicates that ack is sent to coordinator. */
        private boolean ackSent;

        /**
         * Constructor.
         *
         * @param latchUid Latch uid.
         * @param coordinator Coordinator node.
         * @param participants Participant nodes.
         */
        ClientLatch(CompletableLatchUid latchUid, ClusterNode coordinator, Collection<ClusterNode> participants) {
            super(latchUid, participants);

            this.coordinator = coordinator;
        }

        /**
         * Checks if latch coordinator is given {@code node}.
         *
         * @param node Node.
         * @return {@code true} if latch coordinator is given node.
         */
        private boolean hasCoordinator(UUID node) {
            return coordinator.id().equals(node);
        }

        /**
         * Changes coordinator of latch and resends ack to new coordinator if needed.
         *
         * @param coordinator New coordinator.
         */
        private void newCoordinator(ClusterNode coordinator) {
            synchronized (this) {
                if (log.isDebugEnabled())
                    log.debug("Coordinator is changed [latch=" + latchId() + ", newCrd=" + coordinator.id() +
                        ", ackSent=" + ackSent + "]");

                this.coordinator = coordinator;

                // Resend ack to new coordinator.
                if (ackSent)
                    sendAck();
            }
        }

        /**
         * Sends ack to coordinator node. There is ack deduplication on coordinator. So it's fine to send same ack
         * twice.
         */
        private void sendAck() {
            ackSent = true;

            ExchangeLatchManager.this.sendAck(coordinator.id(), id, false);
        }

        /** {@inheritDoc} */
        @Override public void countDown() {
            if (isCompleted())
                return;

            // Synchronize in case of changed coordinator.
            synchronized (this) {
                sendAck();
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ClientLatch.class, this,
                "super", super.toString());
        }
    }

    /**
     * Base latch functionality with implemented complete / await logic.
     */
    private abstract static class CompletableLatch implements Latch {
        /** Latch id. */
        @GridToStringInclude
        protected final CompletableLatchUid id;

        /** Latch node participants. Only participant nodes are able to change state of latch. */
        @GridToStringExclude
        protected final Set<UUID> participants;

        /** Future indicates that latch is completed. */
        @GridToStringExclude
        protected final GridFutureAdapter<?> complete = new GridFutureAdapter<>();

        /**
         * Constructor.
         *
         * @param latchUid Latch uid.
         * @param participants Participant nodes.
         */
        CompletableLatch(CompletableLatchUid latchUid, Collection<ClusterNode> participants) {
            id = latchUid;

            this.participants = participants.stream().map(ClusterNode::id).collect(Collectors.toSet());
        }

        /** {@inheritDoc} */
        @Override public void await() throws IgniteCheckedException {
            complete.get();
        }

        /** {@inheritDoc} */
        @Override public void await(long timeout, TimeUnit timeUnit) throws IgniteCheckedException {
            complete.get(timeout, timeUnit);
        }

        /**
         * Checks if latch participants contain given {@code node}.
         *
         * @param node Node.
         * @return {@code true} if latch participants contain given node.
         */
        boolean hasParticipant(UUID node) {
            return participants.contains(node);
        }

        /**
         * @return {@code true} if latch is completed.
         */
        boolean isCompleted() {
            return complete.isDone();
        }

        /**
         * Completes current latch.
         */
        void complete() {
            complete.onDone();
        }

        /**
         * Completes current latch with given {@code error}.
         *
         * @param error Error.
         */
        void complete(Throwable error) {
            complete.onDone(error);
        }

        /**
         * @return Full latch id.
         */
        CompletableLatchUid latchId() {
            return id;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CompletableLatch.class, this);
        }
    }

    /**
     * Latch id + topology
     */
    private static class CompletableLatchUid {
        /** Id. */
        @GridToStringInclude
        private String id;

        /** Topology version. */
        @GridToStringInclude
        private AffinityTopologyVersion topVer;

        /**
         * @param id Id.
         * @param topVer Topology version.
         */
        private CompletableLatchUid(String id, AffinityTopologyVersion topVer) {
            this.id = id;
            this.topVer = topVer;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            CompletableLatchUid uid = (CompletableLatchUid)o;
            return Objects.equals(id, uid.id) &&
                Objects.equals(topVer, uid.topVer);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(id, topVer);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CompletableLatchUid.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExchangeLatchManager.class, this);
    }
}
