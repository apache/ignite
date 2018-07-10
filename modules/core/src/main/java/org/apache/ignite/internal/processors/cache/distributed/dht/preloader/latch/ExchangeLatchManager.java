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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteProductVersion;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

/**
 * Class is responsible to create and manage instances of distributed latches {@link Latch}.
 */
public class ExchangeLatchManager {
    /** Version since latch management is available. */
    private static final IgniteProductVersion VERSION_SINCE = IgniteProductVersion.fromString("2.5.0");

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
    private final ConcurrentMap<T2<String, AffinityTopologyVersion>, Set<UUID>> pendingAcks = new ConcurrentHashMap<>();

    /** Server latches collection. */
    @GridToStringInclude
    private final ConcurrentMap<T2<String, AffinityTopologyVersion>, ServerLatch> serverLatches = new ConcurrentHashMap<>();

    /** Client latches collection. */
    @GridToStringInclude
    private final ConcurrentMap<T2<String, AffinityTopologyVersion>, ClientLatch> clientLatches = new ConcurrentHashMap<>();

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

        if (!ctx.clientNode()) {
            ctx.io().addMessageListener(GridTopic.TOPIC_EXCHANGE, (nodeId, msg, plc) -> {
                if (msg instanceof LatchAckMessage)
                    processAck(nodeId, (LatchAckMessage) msg);
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
                ctx.closure().runLocalSafe(() -> processNodeLeft(e.eventNode()));
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);
        }
    }

    /**
     * Creates server latch with given {@code id} and {@code topVer}.
     * Adds corresponding pending acks to it.
     *
     * @param id Latch id.
     * @param topVer Latch topology version.
     * @param participants Participant nodes.
     * @return Server latch instance.
     */
    private Latch createServerLatch(String id, AffinityTopologyVersion topVer, Collection<ClusterNode> participants) {
        final T2<String, AffinityTopologyVersion> latchId = new T2<>(id, topVer);

        if (serverLatches.containsKey(latchId))
            return serverLatches.get(latchId);

        ServerLatch latch = new ServerLatch(id, topVer, participants);

        serverLatches.put(latchId, latch);

        if (log.isDebugEnabled())
            log.debug("Server latch is created [latch=" + latchId + ", participantsSize=" + participants.size() + "]");

        if (pendingAcks.containsKey(latchId)) {
            Set<UUID> acks = pendingAcks.get(latchId);

            for (UUID node : acks)
                if (latch.hasParticipant(node) && !latch.hasAck(node))
                    latch.ack(node);

            pendingAcks.remove(latchId);
        }

        if (latch.isCompleted())
            serverLatches.remove(latchId);

        return latch;
    }

    /**
     * Creates client latch.
     * If there is final ack corresponds to given {@code id} and {@code topVer}, latch will be completed immediately.
     *
     * @param id Latch id.
     * @param topVer Latch topology version.
     * @param coordinator Coordinator node.
     * @param participants Participant nodes.
     * @return Client latch instance.
     */
    private Latch createClientLatch(String id, AffinityTopologyVersion topVer, ClusterNode coordinator, Collection<ClusterNode> participants) {
        final T2<String, AffinityTopologyVersion> latchId = new T2<>(id, topVer);

        if (clientLatches.containsKey(latchId))
            return clientLatches.get(latchId);

        ClientLatch latch = new ClientLatch(id, topVer, coordinator, participants);

        if (log.isDebugEnabled())
            log.debug("Client latch is created [latch=" + latchId
                    + ", crd=" + coordinator
                    + ", participantsSize=" + participants.size() + "]");

        // There is final ack for created latch.
        if (pendingAcks.containsKey(latchId)) {
            latch.complete();
            pendingAcks.remove(latchId);
        }
        else
            clientLatches.put(latchId, latch);

        return latch;
    }

    /**
     * Creates new latch with specified {@code id} and {@code topVer} or returns existing latch.
     *
     * Participants of latch are calculated from given {@code topVer} as alive server nodes.
     * If local node is coordinator {@code ServerLatch} instance will be created, otherwise {@code ClientLatch} instance.
     *
     * @param id Latch id.
     * @param topVer Latch topology version.
     * @return Latch instance.
     */
    public Latch getOrCreate(String id, AffinityTopologyVersion topVer) {
        lock.lock();

        try {
            ClusterNode coordinator = getLatchCoordinator(topVer);

            if (coordinator == null) {
                ClientLatch latch = new ClientLatch(id, AffinityTopologyVersion.NONE, null, Collections.emptyList());
                latch.complete();

                return latch;
            }

            Collection<ClusterNode> participants = getLatchParticipants(topVer);

            return coordinator.isLocal()
                ? createServerLatch(id, topVer, participants)
                : createClientLatch(id, topVer, coordinator, participants);
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
     */
    private Collection<ClusterNode> aliveNodesForTopologyVer(AffinityTopologyVersion topVer) {
        if (topVer == AffinityTopologyVersion.NONE)
            return discovery.aliveServerNodes();
        else {
            DiscoCache discoCache = discovery.discoCache(topVer);

            if (discoCache != null)
                return discoCache.aliveServerNodes();
            else
                throw new IgniteException("DiscoCache not found for topology "
                        + topVer
                        + "; consider increasing IGNITE_DISCOVERY_HISTORY_SIZE property. Current value is "
                        + IgniteSystemProperties.getInteger(IgniteSystemProperties.IGNITE_DISCOVERY_HISTORY_SIZE, -1));
        }
    }

    /**
     * @param topVer Latch topology version.
     * @return Collection of alive server nodes with latch functionality.
     */
    private Collection<ClusterNode> getLatchParticipants(AffinityTopologyVersion topVer) {
        Collection<ClusterNode> aliveNodes = aliveNodesForTopologyVer(topVer);

        return aliveNodes
                .stream()
                .filter(node -> node.version().compareTo(VERSION_SINCE) >= 0)
                .collect(Collectors.toList());
    }

    /**
     * @param topVer Latch topology version.
     * @return Oldest alive server node with latch functionality.
     */
    @Nullable private ClusterNode getLatchCoordinator(AffinityTopologyVersion topVer) {
        Collection<ClusterNode> aliveNodes = aliveNodesForTopologyVer(topVer);

        return aliveNodes
                .stream()
                .filter(node -> node.version().compareTo(VERSION_SINCE) >= 0)
                .findFirst()
                .orElse(null);
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
            ClusterNode coordinator = getLatchCoordinator(AffinityTopologyVersion.NONE);

            if (coordinator == null)
                return;

            T2<String, AffinityTopologyVersion> latchId = new T2<>(message.latchId(), message.topVer());

            if (message.isFinal()) {
                if (log.isDebugEnabled())
                    log.debug("Process final ack [latch=" + latchId + ", from=" + from + "]");

                if (clientLatches.containsKey(latchId)) {
                    ClientLatch latch = clientLatches.remove(latchId);
                    latch.complete();
                }
                else if (!coordinator.isLocal()) {
                    pendingAcks.computeIfAbsent(latchId, (id) -> new GridConcurrentHashSet<>());
                    pendingAcks.get(latchId).add(from);
                }
                else if (coordinator.isLocal())
                    serverLatches.remove(latchId);
            } else {
                if (log.isDebugEnabled())
                    log.debug("Process ack [latch=" + latchId + ", from=" + from + "]");

                if (serverLatches.containsKey(latchId)) {
                    ServerLatch latch = serverLatches.get(latchId);

                    if (latch.hasParticipant(from) && !latch.hasAck(from)) {
                        latch.ack(from);

                        if (latch.isCompleted())
                            serverLatches.remove(latchId);
                    }
                }
                else {
                    pendingAcks.computeIfAbsent(latchId, (id) -> new GridConcurrentHashSet<>());
                    pendingAcks.get(latchId).add(from);
                }
            }
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * Changes coordinator to current local node.
     * Restores all server latches from pending acks and own client latches.
     */
    private void becomeNewCoordinator() {
        if (log.isInfoEnabled())
            log.info("Become new coordinator " + crd.id());

        List<T2<String, AffinityTopologyVersion>> latchesToRestore = new ArrayList<>();
        latchesToRestore.addAll(pendingAcks.keySet());
        latchesToRestore.addAll(clientLatches.keySet());

        for (T2<String, AffinityTopologyVersion> latchId : latchesToRestore) {
            String id = latchId.get1();
            AffinityTopologyVersion topVer = latchId.get2();
            Collection<ClusterNode> participants = getLatchParticipants(topVer);

            if (!participants.isEmpty())
                createServerLatch(id, topVer, participants);
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
    private void processNodeLeft(ClusterNode left) {
        assert this.crd != null : "Coordinator is not initialized";

        lock.lock();

        try {
            if (log.isDebugEnabled())
                log.debug("Process node left " + left.id());

            ClusterNode coordinator = getLatchCoordinator(AffinityTopologyVersion.NONE);

            if (coordinator == null)
                return;

            // Clear pending acks.
            for (Map.Entry<T2<String, AffinityTopologyVersion>, Set<UUID>> ackEntry : pendingAcks.entrySet())
                if (ackEntry.getValue().contains(left.id()))
                    pendingAcks.get(ackEntry.getKey()).remove(left.id());

            // Change coordinator for client latches.
            for (Map.Entry<T2<String, AffinityTopologyVersion>, ClientLatch> latchEntry : clientLatches.entrySet()) {
                ClientLatch latch = latchEntry.getValue();
                if (latch.hasCoordinator(left.id())) {
                    // Change coordinator for latch and re-send ack if necessary.
                    if (latch.hasParticipant(coordinator.id()))
                        latch.newCoordinator(coordinator);
                    else {
                        /* If new coordinator is not able to take control on the latch,
                           it means that all other latch participants are left from topology
                           and there is no reason to track such latch. */
                        AffinityTopologyVersion topVer = latchEntry.getKey().get2();

                        assert getLatchParticipants(topVer).isEmpty();

                        latch.complete(new IgniteCheckedException("All latch participants are left from topology."));
                        clientLatches.remove(latchEntry.getKey());
                    }
                }
            }

            // Add acknowledgements from left node.
            for (Map.Entry<T2<String, AffinityTopologyVersion>, ServerLatch> latchEntry : serverLatches.entrySet()) {
                ServerLatch latch = latchEntry.getValue();

                if (latch.hasParticipant(left.id()) && !latch.hasAck(left.id())) {
                    if (log.isDebugEnabled())
                        log.debug("Process node left [latch=" + latchEntry.getKey() + ", left=" + left.id() + "]");

                    latch.ack(left.id());

                    if (latch.isCompleted())
                        serverLatches.remove(latchEntry.getKey());
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
         * @param id Latch id.
         * @param topVer Latch topology version.
         * @param participants Participant nodes.
         */
        ServerLatch(String id, AffinityTopologyVersion topVer, Collection<ClusterNode> participants) {
            super(id, topVer, participants);
            this.permits = new AtomicInteger(participants.size());

            // Send final acks when latch is completed.
            this.complete.listen(f -> {
                for (ClusterNode node : participants) {
                    try {
                        if (discovery.alive(node)) {
                            io.sendToGridTopic(node, GridTopic.TOPIC_EXCHANGE, new LatchAckMessage(id, topVer, true), GridIoPolicy.SYSTEM_POOL);

                            if (log.isDebugEnabled())
                                log.debug("Final ack has sent [latch=" + latchId() + ", to=" + node.id() + "]");
                        }
                    }
                    catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Failed to send final ack [latch=" + latchId() + ", to=" + node.id() + "]: " + e.getMessage());
                    }
                }
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
         * Receives ack from given node.
         * Count downs latch if ack was not already processed.
         *
         * @param from Node.
         */
        private void ack(UUID from) {
            if (log.isDebugEnabled())
                log.debug("Ack is accepted [latch=" + latchId() + ", from=" + from + "]");

            countDown0(from);
        }

        /**
         * Count down latch from ack of given node.
         * Completes latch if all acks are received.
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

            if (remaining == 0)
                complete();
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
     * Latch creating on non-coordinator node.
     * Latch completes when final ack from coordinator is received.
     */
    class ClientLatch extends CompletableLatch {
        /** Latch coordinator node. Can be changed if coordinator is left from topology. */
        private volatile ClusterNode coordinator;

        /** Flag indicates that ack is sent to coordinator. */
        private boolean ackSent;

        /**
         * Constructor.
         *
         * @param id Latch id.
         * @param topVer Latch topology version.
         * @param coordinator Coordinator node.
         * @param participants Participant nodes.
         */
        ClientLatch(String id, AffinityTopologyVersion topVer, ClusterNode coordinator, Collection<ClusterNode> participants) {
            super(id, topVer, participants);

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
            if (log.isDebugEnabled())
                log.debug("Coordinator is changed [latch=" + latchId() + ", newCrd=" + coordinator.id() + "]");

            synchronized (this) {
                this.coordinator = coordinator;

                // Resend ack to new coordinator.
                if (ackSent)
                    sendAck();
            }
        }

        /**
         * Sends ack to coordinator node.
         * There is ack deduplication on coordinator. So it's fine to send same ack twice.
         */
        private void sendAck() {
            try {
                ackSent = true;

                io.sendToGridTopic(coordinator, GridTopic.TOPIC_EXCHANGE, new LatchAckMessage(id, topVer, false), GridIoPolicy.SYSTEM_POOL);

                if (log.isDebugEnabled())
                    log.debug("Ack has sent [latch=" + latchId() + ", to=" + coordinator.id() + "]");
            }
            catch (IgniteCheckedException e) {
                // Coordinator is unreachable. On coodinator node left discovery event ack will be resent.
                if (log.isDebugEnabled())
                    log.debug("Failed to send ack [latch=" + latchId() + ", to=" + coordinator.id() + "]: " + e.getMessage());
            }
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
        protected final String id;

        /** Latch topology version. */
        @GridToStringInclude
        protected final AffinityTopologyVersion topVer;

        /** Latch node participants. Only participant nodes are able to change state of latch. */
        @GridToStringExclude
        protected final Set<UUID> participants;

        /** Future indicates that latch is completed. */
        @GridToStringExclude
        protected final GridFutureAdapter<?> complete = new GridFutureAdapter<>();

        /**
         * Constructor.
         *
         * @param id Latch id.
         * @param topVer Latch topology version.
         * @param participants Participant nodes.
         */
        CompletableLatch(String id, AffinityTopologyVersion topVer, Collection<ClusterNode> participants) {
            this.id = id;
            this.topVer = topVer;
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
        String latchId() {
            return id + "-" + topVer;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(CompletableLatch.class, this);
        }
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(ExchangeLatchManager.class, this);
    }
}
