package org.apache.ignite.internal.processors.cache.datastructures.latch;

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
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.GridDiscoveryManager;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.typedef.T2;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

public class LatchManager {

    private final IgniteLogger log;

    private final GridDiscoveryManager discovery;

    private final GridIoManager io;

    private volatile ClusterNode coordinator;

    private final ConcurrentMap<T2<String, AffinityTopologyVersion>, Set<UUID>> pendingAcks = new ConcurrentHashMap<>();

    private final ConcurrentMap<T2<String, AffinityTopologyVersion>, LatchImpl> activeLatches = new ConcurrentHashMap<>();

    private final ConcurrentMap<T2<String, AffinityTopologyVersion>, LatchProxy> proxyLatches = new ConcurrentHashMap<>();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public LatchManager(GridKernalContext ctx) {
        this.log = ctx.log(getClass());
        this.discovery = ctx.discovery();
        this.io = ctx.io();

        if (!ctx.clientNode()) {
            ctx.io().addMessageListener(GridTopic.TOPIC_LATCH, (nodeId, msg, plc) -> {
                if (msg instanceof LatchAckMessage) {
                    processAck(nodeId, (LatchAckMessage) msg);
                }
            });

            ctx.event().addDiscoveryEventListener((e, cache) -> {
                assert e != null;
                assert e.type() == EVT_NODE_LEFT || e.type() == EVT_NODE_FAILED : this;

                processNodeLeft(e.eventNode());
            }, EVT_NODE_LEFT, EVT_NODE_FAILED);
        }
    }

    public void release(String id, AffinityTopologyVersion topVer, ClusterNode node) {
        lock.writeLock().lock();

        try {
            final T2<String, AffinityTopologyVersion> latchId = new T2<>(id, topVer);

            assert !activeLatches.containsKey(latchId);

            pendingAcks.remove(latchId);

            // Send final ack.
            io.sendToGridTopic(node, GridTopic.TOPIC_LATCH, new LatchAckMessage(id, topVer, true), GridIoPolicy.SYSTEM_POOL);

            log.warning("Final ack sent to " + node.id() + " " + id);
        }
        catch (IgniteCheckedException e) {
            log.warning("Unable to release latch on " + node.id(), e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private Latch createServerLatch(String id, AffinityTopologyVersion topVer, Collection<ClusterNode> participants) {
        final T2<String, AffinityTopologyVersion> latchId = new T2<>(id, topVer);

        if (activeLatches.containsKey(latchId))
            return activeLatches.get(latchId);

        LatchImpl latch = new LatchImpl(id, topVer, participants, new AtomicInteger(participants.size()));

        activeLatches.put(latchId, latch);

        log.warning("Server latch is created " + id + " " + topVer + " " + participants.size());

        if (pendingAcks.containsKey(latchId)) {
            Set<UUID> acks = pendingAcks.get(latchId);

            for (UUID node : acks) {
                if (latch.hasParticipant(node) && !latch.hasAckFrom(node)) {
                    latch.ack(node);
                    latch.countDown();
                }
            }

            pendingAcks.remove(latchId);
        }

        if (latch.isCompleted()) {
            activeLatches.remove(latchId);
        }

        return latch;
    }

    public Latch getOrCreate(String id, AffinityTopologyVersion topVer) {
        final T2<String, AffinityTopologyVersion> latchId = new T2<>(id, topVer);

        lock.writeLock().lock();

        try {
            ClusterNode coordinator = discovery.discoCache(topVer).oldestAliveServerNode();

            if (this.coordinator == null)
                this.coordinator = coordinator;

            if (coordinator == null) {
                LatchProxy latch = new LatchProxy(id, null, null, null);
                latch.complete();

                return latch;
            }

            Collection<ClusterNode> participants = discovery.discoCache(topVer).aliveServerNodes();

            if (coordinator.isLocal()) {
                return createServerLatch(id, topVer, participants);
            }
            else {
                if (proxyLatches.containsKey(latchId))
                    return proxyLatches.get(latchId);

                LatchProxy latch = new LatchProxy(id, topVer, coordinator, participants);

                log.warning("Client latch was created " + id + " " + topVer + " " + participants);

                // There is final ack for already created latch.
                if (pendingAcks.containsKey(latchId)) {
                    latch.complete();
                    pendingAcks.remove(latchId);
                }
                else {
                    proxyLatches.put(latchId, latch);
                }

                return latch;
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private void processAck(UUID from, LatchAckMessage message) {
        lock.writeLock().lock();

        try {
            ClusterNode coordinator = discovery.oldestAliveServerNode(AffinityTopologyVersion.NONE);

            if (coordinator == null)
                return;

            T2<String, AffinityTopologyVersion> latchId = new T2<>(message.latchId(), message.topVer());

            if (message.isFinal()) {
                log.warning("Process final ack from " + coordinator.id() + " " + latchId);

                if (proxyLatches.containsKey(latchId)) {
                    LatchProxy latch = proxyLatches.remove(latchId);
                    latch.complete();
                }
                else if (!coordinator.isLocal()) {
                    pendingAcks.computeIfAbsent(latchId, (id) -> new GridConcurrentHashSet<>());
                    pendingAcks.get(latchId).add(from);
                }
            } else {
                log.warning("Process ack from " + from + " " + latchId);

                if (activeLatches.containsKey(latchId)) {
                    LatchImpl latch = activeLatches.get(latchId);

                    if (latch.hasParticipant(from) && !latch.hasAckFrom(from)) {
                        latch.ack(from);
                        latch.countDown();

                        if (latch.isCompleted())
                            activeLatches.remove(latchId);
                    }
                }
                else {
                    pendingAcks.computeIfAbsent(latchId, (id) -> new GridConcurrentHashSet<>());
                    pendingAcks.get(latchId).add(from);
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private void becomeNewCoordinator() {
        log.warning("Become new coordinator.");

        // Restore latches from pending acks.
        for (T2<String, AffinityTopologyVersion> latchId : pendingAcks.keySet()) {
            String id = latchId.get1();

            AffinityTopologyVersion topVer = latchId.get2();

            Collection<ClusterNode> participants = discovery.discoCache(topVer).aliveServerNodes();

            if (!participants.isEmpty()) {
                createServerLatch(id, topVer, participants);
            }
        }

        // Restore latches from current proxy latches.
        for (T2<String, AffinityTopologyVersion> latchId : proxyLatches.keySet()) {
            String id = latchId.get1();

            AffinityTopologyVersion topVer = latchId.get2();

            Collection<ClusterNode> participants = discovery.discoCache(topVer).aliveServerNodes();

            if (!participants.isEmpty()) {
                createServerLatch(id, topVer, participants);
            }
        }
    }

    private void processNodeLeft(ClusterNode left) {
        lock.writeLock().lock();

        try {
            log.warning("Process node left " + left.id());

            ClusterNode coordinator = discovery.oldestAliveServerNode(AffinityTopologyVersion.NONE);

            assert coordinator != null;

            // Clear pending acks.
            for (Map.Entry<T2<String, AffinityTopologyVersion>, Set<UUID>> ackEntry : pendingAcks.entrySet()) {
                if (ackEntry.getValue().contains(left.id())) {
                    pendingAcks.get(ackEntry.getKey()).remove(left.id());
                }
            }

            // Change coordinators for proxy latches.
            for (Map.Entry<T2<String, AffinityTopologyVersion>, LatchProxy> latchEntry : proxyLatches.entrySet()) {
                LatchProxy latch = latchEntry.getValue();
                if (latch.hasCoordinator(left.id())) {
                    // Change coordinator for latch and re-send ack if necessary.
                    if (latch.hasParticipant(coordinator.id())) {
                        latch.newCoordinator(coordinator);
                    }
                    else {
                        latch.complete(new IgniteCheckedException("Coordinator is left from topology."));
                    }
                }
            }

            // Add acknowledgements for left node.
            for (Map.Entry<T2<String, AffinityTopologyVersion>, LatchImpl> latchEntry : activeLatches.entrySet()) {
                LatchImpl latch = latchEntry.getValue();

                if (latch.hasParticipant(left.id()) && !latch.hasAckFrom(left.id())) {
                    log.warning("[SERVER] Process node left " + left.id() + " " + latch.id);

                    latch.ack(left.id());
                    latch.countDown();

                    if (latch.isCompleted())
                        activeLatches.remove(latchEntry.getKey());
                }
            }

            // Coordinator was changed.
            if (coordinator.isLocal() && this.coordinator.id() != coordinator.id()) {
                this.coordinator = coordinator;

                becomeNewCoordinator();
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    class LatchImpl extends CompletableLatch {
        private final AtomicInteger permits;

        private final Set<UUID> acks = new GridConcurrentHashSet<>();

        LatchImpl(String id, AffinityTopologyVersion topVer, Collection<ClusterNode> participants, AtomicInteger permits) {
            super(id, topVer, participants);
            this.permits = permits;

            this.complete.listen(f -> {
                for (ClusterNode node : participants) {
/*
                    if (node.isLocal())
                        continue;
*/

                    // Send final acks.
                    try {
                        io.sendToGridTopic(node, GridTopic.TOPIC_LATCH, new LatchAckMessage(id, topVer, true), GridIoPolicy.SYSTEM_POOL);
                        log.warning("Final ack is sent to " + node.id() + " " + id);
                    } catch (IgniteCheckedException e) {
                        // Ignore
                    }
                }
            });
        }

        boolean hasAckFrom(UUID nodeId) {
            return acks.contains(nodeId);
        }

        void ack(UUID nodeId) {
            acks.add(nodeId);
        }

        @Override public void countDown() {
            if (isCompleted())
                return;

            int done = permits.decrementAndGet();

            log.warning("Count down " + id + " " + done);

            if (done == 0)
                complete();
        }
    }

    class LatchProxy extends CompletableLatch {
        private volatile ClusterNode coordinator;

        private boolean sent = false;

        LatchProxy(String id, AffinityTopologyVersion topVer, ClusterNode coordinator, Collection<ClusterNode> participants) {
            super(id, topVer, participants);

            this.coordinator = coordinator;
        }

        boolean hasCoordinator(UUID nodeId) {
            return coordinator.id().equals(nodeId);
        }

        void newCoordinator(ClusterNode coordinator) {
            this.coordinator = coordinator;

            synchronized (this) {
                if (sent) {
                    sendAck();
                }
            }
        }

        private void sendAck() {
            try {
                sent = true;

                io.sendToGridTopic(coordinator, GridTopic.TOPIC_LATCH, new LatchAckMessage(id, topVer, false), GridIoPolicy.SYSTEM_POOL);

                log.warning("Ack is sent to " + coordinator.id());
            } catch (IgniteCheckedException e) {
                log.warning("Unable to send ack to " + coordinator);
            }
        }

        @Override public void countDown() {
            if (isCompleted())
                return;

            synchronized (this) {
                sendAck();
            }
        }
    }

    private abstract class CompletableLatch implements Latch {
        protected final String id;

        protected final AffinityTopologyVersion topVer;

        protected final Set<UUID> participants;

        protected final GridFutureAdapter<?> complete = new GridFutureAdapter<>();

        CompletableLatch(String id, AffinityTopologyVersion topVer, Collection<ClusterNode> participants) {
            this.id = id;
            this.topVer = topVer;
            this.participants = participants.stream().map(ClusterNode::id).collect(Collectors.toSet());
        }

        boolean hasParticipant(UUID nodeId) {
            return participants.contains(nodeId);
        }

        boolean isCompleted() {
            return complete.isDone();
        }

        void complete() {
            complete.onDone();
        }

        void complete(Throwable error) {
            complete.onDone(error);
        }

        @Override public void await() throws IgniteCheckedException {
            complete.get();
        }

        @Override public void await(long timeout, TimeUnit timeUnit) throws IgniteCheckedException {
            complete.get(timeout, timeUnit);
        }
    }
}
