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

        final T2<String, AffinityTopologyVersion> latchId = new T2<>(id, topVer);

        try {
            assert !activeLatches.containsKey(latchId);

            pendingAcks.remove(latchId);

            // Send final ack.
            io.sendToGridTopic(node, GridTopic.TOPIC_LATCH, new LatchAckMessage(id, topVer, true), GridIoPolicy.SYSTEM_POOL);

            if (log.isDebugEnabled())
                log.debug("Release final ack is sent [latch=" + latchId + ", to=" + node.id() + "]");
        }
        catch (IgniteCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Unable to send release final ack [latch=" + latchId + ", to=" + node.id() + "]: " + e.getMessage());
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private Latch createServerLatch(String id, AffinityTopologyVersion topVer, Collection<ClusterNode> participants) {
        final T2<String, AffinityTopologyVersion> latchId = new T2<>(id, topVer);

        if (activeLatches.containsKey(latchId))
            return activeLatches.get(latchId);

        LatchImpl latch = new LatchImpl(id, topVer, participants);

        activeLatches.put(latchId, latch);

        if (log.isDebugEnabled())
            log.debug("Server latch is created [latch=" + latchId + ", participantsSize=" + participants.size() + "]");

        if (pendingAcks.containsKey(latchId)) {
            Set<UUID> acks = pendingAcks.get(latchId);

            for (UUID node : acks) {
                if (latch.hasParticipant(node) && !latch.hasAck(node)) {
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

    private Latch createClientLatch(String id, AffinityTopologyVersion topVer, ClusterNode coordinator, Collection<ClusterNode> participants) {
        final T2<String, AffinityTopologyVersion> latchId = new T2<>(id, topVer);

        if (proxyLatches.containsKey(latchId))
            return proxyLatches.get(latchId);

        LatchProxy latch = new LatchProxy(id, topVer, coordinator, participants);

        if (log.isDebugEnabled())
            log.debug("Client latch is created [latch=" + latchId
                    + ", crd=" + coordinator
                    + ", participantsSize=" + participants.size() + "]");

        // There is final ack for created latch.
        if (pendingAcks.containsKey(latchId)) {
            latch.complete();
            pendingAcks.remove(latchId);
        }
        else {
            proxyLatches.put(latchId, latch);
        }

        return latch;
    }

    public Latch getOrCreate(String id, AffinityTopologyVersion topVer) {
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

            if (coordinator.isLocal())
                return createServerLatch(id, topVer, participants);
            else
                return createClientLatch(id, topVer, coordinator, participants);
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
                if (log.isDebugEnabled())
                    log.debug("Process final ack [latch=" + latchId + ", from=" + from + "]");

                if (proxyLatches.containsKey(latchId)) {
                    LatchProxy latch = proxyLatches.remove(latchId);
                    latch.complete();
                }
                else if (!coordinator.isLocal()) {
                    pendingAcks.computeIfAbsent(latchId, (id) -> new GridConcurrentHashSet<>());
                    pendingAcks.get(latchId).add(from);
                }
            } else {
                if (log.isDebugEnabled())
                    log.debug("Process ack [latch=" + latchId + ", from=" + from + "]");

                if (activeLatches.containsKey(latchId)) {
                    LatchImpl latch = activeLatches.get(latchId);

                    if (latch.hasParticipant(from) && !latch.hasAck(from)) {
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
        if (log.isInfoEnabled())
            log.info("Become new coordinator " + coordinator.id());

        List<T2<String, AffinityTopologyVersion>> latchesToRestore = new ArrayList<>();
        // Restore latches from pending acks and own proxy latches.
        latchesToRestore.addAll(pendingAcks.keySet());
        latchesToRestore.addAll(proxyLatches.keySet());

        for (T2<String, AffinityTopologyVersion> latchId : latchesToRestore) {
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
            if (log.isDebugEnabled())
                log.debug("Process node left " + left.id());

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

                if (latch.hasParticipant(left.id()) && !latch.hasAck(left.id())) {
                    if (log.isDebugEnabled())
                        log.debug("Process node left [latch=" + latchEntry.getKey() + ", left=" + left.id() + "]");

                    latch.ack(left.id());
                    latch.countDown();

                    if (latch.isCompleted())
                        activeLatches.remove(latchEntry.getKey());
                }
            }

            // Coordinator is changed.
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

        LatchImpl(String id, AffinityTopologyVersion topVer, Collection<ClusterNode> participants) {
            super(id, topVer, participants);
            this.permits = new AtomicInteger(participants.size());

            this.complete.listen(f -> {
                // Send final acks.
                for (ClusterNode node : participants) {
                    try {
                        if (discovery.alive(node)) {
                            io.sendToGridTopic(node, GridTopic.TOPIC_LATCH, new LatchAckMessage(id, topVer, true), GridIoPolicy.SYSTEM_POOL);

                            if (log.isDebugEnabled())
                                log.debug("Final ack is sent [latch=" + printableId() + ", to=" + node.id() + "]");
                        }
                    } catch (IgniteCheckedException e) {
                        if (log.isDebugEnabled())
                            log.debug("Unable to send final ack [latch=" + printableId() + ", to=" + node.id() + "]");
                    }
                }
            });
        }

        boolean hasAck(UUID from) {
            return acks.contains(from);
        }

        void ack(UUID from) {
            if (log.isDebugEnabled())
                log.debug("Ack is accepted [latch=" + printableId() + ", from=" + from + "]");

            acks.add(from);
        }

        @Override public void countDown() {
            if (isCompleted())
                return;

            int remaining = permits.decrementAndGet();

            if (log.isDebugEnabled())
                log.debug("Count down + [latch=" + printableId() + ", remaining=" + remaining + "]");

            if (remaining == 0)
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

            if (log.isDebugEnabled())
                log.debug("Coordinator is changed [latch=" + printableId() + ", crd=" + coordinator.id() + "]");

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

                if (log.isDebugEnabled())
                    log.debug("Ack is sent + [latch=" + printableId() + ", to=" + coordinator.id() + "]");
            } catch (IgniteCheckedException e) {
                if (log.isDebugEnabled())
                    log.debug("Unable to send ack [latch=" + printableId() + ", to=" + coordinator.id() + "]: " + e.getMessage());
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

        String printableId() {
            return id + "-" + topVer;
        }

        @Override public void await() throws IgniteCheckedException {
            complete.get();
        }

        @Override public void await(long timeout, TimeUnit timeUnit) throws IgniteCheckedException {
            complete.get(timeout, timeUnit);
        }
    }
}
