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
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;

public class LatchManager {

    private final IgniteLogger log;

    private final GridDiscoveryManager discovery;

    private final GridIoManager io;

    private final ConcurrentMap<Object, List<ClusterNode>> pendingAcks = new ConcurrentHashMap<>();

    private final ConcurrentMap<Object, LatchImpl> activeLatches = new ConcurrentHashMap<>();

    private final ConcurrentMap<Object, LatchProxy> proxyLatches = new ConcurrentHashMap<>();

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

    public void release(String id, ClusterNode node) {
        lock.writeLock().lock();

        try {
            assert !activeLatches.containsKey(id);

            pendingAcks.remove(id);

            // Send final ack.
            io.sendToGridTopic(node, GridTopic.TOPIC_LATCH, new LatchAckMessage(id, true), GridIoPolicy.SYSTEM_POOL);

            log.warning("Final ack sent to " + node.id() + " " + id);
        }
        catch (IgniteCheckedException e) {
            log.warning("Unable to release latch on " + node.id(), e);
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    public Latch getOrCreate(String id, AffinityTopologyVersion topVer) {
        lock.writeLock().lock();

        try {
            ClusterNode coordinator = discovery.oldestAliveServerNode(topVer);

            if (coordinator == null) {
                LatchProxy latch = new LatchProxy(id, null, null);
                latch.complete();

                return latch;
            }

            Collection<ClusterNode> participants = discovery.discoCache(topVer).aliveServerNodes();

            if (coordinator.isLocal()) {
                if (activeLatches.containsKey(id))
                    return activeLatches.get(id);

                LatchImpl latch = new LatchImpl(id, participants, new AtomicInteger(participants.size()));

                activeLatches.put(id, latch);

                if (pendingAcks.containsKey(id)) {
                    List<ClusterNode> acks = pendingAcks.get(id);

                    for (ClusterNode node : acks) {
                        if (latch.hasParticipant(node)) {
                            latch.countDown();
                        }
                    }

                    pendingAcks.remove(id);
                }

                return latch;
            }
            else {
                if (proxyLatches.containsKey(id))
                    return proxyLatches.get(id);

                LatchProxy latch = new LatchProxy(id, coordinator, participants);

                // There is final ack for already created latch.
                if (pendingAcks.containsKey(id)) {
                    latch.complete();
                    pendingAcks.remove(id);
                }
                else {
                    proxyLatches.put(id, latch);
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

        ClusterNode coordinator = discovery.oldestAliveServerNode(AffinityTopologyVersion.NONE);

        if (coordinator == null)
            return;

        try {
            ClusterNode node = discovery.node(from);

            Object latchId = message.latchId();

            if (coordinator.isLocal()) {
                assert !message.isFinal();

                log.warning("Process ack from " + from + " " + latchId);

                if (activeLatches.containsKey(latchId)) {
                    LatchImpl latch = activeLatches.get(latchId);

                    if (latch.hasParticipant(node)) {
                        latch.countDown();

                        if (latch.isCompleted())
                            activeLatches.remove(latchId);
                    }
                }
                else {
                    pendingAcks.computeIfAbsent(latchId, (id) -> new ArrayList<>());
                    pendingAcks.get(latchId).add(node);
                }
            }
            else {
                assert message.isFinal();

                log.warning("Process final ack from " + coordinator.id() + " " + latchId);

                if (proxyLatches.containsKey(latchId)) {
                    LatchProxy latch = proxyLatches.remove(latchId);
                    latch.complete();
                }
                else {
                    pendingAcks.put(latchId, Collections.singletonList(node));
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    private void processNodeLeft(ClusterNode node) {
        lock.writeLock().lock();

        try {
            log.warning("Process node left " + node.id());

            for (Map.Entry<Object, List<ClusterNode>> ackEntry : pendingAcks.entrySet()) {
                if (ackEntry.getValue().contains(node))
                    pendingAcks.remove(ackEntry.getKey());
            }

            for (Map.Entry<Object, LatchImpl> latchEntry : activeLatches.entrySet()) {
                LatchImpl latch = latchEntry.getValue();
                if (latch.hasParticipant(node)) {
                    log.warning("[SERVER] Process node left " + node.id() + " " + latch.id);

                    latch.countDown();

                    if (latch.isCompleted())
                        activeLatches.remove(latchEntry.getKey());
                }
            }

            for (Map.Entry<Object, LatchProxy> latchEntry : proxyLatches.entrySet()) {
                LatchProxy latch = latchEntry.getValue();
                if (latch.hasCoordinator(node)) {
                    log.warning("[CLIENT] Process node left " + node.id() + " " + latch.id);

                    latch.complete(new IgniteCheckedException("Latch coordinator is left from topology"));

                    proxyLatches.remove(latchEntry.getKey());
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    class LatchImpl extends CompletableLatch {
        private final AtomicInteger permits;

        public LatchImpl(String id, Collection<ClusterNode> participants, AtomicInteger permits) {
            super(id, participants);
            this.permits = permits;

            this.complete.listen(f -> {
                for (ClusterNode node : participants) {
                    if (node.isLocal())
                        continue;

                    // Send final acks.
                    try {
                        io.sendToGridTopic(node, GridTopic.TOPIC_LATCH, new LatchAckMessage(id, true), GridIoPolicy.SYSTEM_POOL);
                        log.warning("Final ack is sent to " + node.id() + " " + id);
                    } catch (IgniteCheckedException e) {
                        // Ignore
                    }
                }
            });
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
        private final ClusterNode coordinator;

        public LatchProxy(String id, ClusterNode coordinator, Collection<ClusterNode> participants) {
            super(id, participants);

            this.coordinator = coordinator;
        }

        public boolean hasCoordinator(ClusterNode node) {
            return coordinator.id().equals(node.id());
        }

        @Override public void countDown() {
            if (isCompleted())
                return;

            try {
                io.sendToGridTopic(coordinator, GridTopic.TOPIC_LATCH, new LatchAckMessage(id, false), GridIoPolicy.SYSTEM_POOL);
            } catch (IgniteCheckedException e) {
                complete(e);
            }
        }
    }

    private abstract class CompletableLatch implements Latch {
        protected final String id;

        protected final Set<UUID> participants;

        protected final GridFutureAdapter<?> complete = new GridFutureAdapter<>();

        public CompletableLatch(String id, Collection<ClusterNode> participants) {
            this.id = id;
            this.participants = participants.stream().map(ClusterNode::id).collect(Collectors.toSet());
        }

        public boolean hasParticipant(ClusterNode node) {
            return participants.contains(node.id());
        }

        public boolean isCompleted() {
            return complete.isDone();
        }

        public void complete() {
            complete.onDone();
        }

        public void complete(Throwable error) {
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
