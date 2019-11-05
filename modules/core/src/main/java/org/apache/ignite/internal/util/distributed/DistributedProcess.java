package org.apache.ignite.internal.util.distributed;

import java.util.HashSet;
import java.util.Map;
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
 * Provides logic for a distributed process:
 * <ul>
 *  <li>1. Initial discovery message starts process.</li>
 *  <li>2. Each server node process it and send the result via the communication message to the coordinator.
 *  See {@link #process}.</li>
 *  <li>3. The coordinator processes all single nodes results and sends the action message via discovery.</li>
 * </ul>
 * Several processes can be started at the same time. Processes are identified by request id.
 * Follow methods used to manage process:
 * {@link #onSingleResultReceived}
 * {@link #onAllReceived}
 * {@link #onActionMessage}
 * {@link #onAllServersLeft}
 *
 * @param <I> Type of initial message.
 * @param <R> Type of single nodes result message.
 * @param <A> Type of action message.
 */
public abstract class DistributedProcess<I extends DistributedProcessInitialMessage,
    R extends DistributedProcessSingleNodeMessage, A extends DistributedProcessActionMessage> {
    /** Map of all active processes. */
    private final ConcurrentHashMap</*processId*/UUID, Process> processes = new ConcurrentHashMap<>(1);

    /** Synchronization mutex for coordinator initializing and the remaining collection operations. */
    private final Object mux = new Object();

    /** Kernal context. */
    private GridKernalContext ctx;

    /** Logger. */
    private IgniteLogger log;

    /**
     * Processes initial discovery message. Called on each server node.
     * <p>
     * Note: called from discovery thread.
     *
     * @param msg Initial discovery message {@link DistributedProcessInitialMessage}.
     * @param topVer Current topology version.
     * @return Future with single nodes result {@link DistributedProcessSingleNodeMessage}.
     */
    protected abstract IgniteInternalFuture<R> process(I msg, AffinityTopologyVersion topVer);

    /**
     * Initiates process's listeners. Should be called before the discovery manager started.
     *
     * @param ctx     Kernal context.
     * @param initCls Class of the initial message {@link DistributedProcessInitialMessage}.
     * @param resCls  Class of the single nodes result message {@link DistributedProcessSingleNodeMessage}.
     * @param topic   Grid topic of result message.
     * @param actCls  Class of the action message {@link DistributedProcessActionMessage}.
     */
    public void init(GridKernalContext ctx, Class<I> initCls, Class<R> resCls, GridTopic topic, Class<A> actCls) {
        this.ctx = ctx;

        log = ctx.log(getClass());

        ctx.discovery().setCustomEventListener(initCls, (topVer, snd, msg) -> {
            Process proc = processes.computeIfAbsent(msg.requestId(), id -> new Process(msg.requestId()));

            if (proc.initFut.isDone())
                return;

            ClusterNode crd = coordinator();

            if (crd == null) {
                proc.initFut.onDone();

                onAllServersLeft(msg.requestId());

                return;
            }

            proc.crdId = crd.id();

            if (crd.isLocal())
                initCoordinator(topVer, proc);

            IgniteInternalFuture<R> fut = process(msg, topVer);

            fut.listen(f -> {
                try {
                    R res = f.get();

                    proc.singleResFut.onDone(res);

                    if (res == null)
                        return;

                    ClusterNode crdNode = coordinator();

                    if (crdNode != null) {
                        if (crdNode.isLocal())
                            onSingleNodeMessageReceived(res, crdNode.id());
                        else
                            ctx.io().sendToGridTopic(crdNode, topic, res, SYSTEM_POOL);
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

        ctx.io().addMessageListener(topic, (nodeId, msg0, plc) -> {
            if (resCls.isAssignableFrom(msg0.getClass())) {
                R msg = (R)msg0;

                onSingleNodeMessageReceived(msg, nodeId);
            }
        });

        ctx.discovery().setCustomEventListener(actCls, (topVer, snd, msg) -> {
            onActionMessage(msg);

            processes.remove(msg.requestId());
        });

        ctx.event().addDiscoveryEventListener((evt, discoCache) -> {
            UUID leftNodeId = evt.eventNode().id();

            for (Process proc : processes.values()) {
                proc.initFut.listen(fut -> {
                    boolean crdChanged = F.eq(leftNodeId, proc.crdId);

                    if (crdChanged) {
                        ClusterNode crd = coordinator();

                        if (crd != null) {
                            proc.crdId = crd.id();

                            if (crd.isLocal())
                                initCoordinator(discoCache.version(), proc);

                            proc.singleResFut.listen(f -> {
                                try {
                                    R res = f.get();

                                    if (res == null)
                                        return;

                                    if (crd.isLocal())
                                        onSingleNodeMessageReceived(res, crd.id());
                                    else
                                        ctx.io().sendToGridTopic(crd, topic, res, SYSTEM_POOL);
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

                            if (isEmpty) {
                                onAllReceived(proc.results);

                                processes.remove(proc.id);
                            }
                        }
                    }
                });
            }
        }, EVT_NODE_FAILED, EVT_NODE_LEFT);
    }

    /**
     * Called when all single nodes results received.
     *
     * @param res Map of single nodes results.
     */
    protected void onAllReceived(Map<UUID, R> res) {
        // No-op.
    }

    /**
     * Called when single node result received.
     *
     * @param nodeId Node id.
     * @param msg    Single node result.
     */
    protected void onSingleResultReceived(UUID nodeId, R msg) {
        // No-op.
    }

    /**
     * Called when action message received.
     *
     * @param msg Action message.
     */
    protected void onActionMessage(A msg) {
        // No-op.
    }

    /**
     * Sends action message.
     *
     * @param msg Action message.
     */
    public void sendAction(A msg) {
        try {
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
    private void onSingleNodeMessageReceived(R msg, UUID nodeId) {
        Process proc = processes.computeIfAbsent(msg.requestId(), id -> new Process(msg.requestId()));

        proc.initCrdFut.listen(f -> {
            boolean rmvd, isEmpty;

            synchronized (mux) {
                rmvd = proc.remaining.remove(nodeId);

                isEmpty = proc.remaining.isEmpty();
            }

            if (rmvd) {
                proc.results.put(nodeId, msg);

                onSingleResultReceived(nodeId, msg);

                if (isEmpty) {
                    onAllReceived(proc.results);

                    processes.remove(msg.requestId());
                }
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

        /** Init process future. */
        private final GridFutureAdapter<Void> initFut = new GridFutureAdapter<>();

        /** Init coordinator future. */
        private final GridFutureAdapter<Void> initCrdFut = new GridFutureAdapter<>();

        /** Coordinator id. */
        private volatile UUID crdId;

        /** Future of single local node result. */
        private final GridFutureAdapter<R> singleResFut = new GridFutureAdapter<>();

        /** Remaining nodes to received single result message. */
        private final Set</*nodeId*/UUID> remaining = new GridConcurrentHashSet<>();

        /** Single nodes results. */
        private final ConcurrentHashMap</*nodeId*/UUID, R> results = new ConcurrentHashMap<>();

        /** @param id Process id. */
        private Process(UUID id) {
            this.id = id;
        }
    }
}
