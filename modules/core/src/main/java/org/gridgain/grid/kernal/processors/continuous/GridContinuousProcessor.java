/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.processors.continuous;

import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.managers.communication.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.kernal.managers.eventstorage.*;
import org.gridgain.grid.kernal.processors.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.marshaller.*;
import org.gridgain.grid.thread.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.util.future.*;
import org.gridgain.grid.util.tostring.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.worker.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.events.GridEventType.*;
import static org.gridgain.grid.kernal.GridTopic.*;
import static org.gridgain.grid.kernal.managers.communication.GridIoPolicy.*;
import static org.gridgain.grid.kernal.processors.continuous.GridContinuousMessageType.*;

/**
 * Processor for continuous routines.
 */
public class GridContinuousProcessor extends GridProcessorAdapter {
    /** Local infos. */
    private final ConcurrentMap<UUID, LocalRoutineInfo> locInfos = new ConcurrentHashMap8<>();

    /** Remote infos. */
    private final ConcurrentMap<UUID, RemoteRoutineInfo> rmtInfos = new ConcurrentHashMap8<>();

    /** Start futures. */
    private final ConcurrentMap<UUID, StartFuture> startFuts = new ConcurrentHashMap8<>();

    /** Start ack wait lists. */
    private final ConcurrentMap<UUID, Collection<UUID>> waitForStartAck = new ConcurrentHashMap8<>();

    /** Stop futures. */
    private final ConcurrentMap<UUID, StopFuture> stopFuts = new ConcurrentHashMap8<>();

    /** Stop ack wait lists. */
    private final ConcurrentMap<UUID, Collection<UUID>> waitForStopAck = new ConcurrentHashMap8<>();

    /** Threads started by this processor. */
    private final Collection<GridThread> threads = new GridConcurrentHashSet<>();

    /** Pending start requests. */
    private final Map<UUID, Collection<GridContinuousMessage>> pending = new HashMap<>();

    /** Stopped IDs. */
    private final Collection<UUID> stopped = new HashSet<>();

    /** Lock for pending requests. */
    private final Lock pendingLock = new ReentrantLock();

    /** Lock for stop process. */
    private final Lock stopLock = new ReentrantLock();

    /** Delay in milliseconds between retries. */
    private long retryDelay = 1000;

    /** Number of retries using to send messages. */
    private int retryCnt = 3;

    /** Acknowledgement timeout. */
    private long ackTimeout;

    /** Marshaller. */
    private GridMarshaller marsh;

    /**
     * @param ctx Kernal context.
     */
    public GridContinuousProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws GridException {
        if (ctx.config().isDaemon())
            return;

        retryDelay = ctx.config().getNetworkSendRetryDelay();
        retryCnt = ctx.config().getNetworkSendRetryCount();
        ackTimeout = ctx.config().getNetworkTimeout();

        if (ackTimeout < retryDelay * retryCnt) {
            U.warn(log, "Acknowledgement timeout for continuous operations is less than message send " +
                "retry delay multiplied by retries count (will increase timeout value) [ackTimeout=" +
                ackTimeout + ", retryDelay=" + retryDelay + ", retryCnt=" + retryCnt + ']');

            ackTimeout = retryDelay * retryCnt;
        }

        marsh = ctx.config().getMarshaller();

        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @SuppressWarnings({"fallthrough", "TooBroadScope"})
            @Override public void onEvent(GridEvent evt) {
                assert evt instanceof GridDiscoveryEvent;

                UUID nodeId = ((GridDiscoveryEvent)evt).eventNode().id();

                Collection<GridContinuousMessage> reqs;

                pendingLock.lock();

                try {
                    // Remove pending requests to send to joined node
                    // (if node is left or failed, they are dropped).
                    reqs = pending.remove(nodeId);
                }
                finally {
                    pendingLock.unlock();
                }

                switch (evt.type()) {
                    case EVT_NODE_JOINED:
                        if (reqs != null) {
                            UUID routineId = null;

                            // Send pending requests.
                            try {
                                for (GridContinuousMessage req : reqs) {
                                    routineId = req.routineId();

                                    sendWithRetries(nodeId, req, null);
                                }
                            }
                            catch (GridTopologyException ignored) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to send pending start request to node (is node alive?): " +
                                        nodeId);
                            }
                            catch (GridException e) {
                                U.error(log, "Failed to send pending start request to node: " + nodeId, e);

                                completeStartFuture(routineId);
                            }
                        }

                        break;

                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:
                        // Do not wait for start acknowledgements from left node.
                        for (Map.Entry<UUID, Collection<UUID>> e : waitForStartAck.entrySet()) {
                            Collection<UUID> nodeIds = e.getValue();

                            for (Iterator<UUID> it = nodeIds.iterator(); it.hasNext();) {
                                if (nodeId.equals(it.next())) {
                                    it.remove();

                                    break;
                                }
                            }

                            if (nodeIds.isEmpty())
                                completeStartFuture(e.getKey());
                        }

                        // Do not wait for stop acknowledgements from left node.
                        for (Map.Entry<UUID, Collection<UUID>> e : waitForStopAck.entrySet()) {
                            Collection<UUID> nodeIds = e.getValue();

                            for (Iterator<UUID> it = nodeIds.iterator(); it.hasNext();) {
                                if (nodeId.equals(it.next())) {
                                    it.remove();

                                    break;
                                }
                            }

                            if (nodeIds.isEmpty())
                                completeStopFuture(e.getKey());
                        }

                        // Unregister handlers created by left node.
                        for (Map.Entry<UUID, RemoteRoutineInfo> e : rmtInfos.entrySet()) {
                            UUID routineId = e.getKey();
                            RemoteRoutineInfo info = e.getValue();

                            if (info.autoUnsubscribe && nodeId.equals(info.nodeId))
                                unregisterRemote(routineId);
                        }

                        break;

                    default:
                        assert false : "Unexpected event received: " + evt.shortDisplay();
                }
            }
        }, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED);

        ctx.io().addMessageListener(TOPIC_CONTINUOUS, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object obj) {
                GridContinuousMessage msg = (GridContinuousMessage)obj;

                if (msg.data() == null && msg.dataBytes() != null) {
                    try {
                        msg.data(marsh.unmarshal(msg.dataBytes(), null));
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to process message (ignoring): " + msg, e);

                        return;
                    }
                }

                switch (msg.type()) {
                    case MSG_START_REQ:
                        processStartRequest(nodeId, msg);

                        break;

                    case MSG_START_ACK:
                        processStartAck(nodeId, msg);

                        break;

                    case MSG_STOP_REQ:
                        processStopRequest(nodeId, msg);

                        break;

                    case MSG_STOP_ACK:
                        processStopAck(nodeId, msg);

                        break;

                    case MSG_EVT_NOTIFICATION:
                        processNotification(nodeId, msg);

                        break;

                    default:
                        assert false : "Unexpected message received: " + msg.type();
                }
            }
        });

        if (log.isDebugEnabled())
            log.debug("Continuous processor started.");
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws GridException {
        if (ctx.config().isDaemon())
            return;

        ctx.io().removeMessageListener(TOPIC_CONTINUOUS);

        U.interrupt(threads);
        U.joinThreads(threads, log);

        if (log.isDebugEnabled())
            log.debug("Continuous processor stopped.");
    }

    /** {@inheritDoc} */
    @Override @Nullable public Object collectDiscoveryData(UUID nodeId) {
        if (!nodeId.equals(ctx.localNodeId())) {
            pendingLock.lock();

            try {
                // Create empty pending set.
                pending.put(nodeId, new HashSet<GridContinuousMessage>());

                DiscoveryData data = new DiscoveryData(ctx.localNodeId());

                // Collect listeners information (will be sent to
                // joining node during discovery process).
                for (Map.Entry<UUID, LocalRoutineInfo> e : locInfos.entrySet()) {
                    UUID routineId = e.getKey();
                    LocalRoutineInfo info = e.getValue();

                    data.addItem(new DiscoveryDataItem(routineId, info.prjPred,
                        info.hnd, info.bufSize, info.interval));
                }

                return data;
            }
            finally {
                pendingLock.unlock();
            }
        }
        else
            return null;
    }

    /** {@inheritDoc} */
    @Override public void onDiscoveryDataReceived(Object obj) {
        DiscoveryData data = (DiscoveryData)obj;

        if (!ctx.isDaemon() && data != null) {
            for (DiscoveryDataItem item : data.items) {
                // Register handler only if local node passes projection predicate.
                if (item.prjPred == null || item.prjPred.apply(ctx.discovery().localNode())) {
                    try {
                        if (ctx.config().isPeerClassLoadingEnabled())
                            item.hnd.p2pUnmarshal(data.nodeId, ctx);

                        if (registerHandler(data.nodeId, item.routineId, item.hnd, item.bufSize, item.interval,
                            item.autoUnsubscribe, false))
                            item.hnd.onListenerRegistered(item.routineId, ctx);
                    }
                    catch (GridException e) {
                        U.error(log, "Failed to register continuous handler.", e);
                    }
                }
            }
        }
    }

    /**
     * @param hnd Handler.
     * @param bufSize Buffer size.
     * @param interval Time interval.
     * @param autoUnsubscribe Automatic unsubscribe flag.
     * @param prjPred Projection predicate.
     * @return Future.
     */
    @SuppressWarnings("TooBroadScope")
    public IgniteFuture<UUID> startRoutine(GridContinuousHandler hnd, int bufSize, long interval,
        boolean autoUnsubscribe, @Nullable IgnitePredicate<ClusterNode> prjPred) {
        assert hnd != null;
        assert bufSize > 0;
        assert interval >= 0;

        // Whether local node is included in routine.
        boolean locIncluded = prjPred == null || prjPred.apply(ctx.discovery().localNode());

        // Generate ID.
        final UUID routineId = UUID.randomUUID();

        StartRequestData reqData = new StartRequestData(prjPred, hnd, bufSize, interval, autoUnsubscribe);

        try {
            if (ctx.config().isPeerClassLoadingEnabled()) {
                // Handle peer deployment for projection predicate.
                if (prjPred != null && !U.isGrid(prjPred.getClass())) {
                    Class cls = U.detectClass(prjPred);

                    String clsName = cls.getName();

                    GridDeployment dep = ctx.deploy().deploy(cls, U.detectClassLoader(cls));

                    if (dep == null)
                        throw new GridDeploymentException("Failed to deploy projection predicate: " + prjPred);

                    reqData.clsName = clsName;
                    reqData.depInfo = new GridDeploymentInfoBean(dep);

                    reqData.p2pMarshal(marsh);
                }

                // Handle peer deployment for other handler-specific objects.
                hnd.p2pMarshal(ctx);
            }
        }
        catch (GridException e) {
            return new GridFinishedFuture<>(ctx, e);
        }

        // Register per-routine notifications listener if ordered messaging is used.
        if (hnd.orderedTopic() != null) {
            ctx.io().addMessageListener(hnd.orderedTopic(), new GridMessageListener() {
                @Override public void onMessage(UUID nodeId, Object obj) {
                    GridContinuousMessage msg = (GridContinuousMessage)obj;

                    // Only notification can be ordered.
                    assert msg.type() == MSG_EVT_NOTIFICATION;

                    if (msg.data() == null && msg.dataBytes() != null) {
                        try {
                            msg.data(marsh.unmarshal(msg.dataBytes(), null));
                        }
                        catch (GridException e) {
                            U.error(log, "Failed to process message (ignoring): " + msg, e);

                            return;
                        }
                    }

                    processNotification(nodeId, msg);
                }
            });
        }

        Collection<? extends ClusterNode> nodes;
        Collection<UUID> nodeIds;

        pendingLock.lock();

        try {
            // Nodes that participate in routine (request will be sent to these nodes directly).
            nodes = F.view(ctx.discovery().allNodes(), F.and(prjPred, F.remoteNodes(ctx.localNodeId())));

            // Stop with exception if projection is empty.
            if (nodes.isEmpty() && !locIncluded) {
                return new GridFinishedFuture<>(ctx,
                    new GridTopologyException("Failed to register remote continuous listener (projection is empty)."));
            }

            // IDs of nodes where request will be sent.
            nodeIds = new GridConcurrentHashSet<>(F.viewReadOnly(nodes, F.node2id()));

            // If there are currently joining nodes, add request to their pending lists.
            // Node IDs set is updated to make sure that we wait for acknowledgement from
            // these nodes.
            for (Map.Entry<UUID, Collection<GridContinuousMessage>> e : pending.entrySet()) {
                if (nodeIds.add(e.getKey()))
                    e.getValue().add(new GridContinuousMessage(MSG_START_REQ, routineId, reqData));
            }

            // Register routine locally.
            locInfos.put(routineId, new LocalRoutineInfo(prjPred, hnd, bufSize, interval));
        }
        finally {
            pendingLock.unlock();
        }

        StartFuture fut = new StartFuture(ctx, routineId);

        if (!nodeIds.isEmpty()) {
            // Wait for acknowledgements.
            waitForStartAck.put(routineId, nodeIds);

            startFuts.put(routineId, fut);

            // Register acknowledge timeout (timeout object will be removed when
            // future is completed).
            fut.addTimeoutObject(new GridTimeoutObjectAdapter(ackTimeout) {
                @Override public void onTimeout() {
                    // Stop waiting for acknowledgements.
                    Collection<UUID> ids = waitForStartAck.remove(routineId);

                    if (ids != null) {
                        StartFuture f = startFuts.remove(routineId);

                        assert f != null;

                        // If there are still nodes without acknowledgements,
                        // Stop routine with exception. Continue and complete
                        // future otherwise.
                        if (!ids.isEmpty()) {
                            f.onDone(new GridException("Failed to get start acknowledgement from nodes (timeout " +
                                "expired): " + ids + ". Will unregister all continuous listeners."));

                            stopRoutine(routineId);
                        }
                        else
                            f.onRemoteRegistered();
                    }
                }
            });
        }

        if (!nodes.isEmpty()) {
            // Do not send projection predicate (nodes already filtered).
            reqData.prjPred = null;

            // Send start requests.
            try {
                GridContinuousMessage req = new GridContinuousMessage(MSG_START_REQ, routineId, reqData);

                sendWithRetries(nodes, req, null);
            }
            catch (GridException e) {
                startFuts.remove(routineId);
                waitForStartAck.remove(routineId);

                fut.onDone(e);

                stopRoutine(routineId);

                locIncluded = false;
            }
        }
        else {
            // There are no remote nodes, but we didn't throw topology exception.
            assert locIncluded;

            // Do not wait anything from remote nodes.
            fut.onRemoteRegistered();
        }

        // Register local handler if needed.
        if (locIncluded) {
            try {
                if (registerHandler(ctx.localNodeId(), routineId, hnd, bufSize, interval, autoUnsubscribe, true))
                    hnd.onListenerRegistered(routineId, ctx);
            }
            catch (GridException e) {
                return new GridFinishedFuture<>(ctx,
                    new GridException("Failed to register handler locally: " + hnd, e));
            }
        }

        // Handler is registered locally.
        fut.onLocalRegistered();

        return fut;
    }

    /**
     * @param routineId Consume ID.
     * @return Future.
     */
    public IgniteFuture<?> stopRoutine(UUID routineId) {
        assert routineId != null;

        boolean doStop = false;

        StopFuture fut = stopFuts.get(routineId);

        // Only one thread will stop routine with provided ID.
        if (fut == null) {
            StopFuture old = stopFuts.putIfAbsent(routineId, fut = new StopFuture(ctx));

            if (old != null)
                fut = old;
            else
                doStop = true;
        }

        if (doStop) {
            // Unregister routine locally.
            LocalRoutineInfo routine = locInfos.remove(routineId);

            // Finish if routine is not found (wrong ID is provided).
            if (routine == null) {
                stopFuts.remove(routineId);

                fut.onDone();

                return fut;
            }

            // Unregister handler locally.
            unregisterHandler(routineId, routine.hnd, true);

            pendingLock.lock();

            try {
                // Remove pending requests for this routine.
                for (Collection<GridContinuousMessage> msgs : pending.values()) {
                    Iterator<GridContinuousMessage> it = msgs.iterator();

                    while (it.hasNext()) {
                        if (it.next().routineId().equals(routineId))
                            it.remove();
                    }
                }
            }
            finally {
                pendingLock.unlock();
            }

            // Nodes where to send stop requests.
            Collection<? extends ClusterNode> nodes = F.view(ctx.discovery().allNodes(),
                F.and(routine.prjPred, F.remoteNodes(ctx.localNodeId())));

            if (!nodes.isEmpty()) {
                // Wait for acknowledgements.
                waitForStopAck.put(routineId, new GridConcurrentHashSet<>(F.viewReadOnly(nodes, F.node2id())));

                // Register acknowledge timeout (timeout object will be removed when
                // future is completed).
                fut.addTimeoutObject(new StopTimeoutObject(ackTimeout, routineId,
                    new GridContinuousMessage(MSG_STOP_REQ, routineId, null)));

                // Send stop requests.
                try {
                    for (ClusterNode node : nodes) {
                        try {
                            sendWithRetries(node.id(), new GridContinuousMessage(MSG_STOP_REQ, routineId, null), null);
                        }
                        catch (GridTopologyException ignored) {
                            U.warn(log, "Failed to send stop request (node left topology): " + node.id());
                        }
                    }
                }
                catch (GridException e) {
                    stopFuts.remove(routineId);
                    waitForStopAck.remove(routineId);

                    fut.onDone(e);
                }
            }
            else {
                stopFuts.remove(routineId);

                fut.onDone();
            }
        }

        return fut;
    }

    /**
     * @param nodeId ID of the node that started routine.
     * @param routineId Routine ID.
     * @param obj Notification object.
     * @param orderedTopic Topic for ordered notifications.
     *      If {@code null}, non-ordered message will be sent.
     * @throws GridException In case of error.
     */
    public void addNotification(UUID nodeId, UUID routineId, @Nullable Object obj, @Nullable Object orderedTopic)
        throws GridException {
        assert nodeId != null;
        assert routineId != null;

        assert !nodeId.equals(ctx.localNodeId());

        RemoteRoutineInfo info = rmtInfos.get(routineId);

        if (info != null) {
            Collection<Object> toSnd = info.add(obj);

            if (toSnd != null)
                sendNotification(nodeId, routineId, toSnd, orderedTopic);
        }
    }

    /**
     * @param nodeId Node ID.
     * @param routineId Routine ID.
     * @param toSnd Notification object to send.
     * @param orderedTopic Topic for ordered notifications.
     *      If {@code null}, non-ordered message will be sent.
     * @throws GridException In case of error.
     */
    private void sendNotification(UUID nodeId, UUID routineId, Collection<Object> toSnd,
        @Nullable Object orderedTopic) throws GridException {
        assert nodeId != null;
        assert routineId != null;
        assert toSnd != null;
        assert !toSnd.isEmpty();

        sendWithRetries(nodeId, new GridContinuousMessage(MSG_EVT_NOTIFICATION, routineId, toSnd), orderedTopic);
    }

    /**
     * @param nodeId Sender ID.
     * @param req Start request.
     */
    private void processStartRequest(UUID nodeId, GridContinuousMessage req) {
        assert nodeId != null;
        assert req != null;

        UUID routineId = req.routineId();
        StartRequestData data = req.data();

        GridContinuousHandler hnd = data.hnd;

        GridException err = null;

        try {
            if (ctx.config().isPeerClassLoadingEnabled()) {
                String clsName = data.clsName;

                if (clsName != null) {
                    GridDeploymentInfo depInfo = data.depInfo;

                    GridDeployment dep = ctx.deploy().getGlobalDeployment(depInfo.deployMode(), clsName, clsName,
                        depInfo.userVersion(), nodeId, depInfo.classLoaderId(), depInfo.participants(), null);

                    if (dep == null)
                        throw new GridDeploymentException("Failed to obtain deployment for class: " + clsName);

                    data.p2pUnmarshal(marsh, dep.classLoader());
                }

                hnd.p2pUnmarshal(nodeId, ctx);
            }
        }
        catch (GridException e) {
            err = e;

            U.error(log, "Failed to register handler [nodeId=" + nodeId + ", routineId=" + routineId + ']', e);
        }

        boolean registered = false;

        if (err == null) {
            try {
                IgnitePredicate<ClusterNode> prjPred = data.prjPred;

                if (prjPred == null || prjPred.apply(ctx.discovery().node(ctx.localNodeId()))) {
                    registered = registerHandler(nodeId, routineId, hnd, data.bufSize, data.interval,
                        data.autoUnsubscribe, false);
                }
            }
            catch (GridException e) {
                err = e;

                U.error(log, "Failed to register handler [nodeId=" + nodeId + ", routineId=" + routineId + ']', e);
            }
        }

        try {
            sendWithRetries(nodeId, new GridContinuousMessage(MSG_START_ACK, routineId, err), null);
        }
        catch (GridTopologyException ignored) {
            if (log.isDebugEnabled())
                log.debug("Failed to send start acknowledgement to node (is node alive?): " + nodeId);
        }
        catch (GridException e) {
            U.error(log, "Failed to send start acknowledgement to node: " + nodeId, e);
        }

        if (registered)
            hnd.onListenerRegistered(routineId, ctx);
    }

    /**
     * @param nodeId Sender ID.
     * @param ack Start acknowledgement.
     */
    private void processStartAck(UUID nodeId, GridContinuousMessage ack) {
        assert nodeId != null;
        assert ack != null;

        UUID routineId = ack.routineId();

        final GridException err = ack.data();

        if (err != null) {
            if (waitForStartAck.remove(routineId) != null) {
                final StartFuture fut = startFuts.remove(routineId);

                if (fut != null) {
                    fut.onDone(err);

                    stopRoutine(routineId);
                }
            }
        }

        Collection<UUID> nodeIds = waitForStartAck.get(routineId);

        if (nodeIds != null) {
            nodeIds.remove(nodeId);

            if (nodeIds.isEmpty())
                completeStartFuture(routineId);
        }
    }

    /**
     * @param nodeId Sender ID.
     * @param req Stop request.
     */
    private void processStopRequest(UUID nodeId, GridContinuousMessage req) {
        assert nodeId != null;
        assert req != null;

        UUID routineId = req.routineId();

        unregisterRemote(routineId);

        try {
            sendWithRetries(nodeId, new GridContinuousMessage(MSG_STOP_ACK, routineId, null), null);
        }
        catch (GridTopologyException ignored) {
            if (log.isDebugEnabled())
                log.debug("Failed to send stop acknowledgement to node (is node alive?): " + nodeId);
        }
        catch (GridException e) {
            U.error(log, "Failed to send stop acknowledgement to node: " + nodeId, e);
        }
    }

    /**
     * @param nodeId Sender ID.
     * @param ack Stop acknowledgement.
     */
    private void processStopAck(UUID nodeId, GridContinuousMessage ack) {
        assert nodeId != null;
        assert ack != null;

        UUID routineId = ack.routineId();

        Collection<UUID> nodeIds = waitForStopAck.get(routineId);

        if (nodeIds != null) {
            nodeIds.remove(nodeId);

            if (nodeIds.isEmpty())
                completeStopFuture(routineId);
        }
    }

    /**
     * @param nodeId Sender ID.
     * @param ntf Notification.
     */
    private void processNotification(UUID nodeId, GridContinuousMessage ntf) {
        assert nodeId != null;
        assert ntf != null;

        UUID routineId = ntf.routineId();

        LocalRoutineInfo routine = locInfos.get(routineId);

        if (routine != null)
            routine.hnd.notifyCallback(nodeId, routineId, (Collection<?>)ntf.data(), ctx);
    }

    /**
     * @param routineId Consume ID.
     */
    private void completeStartFuture(UUID routineId) {
        assert routineId != null;

        if (waitForStartAck.remove(routineId) != null) {
            StartFuture fut = startFuts.remove(routineId);

            assert fut != null;

            fut.onRemoteRegistered();
        }
    }

    /**
     * @param routineId Consume ID.
     */
    private void completeStopFuture(UUID routineId) {
        assert routineId != null;

        if (waitForStopAck.remove(routineId) != null) {
            GridFutureAdapter <?> fut = stopFuts.remove(routineId);

            assert fut != null;

            fut.onDone();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param routineId Consume ID.
     * @param hnd Handler.
     * @param bufSize Buffer size.
     * @param interval Time interval.
     * @param autoUnsubscribe Automatic unsubscribe flag.
     * @param loc Local registration flag.
     * @return Whether listener was actually registered.
     * @throws GridException In case of error.
     */
    private boolean registerHandler(final UUID nodeId, final UUID routineId, final GridContinuousHandler hnd,
        int bufSize, final long interval, boolean autoUnsubscribe, boolean loc) throws GridException {
        assert nodeId != null;
        assert routineId != null;
        assert hnd != null;
        assert bufSize > 0;
        assert interval >= 0;

        final RemoteRoutineInfo info = new RemoteRoutineInfo(nodeId, hnd, bufSize, interval, autoUnsubscribe);

        boolean doRegister = loc;

        if (!doRegister) {
            stopLock.lock();

            try {
                doRegister = !stopped.remove(routineId) && rmtInfos.putIfAbsent(routineId, info) == null;
            }
            finally {
                stopLock.unlock();
            }
        }

        if (doRegister) {
            if (interval > 0) {
                GridThread checker = new GridThread(new GridWorker(ctx.gridName(), "continuous-buffer-checker", log) {
                    @SuppressWarnings("ConstantConditions")
                    @Override protected void body() {
                        long interval0 = interval;

                        while (!isCancelled()) {
                            try {
                                U.sleep(interval0);
                            }
                            catch (GridInterruptedException ignored) {
                                break;
                            }

                            IgniteBiTuple<Collection<Object>, Long> t = info.checkInterval();

                            Collection<Object> toSnd = t.get1();

                            if (toSnd != null) {
                                try {
                                    sendNotification(nodeId, routineId, toSnd, hnd.orderedTopic());
                                }
                                catch (GridTopologyException ignored) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to send notification to node (is node alive?): " + nodeId);
                                }
                                catch (GridException e) {
                                    U.error(log, "Failed to send notification to node: " + nodeId, e);
                                }
                            }

                            interval0 = t.get2();
                        }
                    }
                });

                threads.add(checker);

                checker.start();
            }

            return hnd.register(nodeId, routineId, ctx);
        }

        return false;
    }

    /**
     * @param routineId Routine ID.
     * @param hnd Handler
     * @param loc If Handler unregistered on master node.
     */
    private void unregisterHandler(UUID routineId, GridContinuousHandler hnd, boolean loc) {
        assert routineId != null;
        assert hnd != null;

        if (loc && hnd.orderedTopic() != null)
            ctx.io().removeMessageListener(hnd.orderedTopic());

        hnd.unregister(routineId, ctx);
    }

    /**
     * @param routineId Routine ID.
     */
    @SuppressWarnings("TooBroadScope")
    private void unregisterRemote(UUID routineId) {
        RemoteRoutineInfo info;

        stopLock.lock();

        try {
            info = rmtInfos.remove(routineId);

            if (info == null)
                stopped.add(routineId);
        }
        finally {
            stopLock.unlock();
        }

        if (info != null)
            unregisterHandler(routineId, info.hnd, false);
    }

    /**
     * @param nodeId Destination node ID.
     * @param msg Message.
     * @param orderedTopic Topic for ordered notifications.
     *      If {@code null}, non-ordered message will be sent.
     * @throws GridException In case of error.
     */
    private void sendWithRetries(UUID nodeId, GridContinuousMessage msg, @Nullable Object orderedTopic)
        throws GridException {
        assert nodeId != null;
        assert msg != null;

        ClusterNode node = ctx.discovery().node(nodeId);

        if (node != null)
            sendWithRetries(node, msg, orderedTopic);
        else
            throw new GridTopologyException("Node for provided ID doesn't exist (did it leave the grid?): " + nodeId);
    }

    /**
     * @param node Destination node.
     * @param msg Message.
     * @param orderedTopic Topic for ordered notifications.
     *      If {@code null}, non-ordered message will be sent.
     * @throws GridException In case of error.
     */
    private void sendWithRetries(ClusterNode node, GridContinuousMessage msg, @Nullable Object orderedTopic)
        throws GridException {
        assert node != null;
        assert msg != null;

        sendWithRetries(F.asList(node), msg, orderedTopic);
    }

    /**
     * @param nodes Destination nodes.
     * @param msg Message.
     * @param orderedTopic Topic for ordered notifications.
     *      If {@code null}, non-ordered message will be sent.
     * @throws GridException In case of error.
     */
    private void sendWithRetries(Collection<? extends ClusterNode> nodes, GridContinuousMessage msg,
        @Nullable Object orderedTopic) throws GridException {
        assert !F.isEmpty(nodes);
        assert msg != null;

        if (msg.data() != null && (nodes.size() > 1 || !ctx.localNodeId().equals(F.first(nodes).id())))
            msg.dataBytes(marsh.marshal(msg.data()));

        boolean first = true;

        for (ClusterNode node : nodes) {
            msg = first ? msg : (GridContinuousMessage)msg.clone();

            first = false;

            int cnt = 0;

            while (cnt <= retryCnt) {
                try {
                    cnt++;

                    if (orderedTopic != null) {
                        ctx.io().sendOrderedMessage(
                            node,
                            orderedTopic,
                            ctx.io().nextMessageId(orderedTopic, node.id()),
                            msg,
                            SYSTEM_POOL,
                            0,
                            true);
                    }
                    else
                        ctx.io().send(node, TOPIC_CONTINUOUS, msg, SYSTEM_POOL);

                    break;
                }
                catch (GridInterruptedException e) {
                    throw e;
                }
                catch (GridException e) {
                    if (!ctx.discovery().alive(node.id()))
                        throw new GridTopologyException("Node left grid while sending message to: " + node.id(), e);

                    if (cnt == retryCnt)
                        throw e;
                    else if (log.isDebugEnabled())
                        log.debug("Failed to send message to node (will retry): " + node.id());
                }

                U.sleep(retryDelay);
            }
        }
    }

    /**
     * Local routine info.
     */
    @SuppressWarnings("PackageVisibleInnerClass")
    static class LocalRoutineInfo {
        /** Projection predicate. */
        private final IgnitePredicate<ClusterNode> prjPred;

        /** Continuous routine handler. */
        private final GridContinuousHandler hnd;

        /** Buffer size. */
        private final int bufSize;

        /** Time interval. */
        private final long interval;

        /**
         * @param prjPred Projection predicate.
         * @param hnd Continuous routine handler.
         * @param bufSize Buffer size.
         * @param interval Interval.
         */
        LocalRoutineInfo(@Nullable IgnitePredicate<ClusterNode> prjPred, GridContinuousHandler hnd, int bufSize,
            long interval) {
            assert hnd != null;
            assert bufSize > 0;
            assert interval >= 0;

            this.prjPred = prjPred;
            this.hnd = hnd;
            this.bufSize = bufSize;
            this.interval = interval;
        }

        /**
         * @return Handler.
         */
        GridContinuousHandler handler() {
            return hnd;
        }
    }

    /**
     * Remote routine info.
     */
    private static class RemoteRoutineInfo {
        /** Master node ID. */
        private final UUID nodeId;

        /** Continuous routine handler. */
        private final GridContinuousHandler hnd;

        /** Buffer size. */
        private final int bufSize;

        /** Time interval. */
        private final long interval;

        /** Lock. */
        private final ReadWriteLock lock = new ReentrantReadWriteLock();

        /** Buffer. */
        private ConcurrentLinkedDeque8<Object> buf;

        /** Last send time. */
        private long lastSndTime = U.currentTimeMillis();

        /** Automatic unsubscribe flag. */
        private boolean autoUnsubscribe;

        /**
         * @param nodeId Master node ID.
         * @param hnd Continuous routine handler.
         * @param bufSize Buffer size.
         * @param interval Interval.
         * @param autoUnsubscribe Automatic unsubscribe flag.
         */
        RemoteRoutineInfo(UUID nodeId, GridContinuousHandler hnd, int bufSize, long interval,
            boolean autoUnsubscribe) {
            assert nodeId != null;
            assert hnd != null;
            assert bufSize > 0;
            assert interval >= 0;

            this.nodeId = nodeId;
            this.hnd = hnd;
            this.bufSize = bufSize;
            this.interval = interval;
            this.autoUnsubscribe = autoUnsubscribe;

            buf = new ConcurrentLinkedDeque8<>();
        }

        /**
         * @param obj Object to add.
         * @return Object to send or {@code null} if there is nothing to send for now.
         */
        @Nullable Collection<Object> add(@Nullable Object obj) {
            Collection<Object> toSnd = null;

            if (buf.sizex() >= bufSize - 1) {
                lock.writeLock().lock();

                try {
                    buf.add(obj);

                    toSnd = buf;

                    buf = new ConcurrentLinkedDeque8<>();

                    if (interval > 0)
                        lastSndTime = U.currentTimeMillis();
                }
                finally {
                    lock.writeLock().unlock();
                }
            }
            else {
                lock.readLock().lock();

                try {
                    buf.add(obj);
                }
                finally {
                    lock.readLock().unlock();
                }
            }

            return toSnd != null ? new ArrayList<>(toSnd) : null;
        }

        /**
         * @return Tuple with objects to sleep (or {@code null} if there is nothing to
         *      send for now) and time interval after next check is needed.
         */
        @SuppressWarnings("TooBroadScope")
        IgniteBiTuple<Collection<Object>, Long> checkInterval() {
            assert interval > 0;

            Collection<Object> toSnd = null;
            long diff;

            long now = U.currentTimeMillis();

            lock.writeLock().lock();

            try {
                diff = now - lastSndTime;

                if (diff >= interval && !buf.isEmpty()) {
                    toSnd = buf;

                    buf = new ConcurrentLinkedDeque8<>();

                    lastSndTime = now;
                }
            }
            finally {
                lock.writeLock().unlock();
            }

            return F.t(toSnd, diff < interval ? interval - diff : interval);
        }
    }

    /**
     * Start request data.
     */
    private static class StartRequestData implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Projection predicate. */
        private IgnitePredicate<ClusterNode> prjPred;

        /** Serialized projection predicate. */
        private byte[] prjPredBytes;

        /** Deployment class name. */
        private String clsName;

        /** Deployment info. */
        private GridDeploymentInfo depInfo;

        /** Handler. */
        private GridContinuousHandler hnd;

        /** Buffer size. */
        private int bufSize;

        /** Time interval. */
        private long interval;

        /** Automatic unsubscribe flag. */
        private boolean autoUnsubscribe;

        /**
         * Required by {@link Externalizable}.
         */
        public StartRequestData() {
            // No-op.
        }

        /**
         * @param prjPred Serialized projection predicate.
         * @param hnd Handler.
         * @param bufSize Buffer size.
         * @param interval Time interval.
         * @param autoUnsubscribe Automatic unsubscribe flag.
         */
        StartRequestData(@Nullable IgnitePredicate<ClusterNode> prjPred, GridContinuousHandler hnd,
            int bufSize, long interval, boolean autoUnsubscribe) {
            assert hnd != null;
            assert bufSize > 0;
            assert interval >= 0;

            this.prjPred = prjPred;
            this.hnd = hnd;
            this.bufSize = bufSize;
            this.interval = interval;
            this.autoUnsubscribe = autoUnsubscribe;
        }

        /**
         * @param marsh Marshaller.
         * @throws GridException In case of error.
         */
        void p2pMarshal(GridMarshaller marsh) throws GridException {
            assert marsh != null;

            prjPredBytes = marsh.marshal(prjPred);
        }

        /**
         * @param marsh Marshaller.
         * @param ldr Class loader.
         * @throws GridException In case of error.
         */
        void p2pUnmarshal(GridMarshaller marsh, @Nullable ClassLoader ldr) throws GridException {
            assert marsh != null;

            assert prjPred == null;
            assert prjPredBytes != null;

            prjPred = marsh.unmarshal(prjPredBytes, ldr);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            boolean b = prjPredBytes != null;

            out.writeBoolean(b);

            if (b) {
                U.writeByteArray(out, prjPredBytes);
                U.writeString(out, clsName);
                out.writeObject(depInfo);
            }
            else
                out.writeObject(prjPred);

            out.writeObject(hnd);
            out.writeInt(bufSize);
            out.writeLong(interval);
            out.writeBoolean(autoUnsubscribe);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            boolean b = in.readBoolean();

            if (b) {
                prjPredBytes = U.readByteArray(in);
                clsName = U.readString(in);
                depInfo = (GridDeploymentInfo)in.readObject();
            }
            else
                prjPred = (IgnitePredicate<ClusterNode>)in.readObject();

            hnd = (GridContinuousHandler)in.readObject();
            bufSize = in.readInt();
            interval = in.readLong();
            autoUnsubscribe = in.readBoolean();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StartRequestData.class, this);
        }
    }

    /**
     * Discovery data.
     */
    private static class DiscoveryData implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Node ID. */
        private UUID nodeId;

        /** Items. */
        @GridToStringInclude
        private Collection<DiscoveryDataItem> items;

        /**
         * Required by {@link Externalizable}.
         */
        public DiscoveryData() {
            // No-op.
        }

        /**
         * @param nodeId Node ID.
         */
        DiscoveryData(UUID nodeId) {
            assert nodeId != null;

            this.nodeId = nodeId;

            items = new ArrayList<>();
        }

        /**
         * @param item Item.
         */
        public void addItem(DiscoveryDataItem item) {
            items.add(item);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeUuid(out, nodeId);
            U.writeCollection(out, items);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            nodeId = U.readUuid(in);
            items = U.readCollection(in);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DiscoveryData.class, this);
        }
    }

    /**
     * Discovery data item.
     */
    private static class DiscoveryDataItem implements Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Consume ID. */
        private UUID routineId;

        /** Projection predicate. */
        private IgnitePredicate<ClusterNode> prjPred;

        /** Handler. */
        private GridContinuousHandler hnd;

        /** Buffer size. */
        private int bufSize;

        /** Time interval. */
        private long interval;

        /** Automatic unsubscribe flag. */
        private boolean autoUnsubscribe;

        /**
         * Required by {@link Externalizable}.
         */
        public DiscoveryDataItem() {
            // No-op.
        }

        /**
         * @param routineId Consume ID.
         * @param prjPred Projection predicate.
         * @param hnd Handler.
         * @param bufSize Buffer size.
         * @param interval Time interval.
         */
        DiscoveryDataItem(UUID routineId, @Nullable IgnitePredicate<ClusterNode> prjPred,
            GridContinuousHandler hnd, int bufSize, long interval) {
            assert routineId != null;
            assert hnd != null;
            assert bufSize > 0;
            assert interval >= 0;

            this.routineId = routineId;
            this.prjPred = prjPred;
            this.hnd = hnd;
            this.bufSize = bufSize;
            this.interval = interval;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeUuid(out, routineId);
            out.writeObject(prjPred);
            out.writeObject(hnd);
            out.writeInt(bufSize);
            out.writeLong(interval);
            out.writeBoolean(autoUnsubscribe);
        }

        /** {@inheritDoc} */
        @SuppressWarnings("unchecked")
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            routineId = U.readUuid(in);
            prjPred = (IgnitePredicate<ClusterNode>)in.readObject();
            hnd = (GridContinuousHandler)in.readObject();
            bufSize = in.readInt();
            interval = in.readLong();
            autoUnsubscribe = in.readBoolean();
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DiscoveryDataItem.class, this);
        }
    }

    /**
     * Future for start routine.
     */
    private static class StartFuture extends GridFutureAdapter<UUID> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Consume ID. */
        private UUID routineId;

        /** Local listener is registered. */
        private volatile boolean loc;

        /** All remote listeners are registered. */
        private volatile boolean rmt;

        /** Timeout object. */
        private volatile GridTimeoutObject timeoutObj;

        /**
         * Required by {@link Externalizable}.
         */
        public StartFuture() {
            // No-op.
        }

        /**
         * @param ctx Kernal context.
         * @param routineId Consume ID.
         */
        StartFuture(GridKernalContext ctx, UUID routineId) {
            super(ctx);

            this.routineId = routineId;
        }

        /**
         * Called when local listener is registered.
         */
        public void onLocalRegistered() {
            loc = true;

            if (rmt && !isDone())
                onDone(routineId);
        }

        /**
         * Called when all remote listeners are registered.
         */
        public void onRemoteRegistered() {
            rmt = true;

            if (loc && !isDone())
                onDone(routineId);
        }

        /**
         * @param timeoutObj Timeout object.
         */
        public void addTimeoutObject(GridTimeoutObject timeoutObj) {
            assert timeoutObj != null;

            this.timeoutObj = timeoutObj;

            ctx.timeout().addTimeoutObject(timeoutObj);
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable UUID res, @Nullable Throwable err) {
            if (timeoutObj != null)
                ctx.timeout().removeTimeoutObject(timeoutObj);

            return super.onDone(res, err);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StartFuture.class, this);
        }
    }

    /**
     * Future for stop routine.
     */
    private static class StopFuture extends GridFutureAdapter<Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** Timeout object. */
        private volatile GridTimeoutObject timeoutObj;

        /**
         * Required by {@link Externalizable}.
         */
        public StopFuture() {
            // No-op.
        }

        /**
         * @param ctx Kernal context.
         */
        StopFuture(GridKernalContext ctx) {
            super(ctx);
        }

        /**
         * @param timeoutObj Timeout object.
         */
        public void addTimeoutObject(GridTimeoutObject timeoutObj) {
            assert timeoutObj != null;

            this.timeoutObj = timeoutObj;

            ctx.timeout().addTimeoutObject(timeoutObj);
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(@Nullable Object res, @Nullable Throwable err) {
            if (timeoutObj != null)
                ctx.timeout().removeTimeoutObject(timeoutObj);

            return super.onDone(res, err);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(StopFuture.class, this);
        }
    }

    /**
     * Timeout object for stop process.
     */
    private class StopTimeoutObject extends GridTimeoutObjectAdapter {
        /** Timeout. */
        private final long timeout;

        /** Routine ID. */
        private final UUID routineId;

        /** Request. */
        private final GridContinuousMessage req;

        /**
         * @param timeout Timeout.
         * @param routineId Routine ID.
         * @param req Request.
         */
        protected StopTimeoutObject(long timeout, UUID routineId, GridContinuousMessage req) {
            super(timeout);

            assert routineId != null;
            assert req != null;

            this.timeout = timeout;
            this.routineId = routineId;
            this.req = req;
        }

        /** {@inheritDoc} */
        @Override public void onTimeout() {
            Collection<UUID> ids = waitForStopAck.remove(routineId);

            if (ids != null) {
                U.warn(log, "Failed to get stop acknowledgement from nodes (timeout expired): " + ids +
                    ". Will retry.");

                StopFuture f = stopFuts.get(routineId);

                if (f != null) {
                    if (!ids.isEmpty()) {
                        waitForStopAck.put(routineId, ids);

                        // Resend requests.
                        for (UUID id : ids) {
                            try {
                                sendWithRetries(id, req, null);
                            }
                            catch (GridTopologyException ignored) {
                                if (log.isDebugEnabled())
                                    log.debug("Failed to resend stop request to node (is node alive?): " + id);
                            }
                            catch (GridException e) {
                                U.error(log, "Failed to resend stop request to node: " + id, e);

                                ids.remove(id);

                                if (ids.isEmpty())
                                    f.onDone(e);
                            }
                        }

                        // Reschedule timeout.
                        ctx.timeout().addTimeoutObject(new StopTimeoutObject(timeout, routineId, req));
                    }
                    else if (stopFuts.remove(routineId) != null)
                        f.onDone();
                }
            }
        }
    }
}
