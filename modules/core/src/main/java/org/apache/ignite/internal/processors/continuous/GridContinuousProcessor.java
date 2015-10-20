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

package org.apache.ignite.internal.processors.continuous;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteDeploymentCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteInterruptedCheckedException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfoBean;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.cache.GridCacheContext;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.util.worker.GridWorker;
import org.apache.ignite.lang.IgniteBiTuple;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;
import org.jsr166.ConcurrentLinkedDeque8;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_SEGMENTED;
import static org.apache.ignite.internal.GridTopic.TOPIC_CONTINUOUS;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.processors.continuous.GridContinuousMessageType.MSG_EVT_ACK;
import static org.apache.ignite.internal.processors.continuous.GridContinuousMessageType.MSG_EVT_NOTIFICATION;

/**
 * Processor for continuous routines.
 */
public class GridContinuousProcessor extends GridProcessorAdapter {
    /** Local infos. */
    private final ConcurrentMap<UUID, LocalRoutineInfo> locInfos = new ConcurrentHashMap8<>();

    /** Local infos. */
    private final ConcurrentMap<UUID, Map<UUID, LocalRoutineInfo>> clientInfos = new ConcurrentHashMap8<>();

    /** Remote infos. */
    private final ConcurrentMap<UUID, RemoteRoutineInfo> rmtInfos = new ConcurrentHashMap8<>();

    /** Start futures. */
    private final ConcurrentMap<UUID, StartFuture> startFuts = new ConcurrentHashMap8<>();

    /** Stop futures. */
    private final ConcurrentMap<UUID, StopFuture> stopFuts = new ConcurrentHashMap8<>();

    /** Threads started by this processor. */
    private final Map<UUID, IgniteThread> bufCheckThreads = new ConcurrentHashMap8<>();

    /** */
    private final ConcurrentMap<IgniteUuid, SyncMessageAckFuture> syncMsgFuts = new ConcurrentHashMap8<>();

    /** Stopped IDs. */
    private final Collection<UUID> stopped = new HashSet<>();

    /** Lock for stop process. */
    private final Lock stopLock = new ReentrantLock();

    /** Marshaller. */
    private Marshaller marsh;

    /** Delay in milliseconds between retries. */
    private long retryDelay = 1000;

    /** Number of retries using to send messages. */
    private int retryCnt = 3;

    /** */
    private final ReentrantReadWriteLock processorStopLock = new ReentrantReadWriteLock();

    /** */
    private boolean processorStopped;

    /**
     * @param ctx Kernal context.
     */
    public GridContinuousProcessor(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        retryDelay = ctx.config().getNetworkSendRetryDelay();
        retryCnt = ctx.config().getNetworkSendRetryCount();

        marsh = ctx.config().getMarshaller();

        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @SuppressWarnings({"fallthrough", "TooBroadScope"})
            @Override public void onEvent(Event evt) {
                assert evt instanceof DiscoveryEvent;
                assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED;

                UUID nodeId = ((DiscoveryEvent)evt).eventNode().id();

                clientInfos.remove(nodeId);

                // Unregister handlers created by left node.
                for (Map.Entry<UUID, RemoteRoutineInfo> e : rmtInfos.entrySet()) {
                    UUID routineId = e.getKey();
                    RemoteRoutineInfo info = e.getValue();

                    if (info.autoUnsubscribe && nodeId.equals(info.nodeId))
                        unregisterRemote(routineId);
                }

                for (Map.Entry<IgniteUuid, SyncMessageAckFuture> e : syncMsgFuts.entrySet()) {
                    SyncMessageAckFuture fut = e.getValue();

                    if (fut.nodeId().equals(nodeId)) {
                        SyncMessageAckFuture fut0 = syncMsgFuts.remove(e.getKey());

                        if (fut0 != null) {
                            ClusterTopologyCheckedException err = new ClusterTopologyCheckedException(
                                "Node left grid while sending message to: " + nodeId);

                            fut0.onDone(err);
                        }
                    }
                }
            }
        }, EVT_NODE_LEFT, EVT_NODE_FAILED);

        ctx.event().addLocalEventListener(new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                cancelFutures(new IgniteCheckedException("Topology segmented"));
            }
        }, EVT_NODE_SEGMENTED);

        ctx.discovery().setCustomEventListener(StartRoutineDiscoveryMessage.class,
            new CustomEventListener<StartRoutineDiscoveryMessage>() {
                @Override public void onCustomEvent(ClusterNode snd, StartRoutineDiscoveryMessage msg) {
                    if (!snd.id().equals(ctx.localNodeId()) && !ctx.isStopping())
                        processStartRequest(snd, msg);
                }
            });

        ctx.discovery().setCustomEventListener(StartRoutineAckDiscoveryMessage.class,
            new CustomEventListener<StartRoutineAckDiscoveryMessage>() {
                @Override public void onCustomEvent(ClusterNode snd, StartRoutineAckDiscoveryMessage msg) {
                    StartFuture fut = startFuts.remove(msg.routineId());

                    if (fut != null) {
                        if (msg.errs().isEmpty())
                            fut.onRemoteRegistered();
                        else {
                            IgniteCheckedException firstEx = F.first(msg.errs().values());

                            fut.onDone(firstEx);

                            stopRoutine(msg.routineId());
                        }
                    }
                }
            });

        ctx.discovery().setCustomEventListener(StopRoutineDiscoveryMessage.class,
            new CustomEventListener<StopRoutineDiscoveryMessage>() {
                @Override public void onCustomEvent(ClusterNode snd, StopRoutineDiscoveryMessage msg) {
                    if (!snd.id().equals(ctx.localNodeId())) {
                        UUID routineId = msg.routineId();

                        unregisterRemote(routineId);

                        if (snd.isClient()) {
                            Map<UUID, LocalRoutineInfo> clientRoutineMap = clientInfos.get(snd.id());

                            if (clientRoutineMap != null)
                                clientRoutineMap.remove(msg.routineId());
                        }
                    }
                }
            });

        ctx.discovery().setCustomEventListener(StopRoutineAckDiscoveryMessage.class,
            new CustomEventListener<StopRoutineAckDiscoveryMessage>() {
                @Override public void onCustomEvent(ClusterNode snd, StopRoutineAckDiscoveryMessage msg) {
                    StopFuture fut = stopFuts.remove(msg.routineId());

                    if (fut != null)
                        fut.onDone();
                }
            });

        ctx.io().addMessageListener(TOPIC_CONTINUOUS, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object obj) {
                GridContinuousMessage msg = (GridContinuousMessage)obj;

                if (msg.data() == null && msg.dataBytes() != null) {
                    try {
                        msg.data(marsh.unmarshal(msg.dataBytes(), null));
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to process message (ignoring): " + msg, e);

                        return;
                    }
                }

                switch (msg.type()) {
                    case MSG_EVT_NOTIFICATION:
                        processNotification(nodeId, msg);

                        break;

                    case MSG_EVT_ACK:
                        processMessageAck(msg);

                        break;

                    default:
                        assert false : "Unexpected message received: " + msg.type();
                }
            }
        });

        if (log.isDebugEnabled())
            log.debug("Continuous processor started.");
    }

    /**
     * @param e Error.
     */
    private void cancelFutures(IgniteCheckedException e) {
        for (Iterator<StartFuture> itr = startFuts.values().iterator(); itr.hasNext(); ) {
            StartFuture fut = itr.next();

            itr.remove();

            fut.onDone(e);
        }

        for (Iterator<StopFuture> itr = stopFuts.values().iterator(); itr.hasNext(); ) {
            StopFuture fut = itr.next();

            itr.remove();

            fut.onDone(e);
        }
    }

    /**
     * @return {@code true} if lock successful, {@code false} if processor already stopped.
     */
    @SuppressWarnings("LockAcquiredButNotSafelyReleased")
    public boolean lockStopping() {
        processorStopLock.readLock().lock();

        if (processorStopped) {
            processorStopLock.readLock().unlock();

            return false;
        }

        return true;
    }

    /**
     *
     */
    public void unlockStopping() {
        processorStopLock.readLock().unlock();
    }

    /** {@inheritDoc} */
    @Override public void onKernalStop(boolean cancel) {
        processorStopLock.writeLock().lock();

        try {
            processorStopped = true;
        }
        finally {
            processorStopLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        if (ctx.config().isDaemon())
            return;

        ctx.io().removeMessageListener(TOPIC_CONTINUOUS);

        for (IgniteThread thread : bufCheckThreads.values()) {
            U.interrupt(thread);
            U.join(thread);
        }

        if (log.isDebugEnabled())
            log.debug("Continuous processor stopped.");
    }

    /** {@inheritDoc} */
    @Nullable @Override public DiscoveryDataExchangeType discoveryDataType() {
        return DiscoveryDataExchangeType.CONTINUOUS_PROC;
    }

    /** {@inheritDoc} */
    @Override @Nullable public Serializable collectDiscoveryData(UUID nodeId) {
        if (!nodeId.equals(ctx.localNodeId()) || !locInfos.isEmpty()) {
            DiscoveryData data = new DiscoveryData(ctx.localNodeId(), clientInfos);

            // Collect listeners information (will be sent to joining node during discovery process).
            for (Map.Entry<UUID, LocalRoutineInfo> e : locInfos.entrySet()) {
                UUID routineId = e.getKey();
                LocalRoutineInfo info = e.getValue();

                data.addItem(new DiscoveryDataItem(routineId,
                    info.prjPred,
                    info.hnd,
                    info.bufSize,
                    info.interval,
                    info.autoUnsubscribe));
            }

            return data;
        }

        return null;
    }

    /** {@inheritDoc} */
    @Override public void onDiscoveryDataReceived(UUID joiningNodeId, UUID rmtNodeId, Serializable obj) {
        DiscoveryData data = (DiscoveryData)obj;

        if (!ctx.isDaemon() && data != null) {
            for (DiscoveryDataItem item : data.items) {
                try {
                    if (item.prjPred != null)
                        ctx.resource().injectGeneric(item.prjPred);

                    // Register handler only if local node passes projection predicate.
                    if (item.prjPred == null || item.prjPred.apply(ctx.discovery().localNode())) {
                        if (registerHandler(data.nodeId, item.routineId, item.hnd, item.bufSize, item.interval,
                            item.autoUnsubscribe, false))
                            item.hnd.onListenerRegistered(item.routineId, ctx);
                    }
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to register continuous handler.", e);
                }
            }

            for (Map.Entry<UUID, Map<UUID, LocalRoutineInfo>> entry : data.clientInfos.entrySet()) {
                UUID clientNodeId = entry.getKey();

                Map<UUID, LocalRoutineInfo> clientRoutineMap = entry.getValue();

                for (Map.Entry<UUID, LocalRoutineInfo> e : clientRoutineMap.entrySet()) {
                    UUID routineId = e.getKey();
                    LocalRoutineInfo info = e.getValue();

                    try {
                        if (info.prjPred != null)
                            ctx.resource().injectGeneric(info.prjPred);

                        if (info.prjPred == null || info.prjPred.apply(ctx.discovery().localNode())) {
                            if (registerHandler(clientNodeId,
                                routineId,
                                info.hnd,
                                info.bufSize,
                                info.interval,
                                info.autoUnsubscribe,
                                false))
                                info.hnd.onListenerRegistered(routineId, ctx);
                        }
                    }
                    catch (IgniteCheckedException err) {
                        U.error(log, "Failed to register continuous handler.", err);
                    }
                }

                Map<UUID, LocalRoutineInfo> map = clientInfos.get(entry.getKey());

                if (map == null) {
                    map = new HashMap<>();

                    clientInfos.put(entry.getKey(), map);
                }

                map.putAll(entry.getValue());
            }
        }
    }

    /**
     * Callback invoked when cache is started.
     *
     * @param ctx Cache context.
     * @throws IgniteCheckedException If failed.
     */
    public void onCacheStart(GridCacheContext ctx) throws IgniteCheckedException {
        for (Map.Entry<UUID, RemoteRoutineInfo> entry : rmtInfos.entrySet()) {
            UUID routineId = entry.getKey();
            RemoteRoutineInfo rmtInfo = entry.getValue();

            GridContinuousHandler hnd = rmtInfo.hnd;

            if (hnd.isForQuery() && F.eq(ctx.name(), hnd.cacheName()) && rmtInfo.clearDelayedRegister()) {
                GridContinuousHandler.RegisterStatus status = hnd.register(rmtInfo.nodeId, routineId, this.ctx);

                assert status != GridContinuousHandler.RegisterStatus.DELAYED;

                if (status == GridContinuousHandler.RegisterStatus.REGISTERED)
                    hnd.onListenerRegistered(routineId, this.ctx);
            }
        }
    }

    /**
     * @param ctx Callback invoked when cache is stopped.
     */
    public void onCacheStop(GridCacheContext ctx) {
        Iterator<Map.Entry<UUID, RemoteRoutineInfo>> it = rmtInfos.entrySet().iterator();

        while (it.hasNext()) {
            Map.Entry<UUID, RemoteRoutineInfo> entry = it.next();

            GridContinuousHandler hnd = entry.getValue().hnd;

            if (hnd.isForQuery() && F.eq(ctx.name(), hnd.cacheName()))
                it.remove();
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
    public IgniteInternalFuture<UUID> startRoutine(GridContinuousHandler hnd,
        int bufSize,
        long interval,
        boolean autoUnsubscribe,
        @Nullable IgnitePredicate<ClusterNode> prjPred) {
        assert hnd != null;
        assert bufSize > 0;
        assert interval >= 0;

        // Whether local node is included in routine.
        boolean locIncluded = prjPred == null || prjPred.apply(ctx.discovery().localNode());

        // Generate ID.
        final UUID routineId = UUID.randomUUID();

        StartRequestData reqData = new StartRequestData(prjPred, hnd.clone(), bufSize, interval, autoUnsubscribe);

        try {
            if (ctx.config().isPeerClassLoadingEnabled()) {
                // Handle peer deployment for projection predicate.
                if (prjPred != null && !U.isGrid(prjPred.getClass())) {
                    Class cls = U.detectClass(prjPred);

                    String clsName = cls.getName();

                    GridDeployment dep = ctx.deploy().deploy(cls, U.detectClassLoader(cls));

                    if (dep == null)
                        throw new IgniteDeploymentCheckedException("Failed to deploy projection predicate: " + prjPred);

                    reqData.className(clsName);
                    reqData.deploymentInfo(new GridDeploymentInfoBean(dep));

                    reqData.p2pMarshal(marsh);
                }

                // Handle peer deployment for other handler-specific objects.
                reqData.handler().p2pMarshal(ctx);
            }
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(e);
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
                        catch (IgniteCheckedException e) {
                            U.error(log, "Failed to process message (ignoring): " + msg, e);

                            return;
                        }
                    }

                    processNotification(nodeId, msg);
                }
            });
        }

        // Register routine locally.
        locInfos.put(routineId, new LocalRoutineInfo(prjPred, hnd, bufSize, interval, autoUnsubscribe));

        StartFuture fut = new StartFuture(ctx, routineId);

        startFuts.put(routineId, fut);

        try {
            if (locIncluded && registerHandler(ctx.localNodeId(), routineId, hnd, bufSize, interval, autoUnsubscribe, true))
                hnd.onListenerRegistered(routineId, ctx);

            ctx.discovery().sendCustomEvent(new StartRoutineDiscoveryMessage(routineId, reqData));
        }
        catch (IgniteCheckedException e) {
            startFuts.remove(routineId);
            locInfos.remove(routineId);

            unregisterHandler(routineId, hnd, true);

            fut.onDone(e);

            return fut;
        }

        // Handler is registered locally.
        fut.onLocalRegistered();

        return fut;
    }

    /**
     * @param routineId Consume ID.
     * @return Future.
     */
    public IgniteInternalFuture<?> stopRoutine(UUID routineId) {
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

            try {
                ctx.discovery().sendCustomEvent(new StopRoutineDiscoveryMessage(routineId));
            }
            catch (IgniteCheckedException e) {
                fut.onDone(e);
            }

            if (ctx.isStopping())
                fut.onDone();
        }

        return fut;
    }

    /**
     * @param nodeId ID of the node that started routine.
     * @param routineId Routine ID.
     * @param obj Notification object.
     * @param orderedTopic Topic for ordered notifications. If {@code null}, non-ordered message will be sent.
     * @param sync If {@code true} then waits for event acknowledgment.
     * @param msg If {@code true} then sent data is message.
     * @throws IgniteCheckedException In case of error.
     */
    public void addNotification(UUID nodeId,
        UUID routineId,
        @Nullable Object obj,
        @Nullable Object orderedTopic,
        boolean sync,
        boolean msg)
        throws IgniteCheckedException {
        assert nodeId != null;
        assert routineId != null;
        assert !msg || obj instanceof Message : obj;

        assert !nodeId.equals(ctx.localNodeId());

        if (processorStopped)
            return;

        RemoteRoutineInfo info = rmtInfos.get(routineId);

        if (info != null) {
            assert info.interval == 0 || !sync;

            if (sync) {
                SyncMessageAckFuture fut = new SyncMessageAckFuture(nodeId);

                IgniteUuid futId = IgniteUuid.randomUuid();

                syncMsgFuts.put(futId, fut);

                try {
                    sendNotification(nodeId, routineId, futId, F.asList(obj), orderedTopic, msg);
                }
                catch (IgniteCheckedException e) {
                    syncMsgFuts.remove(futId);

                    throw e;
                }

                fut.get();
            }
            else {
                Collection<Object> toSnd = info.add(obj);

                if (toSnd != null)
                    sendNotification(nodeId, routineId, null, toSnd, orderedTopic, msg);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public void onDisconnected(IgniteFuture<?> reconnectFut) throws IgniteCheckedException {
        cancelFutures(new IgniteClientDisconnectedCheckedException(reconnectFut, "Client node disconnected."));

        for (UUID rmtId : rmtInfos.keySet())
            unregisterRemote(rmtId);

        rmtInfos.clear();

        clientInfos.clear();
    }

    /**
     * @param nodeId Node ID.
     * @param routineId Routine ID.
     * @param futId Future ID.
     * @param toSnd Notification object to send.
     * @param orderedTopic Topic for ordered notifications.
     *      If {@code null}, non-ordered message will be sent.
     * @param msg If {@code true} then sent data is collection of messages.
     * @throws IgniteCheckedException In case of error.
     */
    private void sendNotification(UUID nodeId,
        UUID routineId,
        @Nullable IgniteUuid futId,
        Collection<Object> toSnd,
        @Nullable Object orderedTopic,
        boolean msg) throws IgniteCheckedException {
        assert nodeId != null;
        assert routineId != null;
        assert toSnd != null;
        assert !toSnd.isEmpty();

        sendWithRetries(nodeId,
            new GridContinuousMessage(MSG_EVT_NOTIFICATION, routineId, futId, toSnd, msg),
            orderedTopic);
    }

    /**
     * @param node Sender.
     * @param req Start request.
     */
    private void processStartRequest(ClusterNode node, StartRoutineDiscoveryMessage req) {
        UUID routineId = req.routineId();
        StartRequestData data = req.startRequestData();

        GridContinuousHandler hnd = data.handler();

        IgniteCheckedException err = null;

        try {
            if (ctx.config().isPeerClassLoadingEnabled()) {
                String clsName = data.className();

                if (clsName != null) {
                    GridDeploymentInfo depInfo = data.deploymentInfo();

                    GridDeployment dep = ctx.deploy().getGlobalDeployment(depInfo.deployMode(), clsName, clsName,
                        depInfo.userVersion(), node.id(), depInfo.classLoaderId(), depInfo.participants(), null);

                    if (dep == null)
                        throw new IgniteDeploymentCheckedException("Failed to obtain deployment for class: " + clsName);

                    data.p2pUnmarshal(marsh, dep.classLoader());
                }

                hnd.p2pUnmarshal(node.id(), ctx);
            }
        }
        catch (IgniteCheckedException e) {
            err = e;

            U.error(log, "Failed to register handler [nodeId=" + node.id() + ", routineId=" + routineId + ']', e);
        }

        if (node.isClient()) {
            Map<UUID, LocalRoutineInfo> clientRoutineMap = clientInfos.get(node.id());

            if (clientRoutineMap == null) {
                clientRoutineMap = new HashMap<>();

                Map<UUID, LocalRoutineInfo> old = clientInfos.put(node.id(), clientRoutineMap);

                assert old == null;
            }

            clientRoutineMap.put(routineId, new LocalRoutineInfo(data.projectionPredicate(),
                hnd,
                data.bufferSize(),
                data.interval(),
                data.autoUnsubscribe()));
        }

        boolean registered = false;

        if (err == null) {
            try {
                IgnitePredicate<ClusterNode> prjPred = data.projectionPredicate();

                ctx.resource().injectGeneric(prjPred);

                if (prjPred == null || prjPred.apply(ctx.discovery().node(ctx.localNodeId()))) {
                    registered = registerHandler(node.id(), routineId, hnd, data.bufferSize(), data.interval(),
                        data.autoUnsubscribe(), false);
                }
            }
            catch (IgniteCheckedException e) {
                err = e;

                U.error(log, "Failed to register handler [nodeId=" + node.id() + ", routineId=" + routineId + ']', e);
            }
        }

        if (err != null)
            req.addError(ctx.localNodeId(), err);

        if (registered)
            hnd.onListenerRegistered(routineId, ctx);
    }

    /**
     * @param msg Message.
     */
    private void processMessageAck(GridContinuousMessage msg) {
        assert msg.futureId() != null;

        SyncMessageAckFuture fut = syncMsgFuts.remove(msg.futureId());

        if (fut != null)
            fut.onDone();
    }

    /**
     * @param nodeId Sender ID.
     * @param msg Message.
     */
    private void processNotification(UUID nodeId, GridContinuousMessage msg) {
        assert nodeId != null;
        assert msg != null;

        UUID routineId = msg.routineId();

        try {
            LocalRoutineInfo routine = locInfos.get(routineId);

            if (routine != null)
                routine.hnd.notifyCallback(nodeId, routineId, (Collection<?>)msg.data(), ctx);
        }
        finally {
            if (msg.futureId() != null) {
                try {
                    sendWithRetries(nodeId,
                        new GridContinuousMessage(MSG_EVT_ACK, null, msg.futureId(), null, false),
                        null);
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to send event acknowledgment to node: " + nodeId, e);
                }
            }
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
     * @throws IgniteCheckedException In case of error.
     */
    private boolean registerHandler(final UUID nodeId,
        final UUID routineId,
        final GridContinuousHandler hnd,
        int bufSize,
        final long interval,
        boolean autoUnsubscribe,
        boolean loc) throws IgniteCheckedException {
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
                IgniteThread checker = new IgniteThread(new GridWorker(ctx.gridName(), "continuous-buffer-checker", log) {
                    @SuppressWarnings("ConstantConditions")
                    @Override protected void body() {
                        long interval0 = interval;

                        while (!isCancelled()) {
                            try {
                                U.sleep(interval0);
                            }
                            catch (IgniteInterruptedCheckedException ignored) {
                                break;
                            }

                            IgniteBiTuple<Collection<Object>, Long> t = info.checkInterval();

                            Collection<Object> toSnd = t.get1();

                            if (toSnd != null && !toSnd.isEmpty()) {
                                try {
                                    boolean msg = toSnd.iterator().next() instanceof Message;

                                    sendNotification(nodeId, routineId, null, toSnd, hnd.orderedTopic(), msg);
                                }
                                catch (ClusterTopologyCheckedException ignored) {
                                    if (log.isDebugEnabled())
                                        log.debug("Failed to send notification to node (is node alive?): " + nodeId);
                                }
                                catch (IgniteCheckedException e) {
                                    U.error(log, "Failed to send notification to node: " + nodeId, e);
                                }
                            }

                            interval0 = t.get2();
                        }
                    }
                });

                bufCheckThreads.put(routineId, checker);

                checker.start();
            }

            GridContinuousHandler.RegisterStatus status = hnd.register(nodeId, routineId, ctx);

            if (status == GridContinuousHandler.RegisterStatus.DELAYED) {
                info.markDelayedRegister();

                return false;
            }
            else
                return status == GridContinuousHandler.RegisterStatus.REGISTERED;
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

        IgniteThread checker = bufCheckThreads.remove(routineId);

        if (checker != null)
            checker.interrupt();
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
     * @throws IgniteCheckedException In case of error.
     */
    private void sendWithRetries(UUID nodeId, GridContinuousMessage msg, @Nullable Object orderedTopic)
        throws IgniteCheckedException {
        assert nodeId != null;
        assert msg != null;

        ClusterNode node = ctx.discovery().node(nodeId);

        if (node != null)
            sendWithRetries(node, msg, orderedTopic);
        else
            throw new ClusterTopologyCheckedException("Node for provided ID doesn't exist (did it leave the grid?): " + nodeId);
    }

    /**
     * @param node Destination node.
     * @param msg Message.
     * @param orderedTopic Topic for ordered notifications.
     *      If {@code null}, non-ordered message will be sent.
     * @throws IgniteCheckedException In case of error.
     */
    private void sendWithRetries(ClusterNode node, GridContinuousMessage msg, @Nullable Object orderedTopic)
        throws IgniteCheckedException {
        assert node != null;
        assert msg != null;

        sendWithRetries(F.asList(node), msg, orderedTopic);
    }

    /**
     * @param nodes Destination nodes.
     * @param msg Message.
     * @param orderedTopic Topic for ordered notifications.
     *      If {@code null}, non-ordered message will be sent.
     * @throws IgniteCheckedException In case of error.
     */
    private void sendWithRetries(Collection<? extends ClusterNode> nodes, GridContinuousMessage msg,
        @Nullable Object orderedTopic) throws IgniteCheckedException {
        assert !F.isEmpty(nodes);
        assert msg != null;

        if (!msg.messages() &&
            msg.data() != null &&
            (nodes.size() > 1 || !ctx.localNodeId().equals(F.first(nodes).id())))
            msg.dataBytes(marsh.marshal(msg.data()));

        for (ClusterNode node : nodes) {
            int cnt = 0;

            while (cnt <= retryCnt) {
                try {
                    cnt++;

                    if (orderedTopic != null) {
                        ctx.io().sendOrderedMessage(
                            node,
                            orderedTopic,
                            msg,
                            SYSTEM_POOL,
                            0,
                            true);
                    }
                    else
                        ctx.io().send(node, TOPIC_CONTINUOUS, msg, SYSTEM_POOL);

                    break;
                }
                catch (IgniteInterruptedCheckedException e) {
                    throw e;
                }
                catch (IgniteCheckedException e) {
                    if (!ctx.discovery().alive(node.id()))
                        throw new ClusterTopologyCheckedException("Node left grid while sending message to: " + node.id(), e);

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
    static class LocalRoutineInfo implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Projection predicate. */
        private final IgnitePredicate<ClusterNode> prjPred;

        /** Continuous routine handler. */
        private final GridContinuousHandler hnd;

        /** Buffer size. */
        private final int bufSize;

        /** Time interval. */
        private final long interval;

        /** Automatic unsubscribe flag. */
        private boolean autoUnsubscribe;

        /**
         * @param prjPred Projection predicate.
         * @param hnd Continuous routine handler.
         * @param bufSize Buffer size.
         * @param interval Interval.
         * @param autoUnsubscribe Automatic unsubscribe flag.
         */
        LocalRoutineInfo(@Nullable IgnitePredicate<ClusterNode> prjPred,
            GridContinuousHandler hnd,
            int bufSize,
            long interval,
            boolean autoUnsubscribe)
        {
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
         * @return Handler.
         */
        GridContinuousHandler handler() {
            return hnd;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(LocalRoutineInfo.class, this);
        }
    }

    /**
     * Remote routine info.
     */
    private static class RemoteRoutineInfo {
        /** Master node ID. */
        private UUID nodeId;

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

        /** Delayed register flag. */
        private boolean delayedRegister;

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
         * Marks info to be registered when cache is started.
         */
        public void markDelayedRegister() {
            assert hnd.isForQuery();

            delayedRegister = true;
        }

        /**
         * Clears delayed register flag if it was set.
         *
         * @return {@code True} if flag was cleared.
         */
        public boolean clearDelayedRegister() {
            if (delayedRegister) {
                delayedRegister = false;

                return true;
            }

            return false;
        }

        /**
         * @param obj Object to add.
         * @return Object to send or {@code null} if there is nothing to send for now.
         */
        @Nullable Collection<Object> add(@Nullable Object obj) {
            ConcurrentLinkedDeque8 buf0 = null;

            if (buf.sizex() >= bufSize - 1) {
                lock.writeLock().lock();

                try {
                    buf.add(obj);

                    buf0 = buf;

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

            Collection<Object> toSnd = null;

            if (buf0 != null) {
                toSnd = new ArrayList<>(buf0.sizex());

                for (Object o : buf0)
                    toSnd.add(o);
            }

            return toSnd;
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

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(RemoteRoutineInfo.class, this);
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

        /** */
        private Map<UUID, Map<UUID, LocalRoutineInfo>> clientInfos;

        /**
         * Required by {@link Externalizable}.
         */
        public DiscoveryData() {
            // No-op.
        }

        /**
         * @param nodeId Node ID.
         * @param clientInfos Client information.
         */
        DiscoveryData(UUID nodeId, Map<UUID, Map<UUID, LocalRoutineInfo>> clientInfos) {
            assert nodeId != null;

            this.nodeId = nodeId;

            this.clientInfos = clientInfos;

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
            U.writeMap(out, clientInfos);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            nodeId = U.readUuid(in);
            items = U.readCollection(in);
            clientInfos = U.readMap(in);
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
         * @param autoUnsubscribe Automatic unsubscribe flag.
         */
        DiscoveryDataItem(UUID routineId,
            @Nullable IgnitePredicate<ClusterNode> prjPred,
            GridContinuousHandler hnd,
            int bufSize,
            long interval,
            boolean autoUnsubscribe)
        {
            assert routineId != null;
            assert hnd != null;
            assert bufSize > 0;
            assert interval >= 0;

            this.routineId = routineId;
            this.prjPred = prjPred;
            this.hnd = hnd;
            this.bufSize = bufSize;
            this.interval = interval;
            this.autoUnsubscribe = autoUnsubscribe;
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

        /** */
        private GridKernalContext ctx;

        /** Consume ID. */
        private UUID routineId;

        /** Local listener is registered. */
        private volatile boolean loc;

        /** All remote listeners are registered. */
        private volatile boolean rmt;

        /** Timeout object. */
        private volatile GridTimeoutObject timeoutObj;

        /**
         * @param ctx Kernal context.
         * @param routineId Consume ID.
         */
        StartFuture(GridKernalContext ctx, UUID routineId) {
            this.ctx = ctx;

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

        /** */
        private GridKernalContext ctx;

        /**
         * @param ctx Kernal context.
         */
        StopFuture(GridKernalContext ctx) {
            this.ctx = ctx;
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
     * Synchronous message acknowledgement future.
     */
    private static class SyncMessageAckFuture extends GridFutureAdapter<Object> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private UUID nodeId;

        /**
         * @param nodeId Master node ID.
         */
        SyncMessageAckFuture(UUID nodeId) {
            this.nodeId = nodeId;
        }

        /**
         * @return Master node ID.
         */
        UUID nodeId() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(SyncMessageAckFuture.class, this);
        }
    }
}
