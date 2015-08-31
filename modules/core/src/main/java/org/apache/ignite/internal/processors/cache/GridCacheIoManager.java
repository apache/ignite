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

package org.apache.ignite.internal.processors.cache;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedGetFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionSupplyMessage;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetFuture;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.GridSpinReadWriteLock;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.lang.GridPlainRunnable;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;

/**
 * Cache communication manager.
 */
public class GridCacheIoManager extends GridCacheSharedManagerAdapter {
    /** Message ID generator. */
    private static final AtomicLong idGen = new AtomicLong();

    /** Delay in milliseconds between retries. */
    private long retryDelay;

    /** Number of retries using to send messages. */
    private int retryCnt;

    /** Indexed class handlers. */
    private Map<Integer, IgniteBiInClosure[]> idxClsHandlers = new HashMap<>();

    /** Handler registry. */
    private ConcurrentMap<ListenerKey, IgniteBiInClosure<UUID, GridCacheMessage>>
        clsHandlers = new ConcurrentHashMap8<>();

    /** Ordered handler registry. */
    private ConcurrentMap<Object, IgniteBiInClosure<UUID, ? extends GridCacheMessage>> orderedHandlers =
        new ConcurrentHashMap8<>();

    /** Stopping flag. */
    private boolean stopping;

    /** Error flag. */
    private final AtomicBoolean startErr = new AtomicBoolean();

    /** Mutex. */
    private final GridSpinReadWriteLock rw = new GridSpinReadWriteLock();

    /** Deployment enabled. */
    private boolean depEnabled;

    /** Message listener. */
    private GridMessageListener lsnr = new GridMessageListener() {
        @Override public void onMessage(final UUID nodeId, Object msg) {
            if (log.isDebugEnabled())
                log.debug("Received unordered cache communication message [nodeId=" + nodeId +
                    ", locId=" + cctx.localNodeId() + ", msg=" + msg + ']');

            final GridCacheMessage cacheMsg = (GridCacheMessage)msg;

            IgniteInternalFuture<?> fut = null;

            if (cacheMsg.partitionExchangeMessage()) {
                long locTopVer = cctx.discovery().topologyVersion();
                long rmtTopVer = cacheMsg.topologyVersion().topologyVersion();

                if (locTopVer < rmtTopVer) {
                    if (log.isDebugEnabled())
                        log.debug("Received message has higher topology version [msg=" + msg +
                            ", locTopVer=" + locTopVer + ", rmtTopVer=" + rmtTopVer + ']');

                    fut = cctx.discovery().topologyFuture(rmtTopVer);
                }
            }
            else {
                AffinityTopologyVersion locAffVer = cctx.exchange().readyAffinityVersion();
                AffinityTopologyVersion rmtAffVer = cacheMsg.topologyVersion();

                if (locAffVer.compareTo(rmtAffVer) < 0) {
                    if (log.isDebugEnabled())
                        log.debug("Received message has higher affinity topology version [msg=" + msg +
                            ", locTopVer=" + locAffVer + ", rmtTopVer=" + rmtAffVer + ']');

                    fut = cctx.exchange().affinityReadyFuture(rmtAffVer);
                }
            }

            if (fut != null && !fut.isDone()) {
                fut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> t) {
                        cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                            @Override public void run() {
                                handleMessage(nodeId, cacheMsg);
                            }
                        });
                    }
                });

                return;
            }

            handleMessage(nodeId, cacheMsg);
        }
    };

    /**
     * @param nodeId Sender node ID.
     * @param cacheMsg Message.
     */
    @SuppressWarnings("unchecked")
    private void handleMessage(UUID nodeId, GridCacheMessage cacheMsg) {
        int msgIdx = cacheMsg.lookupIndex();

        IgniteBiInClosure<UUID, GridCacheMessage> c = null;

        if (msgIdx >= 0) {
            IgniteBiInClosure[] cacheClsHandlers = idxClsHandlers.get(cacheMsg.cacheId());

            if (cacheClsHandlers != null)
                c = cacheClsHandlers[msgIdx];
        }

        if (c == null)
            c = clsHandlers.get(new ListenerKey(cacheMsg.cacheId(), cacheMsg.getClass()));

        if (c == null) {
            U.warn(log, "Received message without registered handler (will ignore) [msg=" + cacheMsg +
                ", nodeId=" + nodeId + ']');

            return;
        }

        onMessage0(nodeId, cacheMsg, c);
    }

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        retryDelay = cctx.gridConfig().getNetworkSendRetryDelay();
        retryCnt = cctx.gridConfig().getNetworkSendRetryCount();

        depEnabled = cctx.gridDeploy().enabled();

        cctx.gridIO().addMessageListener(TOPIC_CACHE, lsnr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override protected void onKernalStop0(boolean cancel) {
        cctx.gridIO().removeMessageListener(TOPIC_CACHE);

        for (Object ordTopic : orderedHandlers.keySet())
            cctx.gridIO().removeMessageListener(ordTopic);

        boolean interrupted = false;

        // Busy wait is intentional.
        while (true) {
            try {
                if (rw.tryWriteLock(200, TimeUnit.MILLISECONDS))
                    break;
                else
                    Thread.sleep(200);
            }
            catch (InterruptedException ignore) {
                // Preserve interrupt status & ignore.
                // Note that interrupted flag is cleared.
                interrupted = true;
            }
        }

        if (interrupted)
            Thread.currentThread().interrupt();

        try {
            stopping = true;
        }
        finally {
            rw.writeUnlock();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param cacheMsg Cache message.
     * @param c Handler closure.
     */
    @SuppressWarnings({"unchecked", "ConstantConditions", "ThrowableResultOfMethodCallIgnored"})
    private void onMessage0(final UUID nodeId, final GridCacheMessage cacheMsg,
        final IgniteBiInClosure<UUID, GridCacheMessage> c) {
        rw.readLock();

        try {
            if (stopping) {
                if (log.isDebugEnabled())
                    log.debug("Received cache communication message while stopping (will ignore) [nodeId=" +
                        nodeId + ", msg=" + cacheMsg + ']');

                return;
            }

            if (depEnabled)
                cctx.deploy().ignoreOwnership(true);

            unmarshall(nodeId, cacheMsg);

            if (cacheMsg.classError() != null)
                processFailedMessage(nodeId, cacheMsg);
            else {
                if (cacheMsg.allowForStartup())
                    processMessage(nodeId, cacheMsg, c);
                else {
                    IgniteInternalFuture<?> startFut = startFuture(cacheMsg);

                    if (startFut.isDone())
                        processMessage(nodeId, cacheMsg, c);
                    else {
                        if (log.isDebugEnabled())
                            log.debug("Waiting for start future to complete for message [nodeId=" + nodeId +
                                ", locId=" + cctx.localNodeId() + ", msg=" + cacheMsg + ']');

                        // Don't hold this thread waiting for preloading to complete.
                        startFut.listen(new CI1<IgniteInternalFuture<?>>() {
                            @Override public void apply(final IgniteInternalFuture<?> f) {
                                cctx.kernalContext().closure().runLocalSafe(
                                    new GridPlainRunnable() {
                                        @Override public void run() {
                                            rw.readLock();

                                            try {
                                                if (stopping) {
                                                    if (log.isDebugEnabled())
                                                        log.debug("Received cache communication message while stopping " +
                                                            "(will ignore) [nodeId=" + nodeId + ", msg=" + cacheMsg + ']');

                                                    return;
                                                }

                                                f.get();

                                                if (log.isDebugEnabled())
                                                    log.debug("Start future completed for message [nodeId=" + nodeId +
                                                        ", locId=" + cctx.localNodeId() + ", msg=" + cacheMsg + ']');

                                                processMessage(nodeId, cacheMsg, c);
                                            }
                                            catch (IgniteCheckedException e) {
                                                // Log once.
                                                if (startErr.compareAndSet(false, true))
                                                    U.error(log, "Failed to complete preload start future " +
                                                        "(will ignore message) " +
                                                        "[fut=" + f + ", nodeId=" + nodeId + ", msg=" + cacheMsg + ']', e);
                                            }
                                            finally {
                                                rw.readUnlock();
                                            }
                                        }
                                    }
                                );
                            }
                        });
                    }
                }
            }
        }
        catch (Throwable e) {
            U.error(log, "Failed to process message [senderId=" + nodeId + ", messageType=" + cacheMsg.getClass() + ']', e);

            if (e instanceof Error)
                throw (Error)e;
        }
        finally {
            if (depEnabled)
                cctx.deploy().ignoreOwnership(false);

            rw.readUnlock();
        }
    }

    /**
     * Sends response on failed message.
     * @param nodeId node id.
     * @param res response.
     * @param cctx shared context.
     * @param plc grid io policy.
     */
    private void sendResponseOnFailedMessage(UUID nodeId, GridCacheMessage res, GridCacheSharedContext cctx,
        byte plc) {
        try {
            cctx.io().send(nodeId, res, plc);
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send response to node (is node still alive?) [nodeId=" + nodeId +
                ",res=" + res + ']', e);
        }
    }

    /**
     * Processes failed messages.
     * @param nodeId niode id.
     * @param msg message.
     * @throws IgniteCheckedException If failed.
     */
    private void processFailedMessage(UUID nodeId, GridCacheMessage msg) throws IgniteCheckedException {
        GridCacheContext ctx = cctx.cacheContext(msg.cacheId());

        switch (msg.directType()) {
            case 14: {
                GridCacheEvictionRequest req = (GridCacheEvictionRequest)msg;

                GridCacheEvictionResponse res = new GridCacheEvictionResponse(
                    ctx.cacheId(),
                    req.futureId(),
                    req.classError() != null
                );

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 30: {
                GridDhtLockRequest req = (GridDhtLockRequest)msg;

                GridDhtLockResponse res = new GridDhtLockResponse(
                    ctx.cacheId(),
                    req.version(),
                    req.futureId(),
                    req.miniId(),
                    0);

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 34: {
                GridDhtTxPrepareRequest req = (GridDhtTxPrepareRequest)msg;

                GridDhtTxPrepareResponse res = new GridDhtTxPrepareResponse(
                    req.version(),
                    req.futureId(),
                    req.miniId());

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, req.policy());
            }

            break;

            case 38: {
                GridDhtAtomicUpdateRequest req = (GridDhtAtomicUpdateRequest)msg;

                GridDhtAtomicUpdateResponse res = new GridDhtAtomicUpdateResponse(
                    ctx.cacheId(),
                    req.futureVersion());

                res.onError(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 40: {
                GridNearAtomicUpdateRequest req = (GridNearAtomicUpdateRequest)msg;

                GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(
                    ctx.cacheId(),
                    nodeId,
                    req.futureVersion());

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 42: {
                GridDhtForceKeysRequest req = (GridDhtForceKeysRequest)msg;

                GridDhtForceKeysResponse res = new GridDhtForceKeysResponse(
                    ctx.cacheId(),
                    req.futureId(),
                    req.miniId()
                );

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 45: {
                GridDhtPartitionSupplyMessage req = (GridDhtPartitionSupplyMessage)msg;

                U.error(log, "Supply message cannot be unmarshalled.", req.classError());
            }

            break;

            case 49: {
                GridNearGetRequest req = (GridNearGetRequest)msg;

                GridNearGetResponse res = new GridNearGetResponse(
                    ctx.cacheId(),
                    req.futureId(),
                    req.miniId(),
                    req.version());

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 50: {
                GridNearGetResponse res = (GridNearGetResponse)msg;

                GridCacheFuture fut = ctx.mvcc().future(res.version(), res.futureId());

                if (fut == null) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to find future for get response [sender=" + nodeId + ", res=" + res + ']');

                    return;
                }

                res.error(res.classError());

                if (fut instanceof GridNearGetFuture)
                    ((GridNearGetFuture)fut).onResult(nodeId, res);
                else
                    ((GridPartitionedGetFuture)fut).onResult(nodeId, res);
            }

            break;

            case 51: {
                GridNearLockRequest req = (GridNearLockRequest)msg;

                GridNearLockResponse res = new GridNearLockResponse(
                    ctx.cacheId(),
                    req.version(),
                    req.futureId(),
                    req.miniId(),
                    false,
                    0,
                    req.classError(),
                    null);

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 55: {
                GridNearTxPrepareRequest req = (GridNearTxPrepareRequest)msg;

                GridNearTxPrepareResponse res = new GridNearTxPrepareResponse(
                    req.version(),
                    req.futureId(),
                    req.miniId(),
                    req.version(),
                    req.version(),
                    null,
                    null,
                    null);

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, req.policy());
            }

            break;

            default:
                throw new IgniteCheckedException("Failed to send response to node. Unsupported direct type [message="
                    + msg + "]");
        }
    }

    /**
     * @param cacheMsg Cache message to get start future.
     * @return Preloader start future.
     */
    @SuppressWarnings("unchecked")
    private IgniteInternalFuture<Object> startFuture(GridCacheMessage cacheMsg) {
        int cacheId = cacheMsg.cacheId();

        if (cacheId != 0)
            return cctx.cacheContext(cacheId).preloader().startFuture();
        else {
            if (F.eq(cacheMsg.topologyVersion(), AffinityTopologyVersion.NONE))
                return new GridFinishedFuture<>();

            return cctx.preloadersStartFuture(cacheMsg.topologyVersion());
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     * @param c Closure.
     */
    private void processMessage(UUID nodeId, GridCacheMessage msg,
        IgniteBiInClosure<UUID, GridCacheMessage> c) {
        try {
            // We will not end up with storing a bunch of new UUIDs
            // in each cache entry, since node ID is stored in NIO session
            // on handshake.
            c.apply(nodeId, msg);

            if (log.isDebugEnabled())
                log.debug("Finished processing cache communication message [nodeId=" + nodeId + ", msg=" + msg + ']');
        }
        catch (Throwable e) {
            U.error(log, "Failed processing message [senderId=" + nodeId + ", msg=" + msg + ']', e);

            if (e instanceof Error)
                throw e;
        }
        finally {
            // Reset thread local context.
            cctx.tm().resetContext();
            cctx.mvcc().contextReset();

            // Unwind eviction notifications.
            CU.unwindEvicts(cctx);
        }
    }

    /**
     * Pre-processes message prior to send.
     *
     * @param msg Message to send.
     * @param destNodeId Destination node ID.
     * @throws IgniteCheckedException If failed.
     */
    private void onSend(GridCacheMessage msg, @Nullable UUID destNodeId) throws IgniteCheckedException {
        if (msg.messageId() < 0)
            // Generate and set message ID.
            msg.messageId(idGen.incrementAndGet());

        if (destNodeId == null || !cctx.localNodeId().equals(destNodeId)) {
            msg.prepareMarshal(cctx);

            if (depEnabled && msg instanceof GridCacheDeployable)
                cctx.deploy().prepare((GridCacheDeployable)msg);
        }
    }

    /**
     * Sends communication message.
     *
     * @param node Node to send the message to.
     * @param msg Message to send.
     * @param plc IO policy.
     * @throws IgniteCheckedException If sending failed.
     * @throws ClusterTopologyCheckedException If receiver left.
     */
    @SuppressWarnings("unchecked")
    public void send(ClusterNode node, GridCacheMessage msg, byte plc) throws IgniteCheckedException {
        assert !node.isLocal();

        onSend(msg, node.id());

        if (log.isDebugEnabled())
            log.debug("Sending cache message [msg=" + msg + ", node=" + U.toShortString(node) + ']');

        int cnt = 0;

        while (cnt <= retryCnt) {
            try {
                cnt++;

                cctx.gridIO().send(node, TOPIC_CACHE, msg, plc);

                return;
            }
            catch (IgniteCheckedException e) {
                if (!cctx.discovery().alive(node.id()) || !cctx.discovery().pingNode(node.id()))
                    throw new ClusterTopologyCheckedException("Node left grid while sending message to: " + node.id(), e);

                if (cnt == retryCnt)
                    throw e;
                else if (log.isDebugEnabled())
                    log.debug("Failed to send message to node (will retry): " + node.id());
            }

            U.sleep(retryDelay);
        }

        if (log.isDebugEnabled())
            log.debug("Sent cache message [msg=" + msg + ", node=" + U.toShortString(node) + ']');
    }

    /**
     * Sends message and automatically accounts for lefts nodes.
     *
     * @param nodes Nodes to send to.
     * @param msg Message to send.
     * @param plc IO policy.
     * @param fallback Callback for failed nodes.
     * @return {@code True} if nodes are empty or message was sent, {@code false} if
     *      all nodes have left topology while sending this message.
     * @throws IgniteCheckedException If send failed.
     */
    @SuppressWarnings({"BusyWait", "unchecked"})
    public boolean safeSend(Collection<? extends ClusterNode> nodes, GridCacheMessage msg, byte plc,
        @Nullable IgnitePredicate<ClusterNode> fallback) throws IgniteCheckedException {
        assert nodes != null;
        assert msg != null;

        if (nodes.isEmpty()) {
            if (log.isDebugEnabled())
                log.debug("Message will not be sent as collection of nodes is empty: " + msg);

            return true;
        }

        onSend(msg, null);

        if (log.isDebugEnabled())
            log.debug("Sending cache message [msg=" + msg + ", nodes=" + U.toShortString(nodes) + ']');

        final Collection<UUID> leftIds = new GridLeanSet<>();

        int cnt = 0;

        while (cnt < retryCnt) {
            try {
                Collection<? extends ClusterNode> nodesView = F.view(nodes, new P1<ClusterNode>() {
                    @Override public boolean apply(ClusterNode e) {
                        return !leftIds.contains(e.id());
                    }
                });

                cctx.gridIO().send(nodesView, TOPIC_CACHE, msg, plc);

                boolean added = false;

                // Even if there is no exception, we still check here, as node could have
                // ignored the message during stopping.
                for (ClusterNode n : nodes) {
                    if (!leftIds.contains(n.id()) && !cctx.discovery().alive(n.id())) {
                        leftIds.add(n.id());

                        if (fallback != null && !fallback.apply(n))
                            // If fallback signalled to stop.
                            return false;

                        added = true;
                    }
                }

                if (added) {
                    if (!F.exist(F.nodeIds(nodes), F0.not(F.contains(leftIds)))) {
                        if (log.isDebugEnabled())
                            log.debug("Message will not be sent because all nodes left topology [msg=" + msg +
                                ", nodes=" + U.toShortString(nodes) + ']');

                        return false;
                    }
                }

                break;
            }
            catch (IgniteCheckedException e) {
                boolean added = false;

                for (ClusterNode n : nodes) {
                    if (!leftIds.contains(n.id()) &&
                        (!cctx.discovery().alive(n.id()) || !cctx.discovery().pingNode(n.id()))) {
                        leftIds.add(n.id());

                        if (fallback != null && !fallback.apply(n))
                            // If fallback signalled to stop.
                            return false;

                        added = true;
                    }
                }

                if (!added) {
                    cnt++;

                    if (cnt == retryCnt)
                        throw e;

                    U.sleep(retryDelay);
                }

                if (!F.exist(F.nodeIds(nodes), F0.not(F.contains(leftIds)))) {
                    if (log.isDebugEnabled())
                        log.debug("Message will not be sent because all nodes left topology [msg=" + msg + ", nodes=" +
                            U.toShortString(nodes) + ']');

                    return false;
                }

                if (log.isDebugEnabled())
                    log.debug("Message send will be retried [msg=" + msg + ", nodes=" + U.toShortString(nodes) +
                        ", leftIds=" + leftIds + ']');
            }
        }

        if (log.isDebugEnabled())
            log.debug("Sent cache message [msg=" + msg + ", nodes=" + U.toShortString(nodes) + ']');

        return true;
    }

    /**
     * Sends communication message.
     *
     * @param nodeId ID of node to send the message to.
     * @param msg Message to send.
     * @param plc IO policy.
     * @throws IgniteCheckedException If sending failed.
     */
    public void send(UUID nodeId, GridCacheMessage msg, byte plc) throws IgniteCheckedException {
        ClusterNode n = cctx.discovery().node(nodeId);

        if (n == null)
            throw new ClusterTopologyCheckedException("Failed to send message because node left grid [nodeId=" + nodeId +
                ", msg=" + msg + ']');

        send(n, msg, plc);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc IO policy.
     * @param timeout Timeout to keep a message on receiving queue.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendOrderedMessage(ClusterNode node, Object topic, GridCacheMessage msg, byte plc,
        long timeout) throws IgniteCheckedException {
        onSend(msg, node.id());

        int cnt = 0;

        while (cnt <= retryCnt) {
            try {
                cnt++;

                cctx.gridIO().sendOrderedMessage(node, topic, msg, plc, timeout, false);

                if (log.isDebugEnabled())
                    log.debug("Sent ordered cache message [topic=" + topic + ", msg=" + msg +
                        ", nodeId=" + node.id() + ']');

                return;
            }
            catch (IgniteCheckedException e) {
                if (cctx.discovery().node(node.id()) == null)
                    throw new ClusterTopologyCheckedException("Node left grid while sending ordered message to: " + node.id(), e);

                if (cnt == retryCnt)
                    throw e;
                else if (log.isDebugEnabled())
                    log.debug("Failed to send message to node (will retry): " + node.id());
            }

            U.sleep(retryDelay);
        }
    }

    /**
     * @return ID that auto-grows based on local counter and counters received
     *      from other nodes.
     */
    public long nextIoId() {
        return idGen.incrementAndGet();
    }

    /**
     * Sends message without retries and node ping in case of error.
     *
     * @param node Node to send message to.
     * @param msg Message.
     * @param plc IO policy.
     * @throws IgniteCheckedException If send failed.
     */
    public void sendNoRetry(ClusterNode node,
        GridCacheMessage msg,
        byte plc)
        throws IgniteCheckedException
    {
        assert node != null;
        assert msg != null;

        onSend(msg, null);

        try {
            cctx.gridIO().send(node, TOPIC_CACHE, msg, plc);

            if (log.isDebugEnabled())
                log.debug("Sent cache message [msg=" + msg + ", node=" + U.toShortString(node) + ']');
        }
        catch (IgniteCheckedException e) {
            if (!cctx.discovery().alive(node.id()))
                throw new ClusterTopologyCheckedException("Node left grid while sending message to: " + node.id(), e);
            else
                throw e;
        }
    }

    /**
     * Adds message handler.
     *
     * @param cacheId Cache ID.
     * @param type Type of message.
     * @param c Handler.
     */
    @SuppressWarnings({"unchecked"})
    public void addHandler(
        int cacheId,
        Class<? extends GridCacheMessage> type,
        IgniteBiInClosure<UUID, ? extends GridCacheMessage> c) {
        int msgIdx = messageIndex(type);

        if (msgIdx != -1) {
            IgniteBiInClosure[] cacheClsHandlers = idxClsHandlers.get(cacheId);

            if (cacheClsHandlers == null) {
                cacheClsHandlers = new IgniteBiInClosure[GridCacheMessage.MAX_CACHE_MSG_LOOKUP_INDEX];

                idxClsHandlers.put(cacheId, cacheClsHandlers);
            }

            if (cacheClsHandlers[msgIdx] != null)
                throw new IgniteException("Duplicate cache message ID found [cacheId=" + cacheId +
                    ", type=" + type + ']');

            cacheClsHandlers[msgIdx] = c;

            return;
        }
        else {
            ListenerKey key = new ListenerKey(cacheId, type);

            if (clsHandlers.putIfAbsent(key,
                (IgniteBiInClosure<UUID, GridCacheMessage>)c) != null)
                assert false : "Handler for class already registered [cacheId=" + cacheId + ", cls=" + type +
                    ", old=" + clsHandlers.get(key) + ", new=" + c + ']';
        }

        if (log != null && log.isDebugEnabled())
            log.debug("Registered cache communication handler [cacheId=" + cacheId + ", type=" + type +
                ", msgIdx=" + msgIdx + ", handler=" + c + ']');
    }

    /**
     * @param cacheId Cache ID to remove handlers for.
     */
    public void removeHandlers(int cacheId) {
        assert cacheId != 0;

        idxClsHandlers.remove(cacheId);

        for (Iterator<ListenerKey> iter = clsHandlers.keySet().iterator(); iter.hasNext(); ) {
            ListenerKey key = iter.next();

            if (key.cacheId == cacheId)
                iter.remove();
        }
    }

    /**
     * @param cacheId Cache ID to remove handlers for.
     * @param type Message type.
     */
    public void removeHandler(int cacheId, Class<? extends GridCacheMessage> type) {
        clsHandlers.remove(new ListenerKey(cacheId, type));
    }

    /**
     * @param msgCls Message class to check.
     * @return Message index.
     */
    private int messageIndex(Class<?> msgCls) {
        try {
            Integer msgIdx = U.field(msgCls, GridCacheMessage.CACHE_MSG_INDEX_FIELD_NAME);

            if (msgIdx == null || msgIdx < 0)
                return -1;

            return msgIdx;
        }
        catch (IgniteCheckedException ignored) {
            return -1;
        }
    }

    /**
     * Adds ordered message handler.
     *
     * @param topic Topic.
     * @param c Handler.
     */
    @SuppressWarnings({"unchecked"})
    public void addOrderedHandler(Object topic, IgniteBiInClosure<UUID, ? extends GridCacheMessage> c) {
        if (orderedHandlers.putIfAbsent(topic, c) == null) {
            cctx.gridIO().addMessageListener(topic, new OrderedMessageListener(
                (IgniteBiInClosure<UUID, GridCacheMessage>)c));

            if (log != null && log.isDebugEnabled())
                log.debug("Registered ordered cache communication handler [topic=" + topic + ", handler=" + c + ']');
        }
        else if (log != null)
            U.warn(log, "Failed to register ordered cache communication handler because it is already " +
                "registered for this topic [topic=" + topic + ", handler=" + c + ']');
    }

    /**
     * Removed ordered message handler.
     *
     * @param topic Topic.
     */
    public void removeOrderedHandler(Object topic) {
        if (orderedHandlers.remove(topic) != null) {
            cctx.gridIO().removeMessageListener(topic);

            if (log != null && log.isDebugEnabled())
                log.debug("Unregistered ordered cache communication handler for topic:" + topic);
        }
        else if (log != null)
            U.warn(log, "Failed to unregister ordered cache communication handler because it was not found " +
                "for topic: " + topic);
    }

    /**
     * @param nodeId Sender node ID.
     * @param cacheMsg Message.
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings({"ErrorNotRethrown", "unchecked"})
    private void unmarshall(UUID nodeId, GridCacheMessage cacheMsg) throws IgniteCheckedException {
        if (cctx.localNodeId().equals(nodeId))
            return;

        GridDeploymentInfo bean = cacheMsg.deployInfo();

        if (bean != null) {
            assert depEnabled : "Received deployment info while peer class loading is disabled [nodeId=" + nodeId +
                ", msg=" + cacheMsg + ']';

            cctx.deploy().p2pContext(nodeId, bean.classLoaderId(), bean.userVersion(),
                bean.deployMode(), bean.participants(), bean.localDeploymentOwner());

            if (log.isDebugEnabled())
                log.debug("Set P2P context [senderId=" + nodeId + ", msg=" + cacheMsg + ']');
        }

        try {
            cacheMsg.finishUnmarshal(cctx, cctx.deploy().globalLoader());
        }
        catch (IgniteCheckedException e) {
            cacheMsg.onClassError(e);
        }
        catch (Error e) {
            if (cacheMsg.ignoreClassErrors() && X.hasCause(e, NoClassDefFoundError.class,
                UnsupportedClassVersionError.class))
                cacheMsg.onClassError(new IgniteCheckedException("Failed to load class during unmarshalling: " + e, e));
            else
                throw e;
        }
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Cache IO manager memory stats [grid=" + cctx.gridName() + ']');
        X.println(">>>   clsHandlersSize: " + clsHandlers.size());
        X.println(">>>   orderedHandlersSize: " + orderedHandlers.size());
    }

    /**
     * Ordered message listener.
     */
    private class OrderedMessageListener implements GridMessageListener {
        /** */
        private final IgniteBiInClosure<UUID, GridCacheMessage> c;

        /**
         * @param c Handler closure.
         */
        OrderedMessageListener(IgniteBiInClosure<UUID, GridCacheMessage> c) {
            this.c = c;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"CatchGenericClass", "unchecked"})
        @Override public void onMessage(final UUID nodeId, Object msg) {
            if (log.isDebugEnabled())
                log.debug("Received cache ordered message [nodeId=" + nodeId + ", msg=" + msg + ']');

            final GridCacheMessage cacheMsg = (GridCacheMessage)msg;

            onMessage0(nodeId, cacheMsg, c);
        }
    }

    /**
     *
     */
    private static class ListenerKey {
        /** Cache ID. */
        private int cacheId;

        /** Message class. */
        private Class<? extends GridCacheMessage> msgCls;

        /**
         * @param cacheId Cache ID.
         * @param msgCls Message class.
         */
        private ListenerKey(int cacheId, Class<? extends GridCacheMessage> msgCls) {
            this.cacheId = cacheId;
            this.msgCls = msgCls;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof ListenerKey))
                return false;

            ListenerKey that = (ListenerKey)o;

            return cacheId == that.cacheId && msgCls.equals(that.msgCls);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = cacheId;

            res = 31 * res + msgCls.hashCode();

            return res;
        }
    }
}