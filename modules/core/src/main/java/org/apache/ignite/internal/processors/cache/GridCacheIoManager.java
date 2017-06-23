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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryObjectException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.deployment.GridDeploymentInfo;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.CacheGetFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtAffinityAssignmentRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridPartitionedSingleGetFuture;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicAbstractUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicNearResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridDhtAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicAbstractUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicCheckUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicFullUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateFilterRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateInvokeRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicSingleUpdateRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.GridNearAtomicUpdateResponse;
import org.apache.ignite.internal.processors.cache.distributed.dht.atomic.UpdateErrors;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysRequest;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtForceKeysResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearLockResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearSingleGetResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxFinishResponse;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareRequest;
import org.apache.ignite.internal.processors.cache.distributed.near.GridNearTxPrepareResponse;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryRequest;
import org.apache.ignite.internal.processors.cache.query.GridCacheQueryResponse;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxState;
import org.apache.ignite.internal.processors.cache.transactions.IgniteTxStateAware;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.F0;
import org.apache.ignite.internal.util.GridLeanSet;
import org.apache.ignite.internal.util.StripedCompositeReadWriteLock;
import org.apache.ignite.internal.util.typedef.CI1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiInClosure;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.thread.IgniteThread;
import org.jetbrains.annotations.Nullable;
import org.jsr166.ConcurrentHashMap8;

import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE;

/**
 * Cache communication manager.
 */
public class GridCacheIoManager extends GridCacheSharedManagerAdapter {
    /** Communication topic prefix for distributed queries. */
    private static final String QUERY_TOPIC_PREFIX = "QUERY";

    /** Message ID generator. */
    private static final AtomicLong idGen = new AtomicLong();

    /** */
    private static final int MAX_STORED_PENDING_MESSAGES = 100;

    /** Delay in milliseconds between retries. */
    private long retryDelay;

    /** Number of retries using to send messages. */
    private int retryCnt;

    /** */
    private final MessageHandlers cacheHandlers = new MessageHandlers();

    /** */
    private final MessageHandlers grpHandlers = new MessageHandlers();

    /** Stopping flag. */
    private boolean stopping;

    /** Mutex. */
    private final StripedCompositeReadWriteLock rw =
        new StripedCompositeReadWriteLock(Runtime.getRuntime().availableProcessors());

    /** Deployment enabled. */
    private boolean depEnabled;

    /** */
    private final List<GridCacheMessage> pendingMsgs = new ArrayList<>(MAX_STORED_PENDING_MESSAGES);

    /**
     *
     */
    public void dumpPendingMessages() {
        synchronized (pendingMsgs) {
            if (pendingMsgs.isEmpty())
                return;

            diagnosticLog.info("Pending cache messages waiting for exchange [" +
                "readyVer=" + cctx.exchange().readyAffinityVersion() +
                ", discoVer=" + cctx.discovery().topologyVersion() + ']');

            for (GridCacheMessage msg : pendingMsgs)
                diagnosticLog.info("Message [waitVer=" + msg.topologyVersion() + ", msg=" + msg + ']');
        }
    }

    /** Message listener. */
    private GridMessageListener lsnr = new GridMessageListener() {
        @Override public void onMessage(final UUID nodeId, final Object msg) {
            if (log.isDebugEnabled())
                log.debug("Received unordered cache communication message [nodeId=" + nodeId +
                    ", locId=" + cctx.localNodeId() + ", msg=" + msg + ']');

            final GridCacheMessage cacheMsg = (GridCacheMessage)msg;

            IgniteInternalFuture<?> fut = null;

            if (cacheMsg.partitionExchangeMessage()) {
                if (cacheMsg instanceof GridDhtAffinityAssignmentRequest) {
                    GridDhtAffinityAssignmentRequest msg0 = (GridDhtAffinityAssignmentRequest)cacheMsg;

                    assert cacheMsg.topologyVersion() != null : cacheMsg;

                    AffinityTopologyVersion startTopVer = new AffinityTopologyVersion(cctx.localNode().order());

                    CacheGroupDescriptor desc = cctx.cache().cacheGroupDescriptors().get(msg0.groupId());

                    if (desc != null) {
                        if (desc.startTopologyVersion() != null)
                            startTopVer = desc.startTopologyVersion();
                        else if (desc.receivedFromStartVersion() != null)
                            startTopVer = desc.receivedFromStartVersion();
                    }

                    // Need to wait for exchange to avoid race between cache start and affinity request.
                    fut = cctx.exchange().affinityReadyFuture(startTopVer);

                    if (fut != null && !fut.isDone()) {
                        if (log.isDebugEnabled()) {
                            log.debug("Wait for exchange before processing message [msg=" + msg +
                                ", node=" + nodeId +
                                ", waitVer=" + startTopVer +
                                ", cacheDesc=" + descriptorForMessage(cacheMsg) + ']');
                        }

                        fut.listen(new CI1<IgniteInternalFuture<?>>() {
                            @Override public void apply(IgniteInternalFuture<?> fut) {
                                cctx.kernalContext().closure().runLocalSafe(new Runnable() {
                                    @Override public void run() {
                                        handleMessage(nodeId, cacheMsg);
                                    }
                                });
                            }
                        });

                        return;
                    }
                }

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
                    IgniteLogger log = cacheMsg.messageLogger(cctx);

                    if (log.isDebugEnabled()) {
                        StringBuilder msg0 = new StringBuilder("Received message has higher affinity topology version [");

                        appendMessageInfo(cacheMsg, nodeId, msg0);

                        msg0.append(", locTopVer=").append(locAffVer).
                            append(", rmtTopVer=").append(rmtAffVer).
                            append(']');

                        log.debug(msg0.toString());
                    }

                    fut = cctx.exchange().affinityReadyFuture(rmtAffVer);
                }
            }

            if (fut != null && !fut.isDone()) {
                synchronized (pendingMsgs) {
                    if (pendingMsgs.size() < MAX_STORED_PENDING_MESSAGES)
                        pendingMsgs.add(cacheMsg);
                }

                Thread curThread = Thread.currentThread();

                final int stripe = curThread instanceof IgniteThread ? ((IgniteThread)curThread).stripe() : -1;

                fut.listen(new CI1<IgniteInternalFuture<?>>() {
                    @Override public void apply(IgniteInternalFuture<?> t) {
                        Runnable c = new Runnable() {
                            @Override public void run() {
                                synchronized (pendingMsgs) {
                                    pendingMsgs.remove(cacheMsg);
                                }

                                IgniteLogger log = cacheMsg.messageLogger(cctx);

                                if (log.isDebugEnabled()) {
                                    StringBuilder msg0 = new StringBuilder("Process cache message after wait for " +
                                            "affinity topology version [");

                                    appendMessageInfo(cacheMsg, nodeId, msg0).append(']');

                                    log.debug(msg0.toString());
                                }

                                handleMessage(nodeId, cacheMsg);
                            }
                        };

                        if (stripe >= 0)
                            cctx.kernalContext().getStripedExecutorService().execute(stripe, c);
                        else
                            cctx.kernalContext().closure().runLocalSafe(c);
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
        handleMessage(nodeId, cacheMsg, cacheMsg.cacheGroupMessage() ? grpHandlers : cacheHandlers);
    }

    /**
     * @param nodeId Sender node ID.
     * @param cacheMsg Message.
     * @param msgHandlers Message handlers.
     */
    @SuppressWarnings("unchecked")
    private void handleMessage(UUID nodeId, GridCacheMessage cacheMsg, MessageHandlers msgHandlers) {
        Lock lock = rw.readLock();

        lock.lock();

        try {
            int msgIdx = cacheMsg.lookupIndex();

            IgniteBiInClosure<UUID, GridCacheMessage> c = null;

            if (msgIdx >= 0) {
                Map<Integer, IgniteBiInClosure[]> idxClsHandlers0 = msgHandlers.idxClsHandlers;

                IgniteBiInClosure[] cacheClsHandlers = idxClsHandlers0.get(cacheMsg.handlerId());

                if (cacheClsHandlers != null)
                    c = cacheClsHandlers[msgIdx];
            }

            if (c == null)
                c = msgHandlers.clsHandlers.get(new ListenerKey(cacheMsg.handlerId(), cacheMsg.getClass()));

            if (c == null) {
                if (processMissedHandler(nodeId, cacheMsg))
                    return;

                IgniteLogger log = cacheMsg.messageLogger(cctx);

                StringBuilder msg0 = new StringBuilder("Received message without registered handler (will ignore) [");

                appendMessageInfo(cacheMsg, nodeId, msg0);

                msg0.append(", locTopVer=").append(cctx.exchange().readyAffinityVersion()).
                    append(", msgTopVer=").append(cacheMsg.topologyVersion()).
                    append(", desc=").append(descriptorForMessage(cacheMsg)).
                    append(']');

                msg0.append(U.nl()).append("Registered listeners:");

                Map<Integer, IgniteBiInClosure[]> idxClsHandlers0 = msgHandlers.idxClsHandlers;

                for (Map.Entry<Integer, IgniteBiInClosure[]> e : idxClsHandlers0.entrySet())
                    msg0.append(U.nl()).append(e.getKey()).append("=").append(Arrays.toString(e.getValue()));

                if (cctx.kernalContext().isStopping()) {
                    if (log.isDebugEnabled())
                        log.debug(msg0.toString());
                }
                else
                    U.error(log, msg0.toString());

                return;
            }

            onMessage0(nodeId, cacheMsg, c);
        }
        finally {
            lock.unlock();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param cacheMsg Message.
     * @return {@code True} if message processed.
     */
    private boolean processMissedHandler(UUID nodeId, GridCacheMessage cacheMsg) {
        // It is possible to receive reader update after client near cache was closed.
        if (cacheMsg instanceof GridDhtAtomicAbstractUpdateRequest) {
            GridDhtAtomicAbstractUpdateRequest req = (GridDhtAtomicAbstractUpdateRequest)cacheMsg;

            if (req.nearSize() > 0) {
                List<KeyCacheObject> nearEvicted = new ArrayList<>(req.nearSize());

                for (int i = 0; i < req.nearSize(); i++)
                    nearEvicted.add(req.nearKey(i));

                GridDhtAtomicUpdateResponse dhtRes = new GridDhtAtomicUpdateResponse(req.cacheId(),
                    req.partition(),
                    req.futureId(),
                    false);

                dhtRes.nearEvicted(nearEvicted);

                sendMessageForMissedHandler(cacheMsg,
                    nodeId,
                    dhtRes,
                    nodeId,
                    GridIoPolicy.SYSTEM_POOL);

                if (req.nearNodeId() != null) {
                    GridDhtAtomicNearResponse nearRes = new GridDhtAtomicNearResponse(req.cacheId(),
                        req.partition(),
                        req.nearFutureId(),
                        nodeId,
                        req.flags());

                    sendMessageForMissedHandler(cacheMsg,
                        nodeId,
                        nearRes,
                        req.nearNodeId(),
                        GridIoPolicy.SYSTEM_POOL);
                }

                return true;
            }
        }

        return false;
    }

    /**
     * @param origMsg Message without handler.
     * @param origMsgNode Node sent {@code origMsg}.
     * @param nodeId Target node ID.
     * @param msg Response.
     * @param plc Policy.
     */
    private void sendMessageForMissedHandler(
        GridCacheMessage origMsg,
        UUID origMsgNode,
        GridCacheMessage msg,
        UUID nodeId,
        byte plc) {
        IgniteLogger log = msg.messageLogger(cctx);

        try {
            if (log.isDebugEnabled()) {
                log.debug("Received message without registered handler, " +
                    "send response [locTopVer=" + cctx.exchange().readyAffinityVersion() +
                    ", msgTopVer=" + origMsg.topologyVersion() +
                    ", node=" + origMsgNode +
                    ", msg=" + origMsg +
                    ", resNode=" + nodeId +
                    ", res=" + msg + ']');
            }

            send(nodeId, msg, plc);
        }
        catch (ClusterTopologyCheckedException e) {
            if (log.isDebugEnabled())
                log.debug("Failed to send response, node left [nodeId=" + nodeId + ", msg=" + msg + ']');
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to send response [nodeId=" + nodeId + ", msg=" + msg + ", err=" + e + ']');
        }
    }

    /** {@inheritDoc} */
    @Override public void start0() throws IgniteCheckedException {
        retryDelay = cctx.gridConfig().getNetworkSendRetryDelay();
        retryCnt = cctx.gridConfig().getNetworkSendRetryCount();

        depEnabled = cctx.gridDeploy().enabled();

        cctx.gridIO().addMessageListener(TOPIC_CACHE, lsnr);
    }

    /**
     *
     */
    public void writeLock() {
        boolean interrupted = false;

        // Busy wait is intentional.
        while (true) {
            try {
                if (rw.writeLock().tryLock(200, TimeUnit.MILLISECONDS))
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
    }

    /**
     *
     */
    public void writeUnlock() {
        rw.writeLock().unlock();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override protected void onKernalStop0(boolean cancel) {
        cctx.gridIO().removeMessageListener(TOPIC_CACHE);

        for (Object ordTopic : cacheHandlers.orderedHandlers.keySet())
            cctx.gridIO().removeMessageListener(ordTopic);

        for (Object ordTopic : grpHandlers.orderedHandlers.keySet())
            cctx.gridIO().removeMessageListener(ordTopic);

        writeLock();

        try {
            stopping = true;
        }
        finally {
            rw.writeLock().unlock();
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
                processFailedMessage(nodeId, cacheMsg, c);
            else
                processMessage(nodeId, cacheMsg, c);
        }
        catch (Throwable e) {
            U.error(log, "Failed to process message [senderId=" + nodeId + ", messageType=" + cacheMsg.getClass() + ']', e);

            if (e instanceof Error)
                throw (Error)e;
        }
        finally {
            if (depEnabled)
                cctx.deploy().ignoreOwnership(false);
        }
    }

    /**
     * Sends response on failed message.
     *
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
     * @param cacheMsg Cache message.
     * @param nodeId Node ID.
     * @param builder Message builder.
     * @return Message builder.
     */
    private StringBuilder appendMessageInfo(GridCacheMessage cacheMsg, UUID nodeId, StringBuilder builder) {
        if (txId(cacheMsg) != null) {
            builder.append("txId=").append(txId(cacheMsg)).
                append(", dhtTxId=").append(dhtTxId(cacheMsg)).
                append(", msg=").append(cacheMsg);
        }
        else if (atomicFututeId(cacheMsg) != null) {
            builder.append("futId=").append(atomicFututeId(cacheMsg)).
                append(", writeVer=").append(atomicWriteVersion(cacheMsg)).
                append(", msg=").append(cacheMsg);
        }
        else
            builder.append("msg=").append(cacheMsg);

        builder.append(", node=").append(nodeId);

        return builder;
    }

    /**
     * @param cacheMsg Cache message.
     * @return Transaction ID if applicable for message.
     */
    @Nullable private GridCacheVersion txId(GridCacheMessage cacheMsg) {
        if (cacheMsg instanceof GridDhtTxPrepareRequest)
            return ((GridDhtTxPrepareRequest)cacheMsg).nearXidVersion();
        else if (cacheMsg instanceof GridNearTxPrepareRequest)
            return ((GridNearTxPrepareRequest)cacheMsg).version();
        else if (cacheMsg instanceof GridNearTxPrepareResponse)
            return ((GridNearTxPrepareResponse)cacheMsg).version();
        else if (cacheMsg instanceof GridNearTxFinishRequest)
            return ((GridNearTxFinishRequest)cacheMsg).version();
        else if (cacheMsg instanceof GridNearTxFinishResponse)
            return ((GridNearTxFinishResponse)cacheMsg).xid();

        return null;
    }

    /**
     * @param cacheMsg Cache message.
     * @return Transaction ID if applicable for message.
     */
    @Nullable private GridCacheVersion dhtTxId(GridCacheMessage cacheMsg) {
        if (cacheMsg instanceof GridDhtTxPrepareRequest)
            return ((GridDhtTxPrepareRequest)cacheMsg).version();
        else if (cacheMsg instanceof GridDhtTxPrepareResponse)
            return ((GridDhtTxPrepareResponse)cacheMsg).version();
        else if (cacheMsg instanceof GridDhtTxFinishRequest)
            return ((GridDhtTxFinishRequest)cacheMsg).version();
        else if (cacheMsg instanceof GridDhtTxFinishResponse)
            return ((GridDhtTxFinishResponse)cacheMsg).xid();

        return null;
    }

    /**
     * @param cacheMsg Cache message.
     * @return Atomic future ID if applicable for message.
     */
    @Nullable private Long atomicFututeId(GridCacheMessage cacheMsg) {
        if (cacheMsg instanceof GridNearAtomicAbstractUpdateRequest)
            return ((GridNearAtomicAbstractUpdateRequest)cacheMsg).futureId();
        else if (cacheMsg instanceof GridNearAtomicUpdateResponse)
            return ((GridNearAtomicUpdateResponse) cacheMsg).futureId();
        else if (cacheMsg instanceof GridDhtAtomicAbstractUpdateRequest)
            return ((GridDhtAtomicAbstractUpdateRequest)cacheMsg).futureId();
        else if (cacheMsg instanceof GridDhtAtomicUpdateResponse)
            return ((GridDhtAtomicUpdateResponse) cacheMsg).futureId();
        else if (cacheMsg instanceof GridNearAtomicCheckUpdateRequest)
            return ((GridNearAtomicCheckUpdateRequest)cacheMsg).futureId();

        return null;
    }


    /**
     * @param cacheMsg Cache message.
     * @return Atomic future ID if applicable for message.
     */
    @Nullable private GridCacheVersion atomicWriteVersion(GridCacheMessage cacheMsg) {
        if (cacheMsg instanceof GridDhtAtomicAbstractUpdateRequest)
            return ((GridDhtAtomicAbstractUpdateRequest)cacheMsg).writeVersion();

        return null;
    }

    /**
     * Processes failed messages.
     *
     * @param nodeId Node ID.
     * @param msg Message.
     * @throws IgniteCheckedException If failed.
     */
    private void processFailedMessage(UUID nodeId, GridCacheMessage msg, IgniteBiInClosure<UUID, GridCacheMessage> c)
        throws IgniteCheckedException {
        assert msg != null;

        GridCacheContext ctx = msg instanceof GridCacheIdMessage ?
            cctx.cacheContext(((GridCacheIdMessage)msg).cacheId()) : null;

        switch (msg.directType()) {
            case 30: {
                GridDhtLockRequest req = (GridDhtLockRequest)msg;

                GridDhtLockResponse res = new GridDhtLockResponse(
                    req.cacheId(),
                    req.version(),
                    req.futureId(),
                    req.miniId(),
                    0,
                    ctx.deploymentEnabled());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 34: {
                GridDhtTxPrepareRequest req = (GridDhtTxPrepareRequest)msg;

                GridDhtTxPrepareResponse res = new GridDhtTxPrepareResponse(
                    req.partition(),
                    req.version(),
                    req.futureId(),
                    req.miniId(),
                    req.deployInfo() != null);

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, req.policy());
            }

            break;

            case 38: {
                GridDhtAtomicUpdateRequest req = (GridDhtAtomicUpdateRequest)msg;

                GridDhtAtomicUpdateResponse res = new GridDhtAtomicUpdateResponse(
                    req.cacheId(),
                    req.partition(),
                    req.futureId(),
                    false);

                res.onError(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());

                if (req.nearNodeId() != null) {
                    GridDhtAtomicNearResponse nearRes = new GridDhtAtomicNearResponse(req.cacheId(),
                        req.partition(),
                        req.nearFutureId(),
                        nodeId,
                        req.flags());

                    nearRes.errors(new UpdateErrors(req.classError()));

                    sendResponseOnFailedMessage(req.nearNodeId(), nearRes, cctx, ctx.ioPolicy());
                }
            }

            break;

            case 40: {
                GridNearAtomicFullUpdateRequest req = (GridNearAtomicFullUpdateRequest)msg;

                GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(
                    req.cacheId(),
                    nodeId,
                    req.futureId(),
                    req.partition(),
                    false,
                    false);

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 42: {
                GridDhtForceKeysRequest req = (GridDhtForceKeysRequest)msg;

                GridDhtForceKeysResponse res = new GridDhtForceKeysResponse(
                    req.cacheId(),
                    req.futureId(),
                    req.miniId(),
                    false
                );

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 45: {
                processMessage(nodeId, msg, c);// Will be handled by Rebalance Demander.
            }

            break;

            case 49: {
                GridNearGetRequest req = (GridNearGetRequest)msg;

                GridNearGetResponse res = new GridNearGetResponse(
                    req.cacheId(),
                    req.futureId(),
                    req.miniId(),
                    req.version(),
                    req.deployInfo() != null);

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 50: {
                GridNearGetResponse res = (GridNearGetResponse)msg;

                CacheGetFuture fut = (CacheGetFuture)cctx.mvcc().future(res.futureId());

                if (fut == null) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to find future for get response [sender=" + nodeId + ", res=" + res + ']');

                    return;
                }

                res.error(res.classError());

                fut.onResult(nodeId, res);
            }

            break;

            case 51: {
                GridNearLockRequest req = (GridNearLockRequest)msg;

                GridNearLockResponse res = new GridNearLockResponse(
                    req.cacheId(),
                    req.version(),
                    req.futureId(),
                    req.miniId(),
                    false,
                    0,
                    req.classError(),
                    null,
                    false);

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 55: {
                GridNearTxPrepareRequest req = (GridNearTxPrepareRequest)msg;

                GridNearTxPrepareResponse res = new GridNearTxPrepareResponse(
                    req.partition(),
                    req.version(),
                    req.futureId(),
                    req.miniId(),
                    req.version(),
                    req.version(),
                    null,
                    null,
                    null,
                    false,
                    req.deployInfo() != null);

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, req.policy());
            }

            break;

            case 58: {
                GridCacheQueryRequest req = (GridCacheQueryRequest)msg;

                GridCacheQueryResponse res = new GridCacheQueryResponse(
                    req.cacheId(),
                    req.id(),
                    req.classError(),
                    cctx.deploymentEnabled());

                cctx.io().sendOrderedMessage(
                    cctx.node(nodeId),
                    TOPIC_CACHE.topic(QUERY_TOPIC_PREFIX, nodeId, req.id()),
                    res,
                    ctx.ioPolicy(),
                    Long.MAX_VALUE);
            }

            break;

            case 114: {
                processMessage(nodeId, msg, c);// Will be handled by Rebalance Demander.
            }

            break;

            case 116: {
                GridNearSingleGetRequest req = (GridNearSingleGetRequest)msg;

                GridNearSingleGetResponse res = new GridNearSingleGetResponse(
                    req.cacheId(),
                    req.futureId(),
                    req.topologyVersion(),
                    null,
                    false,
                    req.deployInfo() != null);

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 117: {
                GridNearSingleGetResponse res = (GridNearSingleGetResponse)msg;

                GridPartitionedSingleGetFuture fut = (GridPartitionedSingleGetFuture)cctx.mvcc()
                    .future(new IgniteUuid(IgniteUuid.VM_ID, res.futureId()));

                if (fut == null) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to find future for get response [sender=" + nodeId + ", res=" + res + ']');

                    return;
                }

                res.error(res.classError());

                fut.onResult(nodeId, res);
            }

            break;

            case 125: {
                GridNearAtomicSingleUpdateRequest req = (GridNearAtomicSingleUpdateRequest)msg;

                GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(
                    req.cacheId(),
                    nodeId,
                    req.futureId(),
                    req.partition(),
                    false,
                    false);

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 126: {
                GridNearAtomicSingleUpdateInvokeRequest req = (GridNearAtomicSingleUpdateInvokeRequest)msg;

                GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(
                    req.cacheId(),
                    nodeId,
                    req.futureId(),
                    req.partition(),
                    false,
                    false);

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case 127: {
                GridNearAtomicSingleUpdateFilterRequest req = (GridNearAtomicSingleUpdateFilterRequest)msg;

                GridNearAtomicUpdateResponse res = new GridNearAtomicUpdateResponse(
                    req.cacheId(),
                    nodeId,
                    req.futureId(),
                    req.partition(),
                    false,
                    false);

                res.error(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());
            }

            break;

            case -36: {
                GridDhtAtomicSingleUpdateRequest req = (GridDhtAtomicSingleUpdateRequest)msg;

                GridDhtAtomicUpdateResponse res = new GridDhtAtomicUpdateResponse(
                    req.cacheId(),
                    req.partition(),
                    req.futureId(),
                    false);

                res.onError(req.classError());

                sendResponseOnFailedMessage(nodeId, res, cctx, ctx.ioPolicy());

                if (req.nearNodeId() != null) {
                    GridDhtAtomicNearResponse nearRes = new GridDhtAtomicNearResponse(req.cacheId(),
                        req.partition(),
                        req.nearFutureId(),
                        nodeId,
                        req.flags());

                    nearRes.errors(new UpdateErrors(req.classError()));

                    sendResponseOnFailedMessage(req.nearNodeId(), nearRes, cctx, ctx.ioPolicy());
                }
            }

            break;

            default:
                throw new IgniteCheckedException("Failed to send response to node. Unsupported direct type [message="
                    + msg + "]", msg.classError());
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     * @param c Closure.
     */
    private void processMessage(UUID nodeId, GridCacheMessage msg, IgniteBiInClosure<UUID, GridCacheMessage> c) {
        try {
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

            GridCacheMvccManager mvcc = cctx.mvcc();

            if (mvcc != null)
                mvcc.contextReset();

            // Unwind eviction notifications.
            if (msg instanceof IgniteTxStateAware) {
                IgniteTxState txState = ((IgniteTxStateAware)msg).txState();

                if (txState != null)
                    txState.unwindEvicts(cctx);
            }
            else if (msg instanceof GridCacheIdMessage) {
                GridCacheContext ctx = cctx.cacheContext(((GridCacheIdMessage)msg).cacheId());

                if (ctx != null)
                    CU.unwindEvicts(ctx);
            }
        }
    }

    /**
     * Pre-processes message prior to send.
     *
     * @param msg Message to send.
     * @param destNodeId Destination node ID.
     * @return {@code True} if should send message.
     * @throws IgniteCheckedException If failed.
     */
    private boolean onSend(GridCacheMessage msg, @Nullable UUID destNodeId) throws IgniteCheckedException {
        if (msg.error() != null && cctx.kernalContext().isStopping())
            return false;

        if (msg.messageId() < 0)
            // Generate and set message ID.
            msg.messageId(idGen.incrementAndGet());

        if (destNodeId == null || !cctx.localNodeId().equals(destNodeId)) {
            msg.prepareMarshal(cctx);

            if (msg instanceof GridCacheDeployable && msg.addDeploymentInfo())
                cctx.deploy().prepare((GridCacheDeployable)msg);
        }

        return true;
    }

    /**
     * @param nodeId Node ID.
     * @param sndErr Send error.
     * @return {@code True} if node left.
     * @param ping {@code True} if try ping node.
     * @throws IgniteClientDisconnectedCheckedException If ping failed.
     */
    public boolean checkNodeLeft(UUID nodeId, IgniteCheckedException sndErr, boolean ping)
        throws IgniteClientDisconnectedCheckedException {
        return cctx.gridIO().checkNodeLeft(nodeId, sndErr, ping);
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
        assert !node.isLocal() : node;

        if (!onSend(msg, node.id()))
            return;

        if (log.isDebugEnabled())
            log.debug("Sending cache message [msg=" + msg + ", node=" + U.toShortString(node) + ']');

        int cnt = 0;

        while (cnt <= retryCnt) {
            try {
                cnt++;

                cctx.gridIO().sendToGridTopic(node, TOPIC_CACHE, msg, plc);

                return;
            }
            catch (ClusterTopologyCheckedException e) {
                throw e;
            }
            catch (IgniteCheckedException e) {
                if (!cctx.discovery().alive(node.id()) || !cctx.discovery().pingNode(node.id()))
                    throw new ClusterTopologyCheckedException("Node left grid while sending message to: " + node.id(), e);

                if (cnt == retryCnt || cctx.kernalContext().isStopping())
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
        if (!onSend(msg, node.id()))
            return;

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
            catch (ClusterTopologyCheckedException e) {
                throw e;
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
     * @return ID that auto-grows based on local counter and counters received from other nodes.
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
    void sendNoRetry(ClusterNode node,
        GridCacheMessage msg,
        byte plc)
        throws IgniteCheckedException {
        assert node != null;
        assert msg != null;

        if (!onSend(msg, null))
            return;

        try {
            cctx.gridIO().sendToGridTopic(node, TOPIC_CACHE, msg, plc);

            if (log.isDebugEnabled())
                log.debug("Sent cache message [msg=" + msg + ", node=" + U.toShortString(node) + ']');
        }
        catch (ClusterTopologyCheckedException e) {
            throw e;
        }
        catch (IgniteCheckedException e) {
            if (!cctx.discovery().alive(node.id()))
                throw new ClusterTopologyCheckedException("Node left grid while sending message to: " + node.id(), e);
            else
                throw e;
        }
    }

    /**
     * @param hndId Message handler ID.
     * @param type Type of message.
     * @param c Handler.
     */
    public void addCacheHandler(
        int hndId,
        Class<? extends GridCacheMessage> type,
        IgniteBiInClosure<UUID, ? extends GridCacheMessage> c) {
        assert !type.isAssignableFrom(GridCacheGroupIdMessage.class) : type;

        addHandler(hndId, type, c, cacheHandlers);
    }

    /**
     * @param hndId Message handler ID.
     * @param type Type of message.
     * @param c Handler.
     */
    public void addCacheGroupHandler(
        int hndId,
        Class<? extends GridCacheGroupIdMessage> type,
        IgniteBiInClosure<UUID, ? extends GridCacheMessage> c) {
        assert !type.isAssignableFrom(GridCacheIdMessage.class) : type;

        addHandler(hndId, type, c, grpHandlers);
    }

    /**
     * @param hndId Message handler ID.
     * @param type Type of message.
     * @param c Handler.
     * @param msgHandlers Message handlers.
     */
    @SuppressWarnings({"unchecked"})
    private void addHandler(
        int hndId,
        Class<? extends GridCacheMessage> type,
        IgniteBiInClosure<UUID, ? extends GridCacheMessage> c,
        MessageHandlers msgHandlers) {
        int msgIdx = messageIndex(type);

        if (msgIdx != -1) {
            Map<Integer, IgniteBiInClosure[]> idxClsHandlers0 = msgHandlers.idxClsHandlers;

            IgniteBiInClosure[] cacheClsHandlers = idxClsHandlers0.get(hndId);

            if (cacheClsHandlers == null) {
                cacheClsHandlers = new IgniteBiInClosure[GridCacheMessage.MAX_CACHE_MSG_LOOKUP_INDEX];

                idxClsHandlers0.put(hndId, cacheClsHandlers);
            }

            if (cacheClsHandlers[msgIdx] != null)
                throw new IgniteException("Duplicate cache message ID found [hndId=" + hndId +
                    ", type=" + type + ']');

            cacheClsHandlers[msgIdx] = c;

            msgHandlers.idxClsHandlers = idxClsHandlers0;

            return;
        }
        else {
            ListenerKey key = new ListenerKey(hndId, type);

            if (msgHandlers.clsHandlers.putIfAbsent(key,
                (IgniteBiInClosure<UUID, GridCacheMessage>)c) != null)
                assert false : "Handler for class already registered [hndId=" + hndId + ", cls=" + type +
                    ", old=" + msgHandlers.clsHandlers.get(key) + ", new=" + c + ']';
        }

        IgniteLogger log0 = log;

        if (log0 != null && log0.isTraceEnabled())
            log0.trace(
                "Registered cache communication handler [hndId=" + hndId + ", type=" + type +
                    ", msgIdx=" + msgIdx + ", handler=" + c + ']');
    }

    /**
     * @param cacheId Cache ID to remove handlers for.
     */
    void removeCacheHandlers(int cacheId) {
        removeHandlers(cacheHandlers, cacheId);
    }

    /**
     * @param grpId Cache group ID to remove handlers for.
     */
    void removeCacheGroupHandlers(int grpId) {
        removeHandlers(grpHandlers, grpId);
    }

    /**
     * @param msgHandlers Handlers.
     * @param hndId ID to remove handlers for.
     */
    private void removeHandlers(MessageHandlers msgHandlers, int hndId) {
        assert hndId != 0;

        msgHandlers.idxClsHandlers.remove(hndId);

        for (Iterator<ListenerKey> iter = msgHandlers.clsHandlers.keySet().iterator(); iter.hasNext(); ) {
            ListenerKey key = iter.next();

            if (key.hndId == hndId)
                iter.remove();
        }
    }

    /**
     * @param cacheGrp {@code True} if cache group handler, {@code false} if cache handler.
     * @param hndId Handler ID.
     * @param type Message type.
     */
    public void removeHandler(boolean cacheGrp, int hndId, Class<? extends GridCacheMessage> type) {
        MessageHandlers msgHandlers = cacheGrp ? grpHandlers : cacheHandlers;

        msgHandlers.clsHandlers.remove(new ListenerKey(hndId, type));
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
     * @param topic Topic.
     * @param c Handler.
     */
    public void addOrderedCacheHandler(Object topic, IgniteBiInClosure<UUID, ? extends GridCacheIdMessage> c) {
        addOrderedHandler(false, topic, c);
    }

    /**
     * @param topic Topic.
     * @param c Handler.
     */
    public void addOrderedCacheGroupHandler(Object topic, IgniteBiInClosure<UUID, ? extends GridCacheGroupIdMessage> c) {
        addOrderedHandler(true, topic, c);
    }

    /**
     * Adds ordered message handler.
     *
     * @param cacheGrp {@code True} if cache group message, {@code false} if cache message.
     * @param topic Topic.
     * @param c Handler.
     */
    @SuppressWarnings({"unchecked"})
    private void addOrderedHandler(boolean cacheGrp, Object topic, IgniteBiInClosure<UUID, ? extends GridCacheMessage> c) {
        MessageHandlers msgHandlers = cacheGrp ? grpHandlers : cacheHandlers;

        IgniteLogger log0 = log;

        if (msgHandlers.orderedHandlers.putIfAbsent(topic, c) == null) {
            cctx.gridIO().addMessageListener(topic, new OrderedMessageListener(
                (IgniteBiInClosure<UUID, GridCacheMessage>)c));

            if (log0 != null && log0.isTraceEnabled())
                log0.trace("Registered ordered cache communication handler [topic=" + topic + ", handler=" + c + ']');
        }
        else if (log0 != null)
            U.warn(log0, "Failed to register ordered cache communication handler because it is already " +
                "registered for this topic [topic=" + topic + ", handler=" + c + ']');
    }

    /**
     * Removed ordered message handler.
     *
     * @param cacheGrp {@code True} if cache group message, {@code false} if cache message.
     * @param topic Topic.
     */
    public void removeOrderedHandler(boolean cacheGrp, Object topic) {
        MessageHandlers msgHandlers = cacheGrp ? grpHandlers : cacheHandlers;

        if (msgHandlers.orderedHandlers.remove(topic) != null) {
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
     */
    @SuppressWarnings({"ErrorNotRethrown", "unchecked"})
    private void unmarshall(UUID nodeId, GridCacheMessage cacheMsg) {
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
        catch (BinaryObjectException e) {
            cacheMsg.onClassError(new IgniteCheckedException(e));
        }
        catch (Error e) {
            if (cacheMsg.ignoreClassErrors() && X.hasCause(e, NoClassDefFoundError.class,
                UnsupportedClassVersionError.class))
                cacheMsg.onClassError(new IgniteCheckedException("Failed to load class during unmarshalling: " + e, e));
            else
                throw e;
        }
    }

    /**
     * @param msg Message.
     * @return Cache or group descriptor.
     */
    private Object descriptorForMessage(GridCacheMessage msg) {
        if (msg instanceof GridCacheIdMessage)
            return cctx.cache().cacheDescriptor(((GridCacheIdMessage)msg).cacheId());
        else if (msg instanceof GridCacheGroupIdMessage)
            return cctx.cache().cacheGroupDescriptors().get(((GridCacheGroupIdMessage)msg).groupId());

        return null;
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>> ");
        X.println(">>> Cache IO manager memory stats [igniteInstanceName=" + cctx.igniteInstanceName() + ']');
        X.println(">>>   cacheClsHandlersSize: " + cacheHandlers.clsHandlers.size());
        X.println(">>>   cacheOrderedHandlersSize: " + cacheHandlers.orderedHandlers.size());
        X.println(">>>   cacheGrpClsHandlersSize: " + grpHandlers.clsHandlers.size());
        X.println(">>>   cacheGrpOrderedHandlersSize: " + grpHandlers.orderedHandlers.size());
    }

    /**
     *
     */
    static class MessageHandlers {
        /** Indexed class handlers. */
        volatile Map<Integer, IgniteBiInClosure[]> idxClsHandlers = new HashMap<>();

        /** Handler registry. */
        ConcurrentMap<ListenerKey, IgniteBiInClosure<UUID, GridCacheMessage>>
            clsHandlers = new ConcurrentHashMap8<>();

        /** Ordered handler registry. */
        ConcurrentMap<Object, IgniteBiInClosure<UUID, ? extends GridCacheMessage>> orderedHandlers =
            new ConcurrentHashMap8<>();
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

            Lock lock = rw.readLock();

            lock.lock();

            try {
                GridCacheMessage cacheMsg = (GridCacheMessage)msg;

                onMessage0(nodeId, cacheMsg, c);
            }
            finally {
                lock.unlock();
            }
        }
    }

    /**
     *
     */
    private static class ListenerKey {
        /** Cache ID. */
        private int hndId;

        /** Message class. */
        private Class<? extends GridCacheMessage> msgCls;

        /**
         * @param hndId Handler ID.
         * @param msgCls Message class.
         */
        private ListenerKey(int hndId, Class<? extends GridCacheMessage> msgCls) {
            this.hndId = hndId;
            this.msgCls = msgCls;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (!(o instanceof ListenerKey))
                return false;

            ListenerKey that = (ListenerKey)o;

            return hndId == that.hndId && msgCls.equals(that.msgCls);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = hndId;

            res = 31 * res + msgCls.hashCode();

            return res;
        }
    }
}
