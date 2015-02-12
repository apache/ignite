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

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.cluster.*;
import org.apache.ignite.internal.managers.communication.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.jdk8.backport.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.internal.GridTopic.*;

/**
 * Cache communication manager.
 */
public class GridCacheIoManager<K, V> extends GridCacheSharedManagerAdapter<K, V> {
    /** Message ID generator. */
    private static final AtomicLong idGen = new AtomicLong();

    /** Delay in milliseconds between retries. */
    private long retryDelay;

    /** Number of retries using to send messages. */
    private int retryCnt;

    /** Indexed class handlers. */
    private Map<Integer, IgniteBiInClosure[]> idxClsHandlers = new HashMap<>();

    /** Handler registry. */
    private ConcurrentMap<ListenerKey, IgniteBiInClosure<UUID, GridCacheMessage<K, V>>>
        clsHandlers = new ConcurrentHashMap8<>();

    /** Ordered handler registry. */
    private ConcurrentMap<Object, IgniteBiInClosure<UUID, ? extends GridCacheMessage<K, V>>> orderedHandlers =
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
        @SuppressWarnings("unchecked")
        @Override public void onMessage(final UUID nodeId, Object msg) {
            if (log.isDebugEnabled())
                log.debug("Received unordered cache communication message [nodeId=" + nodeId +
                    ", locId=" + cctx.localNodeId() + ", msg=" + msg + ']');

            final GridCacheMessage<K, V> cacheMsg = (GridCacheMessage<K, V>)msg;

            int msgIdx = cacheMsg.lookupIndex();

            IgniteBiInClosure<UUID, GridCacheMessage<K, V>> c = null;

            if (msgIdx >= 0) {
                IgniteBiInClosure[] cacheClsHandlers = idxClsHandlers.get(cacheMsg.cacheId());

                if (cacheClsHandlers != null)
                    c = cacheClsHandlers[msgIdx];
            }

            if (c == null)
                c = clsHandlers.get(new ListenerKey(cacheMsg.cacheId(), cacheMsg.getClass()));

            if (c == null) {
                if (log.isDebugEnabled())
                    log.debug("Received message without registered handler (will ignore) [msg=" + msg +
                        ", nodeId=" + nodeId + ']');

                return;
            }

            long locTopVer = cctx.discovery().topologyVersion();
            long rmtTopVer = cacheMsg.topologyVersion();

            if (locTopVer < rmtTopVer) {
                if (log.isDebugEnabled())
                    log.debug("Received message has higher topology version [msg=" + msg +
                        ", locTopVer=" + locTopVer + ", rmtTopVer=" + rmtTopVer + ']');

                IgniteInternalFuture<Long> topFut = cctx.discovery().topologyFuture(rmtTopVer);

                if (!topFut.isDone()) {
                    final IgniteBiInClosure<UUID, GridCacheMessage<K, V>> c0 = c;

                    topFut.listenAsync(new CI1<IgniteInternalFuture<Long>>() {
                        @Override public void apply(IgniteInternalFuture<Long> t) {
                            onMessage0(nodeId, cacheMsg, c0);
                        }
                    });

                    return;
                }
            }

            onMessage0(nodeId, cacheMsg, c);
        }
    };

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
    private void onMessage0(final UUID nodeId, final GridCacheMessage<K, V> cacheMsg,
        final IgniteBiInClosure<UUID, GridCacheMessage<K, V>> c) {
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
                    startFut.listenAsync(new CI1<IgniteInternalFuture<?>>() {
                        @Override public void apply(IgniteInternalFuture<?> f) {
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
                                    U.error(log, "Failed to complete preload start future (will ignore message) " +
                                        "[fut=" + f + ", nodeId=" + nodeId + ", msg=" + cacheMsg + ']', e);
                            }
                            finally {
                                rw.readUnlock();
                            }
                        }
                    });
                }
            }
        }
        catch (Throwable e) {
            if (X.hasCause(e, ClassNotFoundException.class))
                U.error(log, "Failed to process message (note that distributed services " +
                    "do not support peer class loading, if you deploy distributed service " +
                    "you should have all required classes in CLASSPATH on all nodes in topology) " +
                    "[senderId=" + nodeId + ", err=" + X.cause(e, ClassNotFoundException.class).getMessage() + ']');
            else
                U.error(log, "Failed to process message [senderId=" + nodeId + ']', e);
        }
        finally {
            if (depEnabled)
                cctx.deploy().ignoreOwnership(false);

            rw.readUnlock();
        }
    }

    /**
     * @param cacheMsg Cache message to get start future.
     * @return Preloader start future.
     */
    private IgniteInternalFuture<Object> startFuture(GridCacheMessage<K, V> cacheMsg) {
        int cacheId = cacheMsg.cacheId();

        return cacheId != 0 ? cctx.cacheContext(cacheId).preloader().startFuture() : cctx.preloadersStartFuture();
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     * @param c Closure.
     */
    private void processMessage(UUID nodeId, GridCacheMessage<K, V> msg,
        IgniteBiInClosure<UUID, GridCacheMessage<K, V>> c) {
        try {
            // Start clean.
            if (msg.transactional())
                CU.resetTxContext(cctx);

            // We will not end up with storing a bunch of new UUIDs
            // in each cache entry, since node ID is stored in NIO session
            // on handshake.
            c.apply(nodeId, msg);

            if (log.isDebugEnabled())
                log.debug("Finished processing cache communication message [nodeId=" + nodeId + ", msg=" + msg + ']');
        }
        catch (Throwable e) {
            U.error(log, "Failed processing message [senderId=" + nodeId + ']', e);
        }
        finally {
            // Clear thread-local tx contexts.
            CU.resetTxContext(cctx);

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
    private void onSend(GridCacheMessage<K, V> msg, @Nullable UUID destNodeId) throws IgniteCheckedException {
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
     * @throws IgniteCheckedException If sending failed.
     * @throws ClusterTopologyCheckedException If receiver left.
     */
    @SuppressWarnings("unchecked")
    public void send(ClusterNode node, GridCacheMessage<K, V> msg, GridIoPolicy plc) throws IgniteCheckedException {
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
    public boolean safeSend(Collection<? extends ClusterNode> nodes, GridCacheMessage<K, V> msg, GridIoPolicy plc,
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
     * @throws IgniteCheckedException If sending failed.
     */
    public void send(UUID nodeId, GridCacheMessage<K, V> msg, GridIoPolicy plc) throws IgniteCheckedException {
        ClusterNode n = cctx.discovery().node(nodeId);

        if (n == null)
            throw new ClusterTopologyException("Failed to send message because node left grid [nodeId=" + nodeId +
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
    public void sendOrderedMessage(ClusterNode node, Object topic, GridCacheMessage<K, V> msg, GridIoPolicy plc,
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
     * Adds message handler.
     *
     * @param type Type of message.
     * @param c Handler.
     */
    @SuppressWarnings({"unchecked"})
    public void addHandler(
        int cacheId,
        Class<? extends GridCacheMessage> type,
        IgniteBiInClosure<UUID, ? extends GridCacheMessage<K, V>> c) {
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
                (IgniteBiInClosure<UUID, GridCacheMessage<K, V>>)c) != null)
                assert false : "Handler for class already registered [cacheId=" + cacheId + ", cls=" + type +
                    ", old=" + clsHandlers.get(key) + ", new=" + c + ']';
        }

        if (log != null && log.isDebugEnabled())
            log.debug("Registered cache communication handler [cacheId=" + cacheId + ", type=" + type +
                ", msgIdx=" + msgIdx + ", handler=" + c + ']');
    }

    /**
     * @param lsnr Listener to add.
     */
    public void addDisconnectListener(GridDisconnectListener lsnr) {
        cctx.kernalContext().io().addDisconnectListener(lsnr);
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
    public void addOrderedHandler(Object topic, IgniteBiInClosure<UUID, ? extends GridCacheMessage<K, V>> c) {
        if (orderedHandlers.putIfAbsent(topic, c) == null) {
            cctx.gridIO().addMessageListener(topic, new OrderedMessageListener(
                (IgniteBiInClosure<UUID, GridCacheMessage<K, V>>)c));

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
    private void unmarshall(UUID nodeId, GridCacheMessage<K, V> cacheMsg) throws IgniteCheckedException {
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
            if (cacheMsg.ignoreClassErrors() && X.hasCause(e, InvalidClassException.class,
                    ClassNotFoundException.class, NoClassDefFoundError.class, UnsupportedClassVersionError.class))
                cacheMsg.onClassError(e);
            else
                throw e;
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
        private final IgniteBiInClosure<UUID, GridCacheMessage<K, V>> c;

        /**
         * @param c Handler closure.
         */
        OrderedMessageListener(IgniteBiInClosure<UUID, GridCacheMessage<K, V>> c) {
            this.c = c;
        }

        /** {@inheritDoc} */
        @SuppressWarnings( {"CatchGenericClass", "unchecked"})
        @Override public void onMessage(final UUID nodeId, Object msg) {
            if (log.isDebugEnabled())
                log.debug("Received cache ordered message [nodeId=" + nodeId + ", msg=" + msg + ']');

            final GridCacheMessage<K, V> cacheMsg = (GridCacheMessage<K, V>)msg;

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
            int result = cacheId;

            result = 31 * result + msgCls.hashCode();

            return result;
        }
    }
}
