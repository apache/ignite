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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.*;
import org.apache.ignite.cluster.*;
import org.apache.ignite.events.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.direct.*;
import org.apache.ignite.internal.managers.*;
import org.apache.ignite.internal.managers.deployment.*;
import org.apache.ignite.internal.managers.eventstorage.*;
import org.apache.ignite.internal.processors.timeout.*;
import org.apache.ignite.internal.util.*;
import org.apache.ignite.internal.util.lang.*;
import org.apache.ignite.internal.util.tostring.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;
import org.apache.ignite.lang.*;
import org.apache.ignite.marshaller.*;
import org.apache.ignite.plugin.extensions.communication.*;
import org.apache.ignite.spi.*;
import org.apache.ignite.spi.communication.*;
import org.apache.ignite.spi.communication.tcp.*;
import org.apache.ignite.thread.*;
import org.jetbrains.annotations.*;
import org.jsr166.*;

import java.io.*;
import java.util.*;
import java.util.Map.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;

import static org.apache.ignite.events.EventType.*;
import static org.apache.ignite.internal.GridTopic.*;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.*;
import static org.apache.ignite.internal.util.nio.GridNioBackPressureControl.*;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.*;

/**
 * Grid communication manager.
 */
public class GridIoManager extends GridManagerAdapter<CommunicationSpi<Serializable>> {
    /** */
    public static volatile boolean TURBO_DEBUG_MODE;

    /** Empty array of message factories. */
    public static final MessageFactory[] EMPTY = {};

    /** Max closed topics to store. */
    public static final int MAX_CLOSED_TOPICS = 10240;

    /** Listeners by topic. */
    private final ConcurrentMap<Object, GridMessageListener> lsnrMap = new ConcurrentHashMap8<>();

    /** Disconnect listeners. */
    private final Collection<GridDisconnectListener> disconnectLsnrs = new ConcurrentLinkedQueue<>();

    /** Map of {@link IoPool}-s injected by Ignite plugins. */
    private final IoPool[] ioPools = new IoPool[128];

    /** Public pool. */
    private ExecutorService pubPool;

    /** Internal P2P pool. */
    private ExecutorService p2pPool;

    /** Internal system pool. */
    private ExecutorService sysPool;

    /** Internal management pool. */
    private ExecutorService mgmtPool;

    /** Affinity assignment executor service. */
    private ExecutorService affPool;

    /** Utility cache pool. */
    private ExecutorService utilityCachePool;

    /** Marshaller cache pool. */
    private ExecutorService marshCachePool;

    /** Discovery listener. */
    private GridLocalEventListener discoLsnr;

    /** */
    private final ConcurrentMap<Object, ConcurrentMap<UUID, GridCommunicationMessageSet>> msgSetMap =
        new ConcurrentHashMap8<>();

    /** Local node ID. */
    private final UUID locNodeId;

    /** Discovery delay. */
    private final long discoDelay;

    /** Cache for messages that were received prior to discovery. */
    private final ConcurrentMap<UUID, ConcurrentLinkedDeque8<DelayedMessage>> waitMap =
        new ConcurrentHashMap8<>();

    /** Communication message listener. */
    private CommunicationListener<Serializable> commLsnr;

    /** Grid marshaller. */
    private final Marshaller marsh;

    /** Busy lock. */
    private final GridSpinReadWriteLock busyLock = new GridSpinReadWriteLock();

    /** Lock to sync maps access. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Fully started flag. When set to true, can send and receive messages. */
    private volatile boolean started;

    /** Closed topics. */
    private final GridBoundedConcurrentLinkedHashSet<Object> closedTopics =
        new GridBoundedConcurrentLinkedHashSet<>(MAX_CLOSED_TOPICS, MAX_CLOSED_TOPICS, 0.75f, 256,
            PER_SEGMENT_Q_OPTIMIZED_RMV);

    /** */
    private MessageFactory msgFactory;

    /** */
    private MessageFormatter formatter;

    /** Stopping flag. */
    private boolean stopping;

    /**
     * @param ctx Grid kernal context.
     */
    @SuppressWarnings("deprecation")
    public GridIoManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getCommunicationSpi());

        locNodeId = ctx.localNodeId();

        discoDelay = ctx.config().getDiscoveryStartupDelay();

        marsh = ctx.config().getMarshaller();
    }

    /**
     * @return Message factory.
     */
    public MessageFactory messageFactory() {
        assert msgFactory != null;

        return msgFactory;
    }

    /**
     * @return Message writer factory.
     */
    public MessageFormatter formatter() {
        assert formatter != null;

        return formatter;
    }

    /**
     * Resets metrics for this manager.
     */
    public void resetMetrics() {
        getSpi().resetMetrics();
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void start() throws IgniteCheckedException {
        assertParameter(discoDelay > 0, "discoveryStartupDelay > 0");

        startSpi();

        pubPool = ctx.getExecutorService();
        p2pPool = ctx.getPeerClassLoadingExecutorService();
        sysPool = ctx.getSystemExecutorService();
        mgmtPool = ctx.getManagementExecutorService();
        utilityCachePool = ctx.utilityCachePool();
        marshCachePool = ctx.marshallerCachePool();
        affPool = new IgniteThreadPoolExecutor(
            "aff-" + ctx.gridName(),
            1,
            1,
            0,
            new LinkedBlockingQueue<Runnable>());

        getSpi().setListener(commLsnr = new CommunicationListener<Serializable>() {
            @Override public void onMessage(UUID nodeId, Serializable msg, IgniteRunnable msgC) {
                try {
                    onMessage0(nodeId, (GridIoMessage)msg, msgC);
                }
                catch (ClassCastException ignored) {
                    U.error(log, "Communication manager received message of unknown type (will ignore): " +
                        msg.getClass().getName() + ". Most likely GridCommunicationSpi is being used directly, " +
                        "which is illegal - make sure to send messages only via GridProjection API.");
                }
            }

            @Override public void onDisconnected(UUID nodeId) {
                for (GridDisconnectListener lsnr : disconnectLsnrs)
                    lsnr.onNodeDisconnected(nodeId);
            }
        });

        MessageFormatter[] formatterExt = ctx.plugins().extensions(MessageFormatter.class);

        if (formatterExt != null && formatterExt.length > 0) {
            if (formatterExt.length > 1)
                throw new IgniteCheckedException("More than one MessageFormatter extension is defined. Check your " +
                    "plugins configuration and make sure that only one of them provides custom message format.");

            formatter = formatterExt[0];
        }
        else {
            formatter = new MessageFormatter() {
                @Override public MessageWriter writer() {
                    return new DirectMessageWriter();
                }

                @Override public MessageReader reader(MessageFactory factory, Class<? extends Message> msgCls) {
                    return new DirectMessageReader(msgFactory, this);
                }
            };
        }

        MessageFactory[] msgs = ctx.plugins().extensions(MessageFactory.class);

        if (msgs == null)
            msgs = EMPTY;

        List<MessageFactory> compMsgs = new ArrayList<>();

        for (IgniteComponentType compType : IgniteComponentType.values()) {
            MessageFactory f = compType.messageFactory();

            if (f != null)
                compMsgs.add(f);
        }

        if (!compMsgs.isEmpty())
            msgs = F.concat(msgs, compMsgs.toArray(new MessageFactory[compMsgs.size()]));

        msgFactory = new GridIoMessageFactory(msgs);

        if (log.isDebugEnabled())
            log.debug(startInfo());

        registerIoPoolExtensions();
    }

    /**
     * Processes IO messaging pool extensions.
     * @throws IgniteCheckedException On error.
     */
    private void registerIoPoolExtensions() throws IgniteCheckedException {
        // Process custom IO messaging pool extensions:
        final IoPool[] executorExtensions = ctx.plugins().extensions(IoPool.class);

        if (executorExtensions != null) {
            // Store it into the map and check for duplicates:
            for (IoPool ex : executorExtensions) {
                final byte id = ex.id();

                // 1. Check the pool id is non-negative:
                if (id < 0)
                    throw new IgniteCheckedException("Failed to register IO executor pool because its Id is negative " +
                        "[id=" + id + ']');

                // 2. Check the pool id is in allowed range:
                if (isReservedGridIoPolicy(id))
                    throw new IgniteCheckedException("Failed to register IO executor pool because its Id in in the " +
                        "reserved range (0-31) [id=" + id + ']');

                // 3. Check the pool for duplicates:
                if (ioPools[id] != null)
                    throw new IgniteCheckedException("Failed to register IO executor pool because its " +
                        "Id as already used [id=" + id + ']');

                ioPools[id] = ex;
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"deprecation", "SynchronizationOnLocalVariableOrMethodParameter"})
    @Override public void onKernalStart0() throws IgniteCheckedException {
        discoLsnr = new GridLocalEventListener() {
            @SuppressWarnings({"TooBroadScope", "fallthrough"})
            @Override public void onEvent(Event evt) {
                assert evt instanceof DiscoveryEvent : "Invalid event: " + evt;

                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                UUID nodeId = discoEvt.eventNode().id();

                switch (evt.type()) {
                    case EVT_NODE_JOINED:
                        assert waitMap.get(nodeId) == null; // We can't receive messages from undiscovered nodes.

                        break;

                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:
                        for (Map.Entry<Object, ConcurrentMap<UUID, GridCommunicationMessageSet>> e :
                            msgSetMap.entrySet()) {
                            ConcurrentMap<UUID, GridCommunicationMessageSet> map = e.getValue();

                            GridCommunicationMessageSet set;

                            boolean empty;

                            synchronized (map) {
                                set = map.remove(nodeId);

                                empty = map.isEmpty();
                            }

                            if (set != null) {
                                if (log.isDebugEnabled())
                                    log.debug("Removed message set due to node leaving grid: " + set);

                                // Unregister timeout listener.
                                ctx.timeout().removeTimeoutObject(set);

                                // Node may still send stale messages for this topic
                                // even after discovery notification is done.
                                closedTopics.add(set.topic());
                            }

                            if (empty)
                                msgSetMap.remove(e.getKey(), map);
                        }

                        // Clean up delayed and ordered messages (need exclusive lock).
                        lock.writeLock().lock();

                        try {
                            ConcurrentLinkedDeque8<DelayedMessage> waitList = waitMap.remove(nodeId);

                            if (log.isDebugEnabled())
                                log.debug("Removed messages from discovery startup delay list " +
                                    "(sender node left topology): " + waitList);
                        }
                        finally {
                            lock.writeLock().unlock();
                        }

                        break;

                    default:
                        assert false : "Unexpected event: " + evt;
                }
            }
        };

        ctx.event().addLocalEventListener(discoLsnr, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED);

        // Make sure that there are no stale messages due to window between communication
        // manager start and kernal start.
        // 1. Process wait list.
        Collection<Collection<DelayedMessage>> delayedMsgs = new ArrayList<>();

        lock.writeLock().lock();

        try {
            started = true;

            for (Entry<UUID, ConcurrentLinkedDeque8<DelayedMessage>> e : waitMap.entrySet()) {
                if (ctx.discovery().node(e.getKey()) != null) {
                    ConcurrentLinkedDeque8<DelayedMessage> waitList = waitMap.remove(e.getKey());

                    if (log.isDebugEnabled())
                        log.debug("Processing messages from discovery startup delay list: " + waitList);

                    if (waitList != null)
                        delayedMsgs.add(waitList);
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }

        // After write lock released.
        if (!delayedMsgs.isEmpty()) {
            for (Collection<DelayedMessage> col : delayedMsgs)
                for (DelayedMessage msg : col)
                    commLsnr.onMessage(msg.nodeId(), msg.message(), msg.callback());
        }

        // 2. Process messages sets.
        for (Map.Entry<Object, ConcurrentMap<UUID, GridCommunicationMessageSet>> e : msgSetMap.entrySet()) {
            ConcurrentMap<UUID, GridCommunicationMessageSet> map = e.getValue();

            for (GridCommunicationMessageSet set : map.values()) {
                if (ctx.discovery().node(set.nodeId()) == null) {
                    // All map modifications should be synced for consistency.
                    boolean rmv;

                    synchronized (map) {
                        rmv = map.remove(set.nodeId(), set);
                    }

                    if (rmv) {
                        if (log.isDebugEnabled())
                            log.debug("Removed message set due to node leaving grid: " + set);

                        // Unregister timeout listener.
                        ctx.timeout().removeTimeoutObject(set);
                    }

                }
            }

            boolean rmv;

            synchronized (map) {
                rmv = map.isEmpty();
            }

            if (rmv) {
                msgSetMap.remove(e.getKey(), map);

                // Node may still send stale messages for this topic
                // even after discovery notification is done.
                closedTopics.add(e.getKey());
            }
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public void onKernalStop0(boolean cancel) {
        // No more communication messages.
        getSpi().setListener(null);

        boolean interrupted = false;

        // Busy wait is intentional.
        while (true) {
            try {
                if (busyLock.tryWriteLock(200, TimeUnit.MILLISECONDS))
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

        try {
            if (interrupted)
                Thread.currentThread().interrupt();

            U.shutdownNow(getClass(), affPool, log);

            GridEventStorageManager evtMgr = ctx.event();

            if (evtMgr != null && discoLsnr != null)
                evtMgr.removeLocalEventListener(discoLsnr);

            stopping = true;
        }
        finally {
            busyLock.writeUnlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        if (log.isDebugEnabled())
            log.debug(stopInfo());

        Arrays.fill(ioPools, null);
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message bytes.
     * @param msgC Closure to call when message processing finished.
     */
    @SuppressWarnings("fallthrough")
    private void onMessage0(UUID nodeId, GridIoMessage msg, IgniteRunnable msgC) {
        assert nodeId != null;
        assert msg != null;

        busyLock.readLock();

        try {
            if (stopping) {
                if (log.isDebugEnabled())
                    log.debug("Received communication message while stopping (will ignore) [nodeId=" +
                        nodeId + ", msg=" + msg + ']');

                return;
            }

            // Check discovery.
            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null) {
                if (log.isDebugEnabled())
                    log.debug("Ignoring message from dead node [senderId=" + nodeId + ", msg=" + msg + ']');

                return; // We can't receive messages from non-discovered ones.
            }

            if (msg.topic() == null) {
                int topicOrd = msg.topicOrdinal();

                msg.topic(topicOrd >= 0 ? GridTopic.fromOrdinal(topicOrd) : marsh.unmarshal(msg.topicBytes(), null));
            }

            if (!started) {
                lock.readLock().lock();

                try {
                    if (!started) { // Sets to true in write lock, so double checking.
                        // Received message before valid context is set to manager.
                        if (log.isDebugEnabled())
                            log.debug("Adding message to waiting list [senderId=" + nodeId +
                                ", msg=" + msg + ']');

                        ConcurrentLinkedDeque8<DelayedMessage> list =
                            F.addIfAbsent(waitMap, nodeId, F.<DelayedMessage>newDeque());

                        assert list != null;

                        list.add(new DelayedMessage(nodeId, msg, msgC));

                        return;
                    }
                }
                finally {
                    lock.readLock().unlock();
                }
            }

            // If message is P2P, then process in P2P service.
            // This is done to avoid extra waiting and potential deadlocks
            // as thread pool may not have any available threads to give.
            byte plc = msg.policy();

            switch (plc) {
                case P2P_POOL: {
                    processP2PMessage(nodeId, msg, msgC);

                    break;
                }

                case PUBLIC_POOL:
                case SYSTEM_POOL:
                case MANAGEMENT_POOL:
                case AFFINITY_POOL:
                case UTILITY_CACHE_POOL:
                case MARSH_CACHE_POOL:
                {
                    if (msg.isOrdered())
                        processOrderedMessage(nodeId, msg, plc, msgC);
                    else
                        processRegularMessage(nodeId, msg, plc, msgC);

                    break;
                }

                default:
                    assert plc >= 0 : "Negative policy: " + plc;

                    if (isReservedGridIoPolicy(plc))
                        throw new IgniteCheckedException("Failed to process message with policy of reserved range. " +
                            "[policy=" + plc + ']');

                    if (msg.isOrdered())
                        processOrderedMessage(nodeId, msg, plc, msgC);
                    else
                        processRegularMessage(nodeId, msg, plc, msgC);
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to process message (will ignore): " + msg, e);
        }
        finally {
            busyLock.readUnlock();
        }
    }

    /**
     * Gets execution pool for policy.
     *
     * @param plc Policy.
     * @return Execution pool.
     */
    private Executor pool(byte plc) throws IgniteCheckedException {
        switch (plc) {
            case P2P_POOL:
                return p2pPool;
            case SYSTEM_POOL:
                return sysPool;
            case PUBLIC_POOL:
                return pubPool;
            case MANAGEMENT_POOL:
                return mgmtPool;
            case AFFINITY_POOL:
                return affPool;

            case UTILITY_CACHE_POOL:
                assert utilityCachePool != null : "Utility cache pool is not configured.";

                return utilityCachePool;

            case MARSH_CACHE_POOL:
                assert marshCachePool != null : "Marshaller cache pool is not configured.";

                return marshCachePool;

            default: {
                assert plc >= 0 : "Negative policy: " + plc;

                if (isReservedGridIoPolicy(plc))
                    throw new IgniteCheckedException("Failed to process message with policy of reserved" +
                        " range (0-31), [policy=" + plc + ']');

                IoPool pool = ioPools[plc];

                if (pool == null)
                    throw new IgniteCheckedException("Failed to process message because no pool is registered " +
                        "for policy. [policy=" + plc + ']');

                assert plc == pool.id();

                Executor ex = pool.executor();

                if (ex == null)
                    throw new IgniteCheckedException("Failed to process message because corresponding executor " +
                        "is null. [id=" + plc + ']');

                return ex;
            }
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     * @param msgC Closure to call when message processing finished.
     */
    private void processP2PMessage(
        final UUID nodeId,
        final GridIoMessage msg,
        final IgniteRunnable msgC
    ) {
        Runnable c = new Runnable() {
            @Override public void run() {
                try {
                    threadProcessingMessage(true);

                    GridMessageListener lsnr = lsnrMap.get(msg.topic());

                    if (lsnr == null)
                        return;

                    Object obj = msg.message();

                    assert obj != null;

                    lsnr.onMessage(nodeId, obj);
                }
                finally {
                    threadProcessingMessage(false);

                    msgC.run();
                }
            }
        };

        try {
            p2pPool.execute(c);
        }
        catch (RejectedExecutionException e) {
            U.error(log, "Failed to process P2P message due to execution rejection. Increase the upper bound " +
                "on 'ExecutorService' provided by 'IgniteConfiguration.getPeerClassLoadingThreadPoolSize()'. " +
                "Will attempt to process message in the listener thread instead.", e);

            c.run();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     * @param plc Execution policy.
     * @param msgC Closure to call when message processing finished.
     */
    private void processRegularMessage(
        final UUID nodeId,
        final GridIoMessage msg,
        byte plc,
        final IgniteRunnable msgC
    ) throws IgniteCheckedException {
        Runnable c = new Runnable() {
            @Override public void run() {
                try {
                    threadProcessingMessage(true);

                    processRegularMessage0(msg, nodeId);
                }
                finally {
                    threadProcessingMessage(false);

                    msgC.run();
                }
            }
        };

        try {
            pool(plc).execute(c);
        }
        catch (RejectedExecutionException e) {
            U.error(log, "Failed to process regular message due to execution rejection. Increase the upper bound " +
                "on 'ExecutorService' provided by 'IgniteConfiguration.getPublicThreadPoolSize()'. " +
                "Will attempt to process message in the listener thread instead.", e);

            c.run();
        }
    }

    /**
     * @param msg Message.
     * @param nodeId Node ID.
     */
    @SuppressWarnings("deprecation")
    private void processRegularMessage0(GridIoMessage msg, UUID nodeId) {
        GridMessageListener lsnr = lsnrMap.get(msg.topic());

        if (lsnr == null)
            return;

        Object obj = msg.message();

        assert obj != null;

        lsnr.onMessage(nodeId, obj);
    }

    /**
     * @param nodeId Node ID.
     * @param msg Ordered message.
     * @param plc Execution policy.
     * @param msgC Closure to call when message processing finished ({@code null} for sync processing).
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void processOrderedMessage(
        final UUID nodeId,
        final GridIoMessage msg,
        final byte plc,
        @Nullable final IgniteRunnable msgC
    ) throws IgniteCheckedException {
        assert msg != null;

        long timeout = msg.timeout();
        boolean skipOnTimeout = msg.skipOnTimeout();

        boolean isNew = false;

        ConcurrentMap<UUID, GridCommunicationMessageSet> map;

        GridCommunicationMessageSet set = null;

        while (true) {
            map = msgSetMap.get(msg.topic());

            if (map == null) {
                set = new GridCommunicationMessageSet(plc, msg.topic(), nodeId, timeout, skipOnTimeout, msg, msgC);

                map = new ConcurrentHashMap0<>();

                map.put(nodeId, set);

                ConcurrentMap<UUID, GridCommunicationMessageSet> old = msgSetMap.putIfAbsent(
                    msg.topic(), map);

                if (old != null)
                    map = old;
                else {
                    isNew = true;

                    // Put succeeded.
                    break;
                }
            }

            boolean rmv = false;

            synchronized (map) {
                if (map.isEmpty())
                    rmv = true;
                else {
                    set = map.get(nodeId);

                    if (set == null) {
                        GridCommunicationMessageSet old = map.putIfAbsent(nodeId,
                            set = new GridCommunicationMessageSet(plc, msg.topic(),
                                nodeId, timeout, skipOnTimeout, msg, msgC));

                        assert old == null;

                        isNew = true;

                        // Put succeeded.
                        break;
                    }
                }
            }

            if (rmv)
                msgSetMap.remove(msg.topic(), map);
            else {
                assert set != null;
                assert !isNew;

                set.add(msg, msgC);

                break;
            }
        }

        if (isNew && ctx.discovery().node(nodeId) == null) {
            if (log.isDebugEnabled())
                log.debug("Message is ignored as sender has left the grid: " + msg);

            assert map != null;

            boolean rmv;

            synchronized (map) {
                map.remove(nodeId);

                rmv = map.isEmpty();
            }

            if (rmv)
                msgSetMap.remove(msg.topic(), map);

            return;
        }

        if (isNew && set.endTime() != Long.MAX_VALUE)
            ctx.timeout().addTimeoutObject(set);

        final GridMessageListener lsnr = lsnrMap.get(msg.topic());

        if (lsnr == null) {
            if (closedTopics.contains(msg.topic())) {
                if (log.isDebugEnabled())
                    log.debug("Message is ignored as it came for the closed topic: " + msg);

                assert map != null;

                msgSetMap.remove(msg.topic(), map);
            }
            else if (log.isDebugEnabled()) {
                // Note that we simply keep messages if listener is not
                // registered yet, until one will be registered.
                log.debug("Received message for unknown listener (messages will be kept until a " +
                    "listener is registered): " + msg);
            }

            // Mark the message as processed, otherwise reading from the connection
            // may stop.
            if (msgC != null)
                msgC.run();

            return;
        }

        if (msgC == null) {
            // Message from local node can be processed in sync manner.
            assert locNodeId.equals(nodeId) || TURBO_DEBUG_MODE;

            unwindMessageSet(set, lsnr);

            return;
        }

        final GridCommunicationMessageSet msgSet0 = set;

        Runnable c = new Runnable() {
            @Override public void run() {
                try {
                    threadProcessingMessage(true);

                    unwindMessageSet(msgSet0, lsnr);
                }
                finally {
                    threadProcessingMessage(false);
                }
            }
        };

        try {
            pool(plc).execute(c);
        }
        catch (RejectedExecutionException e) {
            U.error(log, "Failed to process ordered message due to execution rejection. " +
                "Increase the upper bound on executor service provided by corresponding " +
                "configuration property. Will attempt to process message in the listener " +
                "thread instead [msgPlc=" + plc + ']', e);

            c.run();
        }
    }

    /**
     * @param msgSet Message set to unwind.
     * @param lsnr Listener to notify.
     */
    private void unwindMessageSet(GridCommunicationMessageSet msgSet, GridMessageListener lsnr) {
        // Loop until message set is empty or
        // another thread owns the reservation.
        while (true) {
            if (msgSet.reserve()) {
                try {
                    msgSet.unwind(lsnr);
                }
                finally {
                    msgSet.release();
                }

                // Check outside of reservation block.
                if (!msgSet.changed()) {
                    if (log.isDebugEnabled())
                        log.debug("Message set has not been changed: " + msgSet);

                    break;
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Another thread owns reservation: " + msgSet);

                return;
            }
        }
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param topicOrd GridTopic enumeration ordinal.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param ordered Ordered flag.
     * @param timeout Timeout.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @param ackClosure Ack closure.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    private void send(
        ClusterNode node,
        Object topic,
        int topicOrd,
        Message msg,
        byte plc,
        boolean ordered,
        long timeout,
        boolean skipOnTimeout,
        IgniteInClosure<IgniteException> ackClosure
    ) throws IgniteCheckedException {
        assert node != null;
        assert topic != null;
        assert msg != null;

        GridIoMessage ioMsg = new GridIoMessage(plc, topic, topicOrd, msg, ordered, timeout, skipOnTimeout);

        if (locNodeId.equals(node.id())) {
            assert plc != P2P_POOL;

            CommunicationListener commLsnr = this.commLsnr;

            if (commLsnr == null)
                throw new IgniteCheckedException("Trying to send message when grid is not fully started.");

            if (ordered)
                processOrderedMessage(locNodeId, ioMsg, plc, null);
            else
                processRegularMessage0(ioMsg, locNodeId);

            if (ackClosure != null)
                ackClosure.apply(null);
        }
        else {
            if (topicOrd < 0)
                ioMsg.topicBytes(marsh.marshal(topic));

            try {
                if ((CommunicationSpi)getSpi() instanceof TcpCommunicationSpi)
                    ((TcpCommunicationSpi)(CommunicationSpi)getSpi()).sendMessage(node, ioMsg, ackClosure);
                else
                    getSpi().sendMessage(node, ioMsg);
            }
            catch (IgniteSpiException e) {
                throw new IgniteCheckedException("Failed to send message (node may have left the grid or " +
                    "TCP connection cannot be established due to firewall issues) " +
                    "[node=" + node + ", topic=" + topic +
                    ", msg=" + msg + ", policy=" + plc + ']', e);
            }
        }
    }

    /**
     * This method can be used for debugging tricky concurrency issues
     * with multi-nodes in single JVM.
     * <p>
     * This method eliminates network between nodes started in single JVM
     * when {@link #TURBO_DEBUG_MODE} is set to {@code true}.
     * <p>
     * How to use it:
     * <ol>
     *     <li>Replace {@link #send(ClusterNode, Object, int, Message, byte, boolean, long, boolean, IgniteInClosure)}
     *          with this method.</li>
     *     <li>Start all grids for your test, then set {@link #TURBO_DEBUG_MODE} to {@code true}.</li>
     *     <li>Perform test operations on the topology. No network will be there.</li>
     *     <li>DO NOT turn on turbo debug before all grids started. This will cause deadlocks.</li>
     * </ol>
     *
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param topicOrd GridTopic enumeration ordinal.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param ordered Ordered flag.
     * @param timeout Timeout.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    private void sendTurboDebug(
        ClusterNode node,
        Object topic,
        int topicOrd,
        Message msg,
        byte plc,
        boolean ordered,
        long timeout,
        boolean skipOnTimeout
    ) throws IgniteCheckedException {
        assert node != null;
        assert topic != null;
        assert msg != null;

        GridIoMessage ioMsg = new GridIoMessage(plc, topic, topicOrd, msg, ordered, timeout, skipOnTimeout);

        IgniteKernal rmt;

        if (locNodeId.equals(node.id())) {
            assert plc != P2P_POOL;

            CommunicationListener commLsnr = this.commLsnr;

            if (commLsnr == null)
                throw new IgniteCheckedException("Trying to send message when grid is not fully started.");

            if (ordered)
                processOrderedMessage(locNodeId, ioMsg, plc, null);
            else
                processRegularMessage0(ioMsg, locNodeId);
        }
        else if (TURBO_DEBUG_MODE && (rmt = IgnitionEx.gridxx(locNodeId)) != null) {
            if (ioMsg.isOrdered())
                rmt.context().io().processOrderedMessage(locNodeId, ioMsg, ioMsg.policy(), null);
            else
                rmt.context().io().processRegularMessage0(ioMsg, locNodeId);
        }
        else {
            if (topicOrd < 0)
                ioMsg.topicBytes(marsh.marshal(topic));

            try {
                getSpi().sendMessage(node, ioMsg);
            }
            catch (IgniteSpiException e) {
                throw new IgniteCheckedException("Failed to send message (node may have left the grid or " +
                    "TCP connection cannot be established due to firewall issues) " +
                    "[node=" + node + ", topic=" + topic +
                    ", msg=" + msg + ", policy=" + plc + ']', e);
            }
        }
    }

    /**
     * @param nodeId Id of destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void send(UUID nodeId, Object topic, Message msg, byte plc)
        throws IgniteCheckedException {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            throw new IgniteCheckedException("Failed to send message to node (has node left grid?): " + nodeId);

        send(node, topic, msg, plc);
    }

    /**
     * @param nodeId Id of destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public void send(UUID nodeId, GridTopic topic, Message msg, byte plc)
        throws IgniteCheckedException {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            throw new IgniteCheckedException("Failed to send message to node (has node left grid?): " + nodeId);

        send(node, topic, topic.ordinal(), msg, plc, false, 0, false, null);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void send(ClusterNode node, Object topic, Message msg, byte plc)
        throws IgniteCheckedException {
        send(node, topic, -1, msg, plc, false, 0, false, null);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void send(ClusterNode node, GridTopic topic, Message msg, byte plc)
        throws IgniteCheckedException {
        send(node, topic, topic.ordinal(), msg, plc, false, 0, false, null);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param timeout Timeout to keep a message on receiving queue.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendOrderedMessage(
        ClusterNode node,
        Object topic,
        Message msg,
        byte plc,
        long timeout,
        boolean skipOnTimeout
    ) throws IgniteCheckedException {
        assert timeout > 0 || skipOnTimeout;

        send(node, topic, (byte)-1, msg, plc, true, timeout, skipOnTimeout, null);
    }

    /**
     * @param nodeId Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param timeout Timeout to keep a message on receiving queue.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendOrderedMessage(
        UUID nodeId,
        Object topic,
        Message msg,
        byte plc,
        long timeout,
        boolean skipOnTimeout
    ) throws IgniteCheckedException {
        assert timeout > 0 || skipOnTimeout;

        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            throw new IgniteCheckedException("Failed to send message to node (has node left grid?): " + nodeId);

        send(node, topic, (byte)-1, msg, plc, true, timeout, skipOnTimeout, null);
    }

    /**
     * @param node Destination nodes.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param ackClosure Ack closure.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void send(ClusterNode node, GridTopic topic, Message msg, byte plc,
        IgniteInClosure<IgniteException> ackClosure) throws IgniteCheckedException {
        send(node, topic, topic.ordinal(), msg, plc, false, 0, false, ackClosure);
    }

    /**
     * @param nodes Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param timeout Timeout to keep a message on receiving queue.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendOrderedMessage(
        Collection<? extends ClusterNode> nodes,
        Object topic,
        Message msg,
        byte plc,
        long timeout,
        boolean skipOnTimeout
    )
        throws IgniteCheckedException {
        assert timeout > 0 || skipOnTimeout;

        send(nodes, topic, -1, msg, plc, true, timeout, skipOnTimeout);
    }

    /**
     * @param node Destination nodes.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param ackClosure Ack closure.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void send(ClusterNode node, Object topic, Message msg, byte plc, IgniteInClosure<IgniteException> ackClosure)
        throws IgniteCheckedException {
        send(node, topic, -1, msg, plc, false, 0, false, ackClosure);
    }

    /**
     * @param nodes Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void send(
        Collection<? extends ClusterNode> nodes,
        Object topic,
        Message msg,
        byte plc
    ) throws IgniteCheckedException {
        send(nodes, topic, -1, msg, plc, false, 0, false);
    }

    /**
     * @param nodes Destination nodes.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void send(
        Collection<? extends ClusterNode> nodes,
        GridTopic topic,
        Message msg,
        byte plc
    ) throws IgniteCheckedException {
        send(nodes, topic, topic.ordinal(), msg, plc, false, 0, false);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param timeout Timeout to keep a message on receiving queue.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @param ackClosure Ack closure.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendOrderedMessage(
        ClusterNode node,
        Object topic,
        Message msg,
        byte plc,
        long timeout,
        boolean skipOnTimeout,
        IgniteInClosure<IgniteException> ackClosure
    ) throws IgniteCheckedException {
        assert timeout > 0 || skipOnTimeout;

        send(node, topic, (byte)-1, msg, plc, true, timeout, skipOnTimeout, ackClosure);
    }

     /**
     * Sends a peer deployable user message.
     *
     * @param nodes Destination nodes.
     * @param msg Message to send.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendUserMessage(Collection<? extends ClusterNode> nodes, Object msg) throws IgniteCheckedException {
        sendUserMessage(nodes, msg, null, false, 0);
    }

    /**
     * Sends a peer deployable user message.
     *
     * @param nodes Destination nodes.
     * @param msg Message to send.
     * @param topic Message topic to use.
     * @param ordered Is message ordered?
     * @param timeout Message timeout in milliseconds for ordered messages.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @SuppressWarnings("ConstantConditions")
    public void sendUserMessage(Collection<? extends ClusterNode> nodes, Object msg,
        @Nullable Object topic, boolean ordered, long timeout) throws IgniteCheckedException {
        boolean loc = nodes.size() == 1 && F.first(nodes).id().equals(locNodeId);

        byte[] serMsg = null;
        byte[] serTopic = null;

        if (!loc) {
            serMsg = marsh.marshal(msg);

            if (topic != null)
                serTopic = marsh.marshal(topic);
        }

        GridDeployment dep = null;

        String depClsName = null;

        if (ctx.config().isPeerClassLoadingEnabled()) {
            Class<?> cls0 = U.detectClass(msg);

            if (U.isJdk(cls0) && topic != null)
                cls0 = U.detectClass(topic);

            dep = ctx.deploy().deploy(cls0, U.detectClassLoader(cls0));

            if (dep == null)
                throw new IgniteDeploymentCheckedException("Failed to deploy user message: " + msg);

            depClsName = cls0.getName();
        }

        Message ioMsg = new GridIoUserMessage(
            msg,
            serMsg,
            depClsName,
            topic,
            serTopic,
            dep != null ? dep.classLoaderId() : null,
            dep != null ? dep.deployMode() : null,
            dep != null ? dep.userVersion() : null,
            dep != null ? dep.participants() : null);

        if (ordered)
            sendOrderedMessage(nodes, TOPIC_COMM_USER, ioMsg, PUBLIC_POOL, timeout, true);
        else if (loc)
            send(F.first(nodes), TOPIC_COMM_USER, ioMsg, PUBLIC_POOL);
        else {
            ClusterNode locNode = F.find(nodes, null, F.localNode(locNodeId));

            Collection<? extends ClusterNode> rmtNodes = F.view(nodes, F.remoteNodes(locNodeId));

            if (!rmtNodes.isEmpty())
                send(rmtNodes, TOPIC_COMM_USER, ioMsg, PUBLIC_POOL);

            // Will call local listeners in current thread synchronously, so must go the last
            // to allow remote nodes execute the requested operation in parallel.
            if (locNode != null)
                send(locNode, TOPIC_COMM_USER, ioMsg, PUBLIC_POOL);
        }
    }

    /**
     * @param topic Topic to subscribe to.
     * @param p Message predicate.
     */
    public void addUserMessageListener(@Nullable final Object topic, @Nullable final IgniteBiPredicate<UUID, ?> p) {
        if (p != null) {
            try {
                if (p instanceof GridLifecycleAwareMessageFilter)
                    ((GridLifecycleAwareMessageFilter)p).initialize(ctx);
                else
                    ctx.resource().injectGeneric(p);

                addMessageListener(TOPIC_COMM_USER,
                    new GridUserMessageListener(topic, (IgniteBiPredicate<UUID, Object>)p));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * @param topic Topic to unsubscribe from.
     * @param p Message predicate.
     */
    public void removeUserMessageListener(@Nullable Object topic, IgniteBiPredicate<UUID, ?> p) {
        try {
            removeMessageListener(TOPIC_COMM_USER,
                new GridUserMessageListener(topic, (IgniteBiPredicate<UUID, Object>)p));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param nodeId Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param timeout Timeout to keep a message on receiving queue.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @param ackClosure Ack closure.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendOrderedMessage(
        UUID nodeId,
        Object topic,
        Message msg,
        byte plc,
        long timeout,
        boolean skipOnTimeout,
        IgniteInClosure<IgniteException> ackClosure
    ) throws IgniteCheckedException {
        assert timeout > 0 || skipOnTimeout;

        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            throw new IgniteCheckedException("Failed to send message to node (has node left grid?): " + nodeId);

        send(node, topic, (byte)-1, msg, plc, true, timeout, skipOnTimeout, ackClosure);
    }

    /**
     * @param nodes Destination nodes.
     * @param topic Topic to send the message to.
     * @param topicOrd Topic ordinal value.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param ordered Ordered flag.
     * @param timeout Message timeout.
     * @param skipOnTimeout Whether message can be skipped in timeout.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    private void send(
        Collection<? extends ClusterNode> nodes,
        Object topic,
        int topicOrd,
        Message msg,
        byte plc,
        boolean ordered,
        long timeout,
        boolean skipOnTimeout
    ) throws IgniteCheckedException {
        assert nodes != null;
        assert topic != null;
        assert msg != null;

        if (!ordered)
            assert F.find(nodes, null, F.localNode(locNodeId)) == null :
                "Internal Ignite code should never call the method with local node in a node list.";

        try {
            // Small optimization, as communication SPIs may have lighter implementation for sending
            // messages to one node vs. many.
            if (!nodes.isEmpty()) {
                for (ClusterNode node : nodes)
                    send(node, topic, topicOrd, msg, plc, ordered, timeout, skipOnTimeout, null);
            }
            else if (log.isDebugEnabled())
                log.debug("Failed to send message to empty nodes collection [topic=" + topic + ", msg=" +
                    msg + ", policy=" + plc + ']');
        }
        catch (IgniteSpiException e) {
            throw new IgniteCheckedException("Failed to send message (nodes may have left the grid or " +
                "TCP connection cannot be established due to firewall issues) " +
                "[nodes=" + nodes + ", topic=" + topic +
                ", msg=" + msg + ", policy=" + plc + ']', e);
        }
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to add.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "deprecation"})
    public void addMessageListener(GridTopic topic, GridMessageListener lsnr) {
        addMessageListener((Object)topic, lsnr);
    }

    /**
     * @param lsnr Listener to add.
     */
    public void addDisconnectListener(GridDisconnectListener lsnr) {
        disconnectLsnrs.add(lsnr);
    }

    /**
     * @param lsnr Listener to remove.
     */
    public void removeDisconnectListener(GridDisconnectListener lsnr) {
        disconnectLsnrs.remove(lsnr);
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to add.
     */
    @SuppressWarnings({"deprecation", "SynchronizationOnLocalVariableOrMethodParameter"})
    public void addMessageListener(Object topic, final GridMessageListener lsnr) {
        assert lsnr != null;
        assert topic != null;

        // Make sure that new topic is not in the list of closed topics.
        closedTopics.remove(topic);

        GridMessageListener lsnrs;

        for (;;) {
            lsnrs = lsnrMap.putIfAbsent(topic, lsnr);

            if (lsnrs == null) {
                lsnrs = lsnr;

                break;
            }

            assert lsnrs != null;

            if (!(lsnrs instanceof ArrayListener)) { // We are putting the second listener, creating array.
                GridMessageListener arrLsnr = new ArrayListener(lsnrs, lsnr);

                if (lsnrMap.replace(topic, lsnrs, arrLsnr)) {
                    lsnrs = arrLsnr;

                    break;
                }
            }
            else {
                if (((ArrayListener)lsnrs).add(lsnr))
                    break;

                // Add operation failed because array is already empty and is about to be removed, helping and retrying.
                lsnrMap.remove(topic, lsnrs);
            }
        }

        Map<UUID, GridCommunicationMessageSet> map = msgSetMap.get(topic);

        Collection<GridCommunicationMessageSet> msgSets = map != null ? map.values() : null;

        if (msgSets != null) {
            final GridMessageListener lsnrs0 = lsnrs;

            try {
                for (final GridCommunicationMessageSet msgSet : msgSets) {
                    pool(msgSet.policy()).execute(
                        new Runnable() {
                            @Override public void run() {
                                unwindMessageSet(msgSet, lsnrs0);
                            }
                        });
                }
            }
            catch (RejectedExecutionException e) {
                U.error(log, "Failed to process delayed message due to execution rejection. Increase the upper bound " +
                    "on executor service provided in 'IgniteConfiguration.getPublicThreadPoolSize()'). Will attempt to " +
                    "process message in the listener thread instead.", e);

                for (GridCommunicationMessageSet msgSet : msgSets)
                    unwindMessageSet(msgSet, lsnr);
            }
            catch (IgniteCheckedException ice) {
                throw new IgniteException(ice);
            }
        }
    }

    /**
     * @param topic Message topic.
     * @return Whether or not listener was indeed removed.
     */
    public boolean removeMessageListener(GridTopic topic) {
        return removeMessageListener((Object)topic);
    }

    /**
     * @param topic Message topic.
     * @return Whether or not listener was indeed removed.
     */
    public boolean removeMessageListener(Object topic) {
        return removeMessageListener(topic, null);
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to remove.
     * @return Whether or not the lsnr was removed.
     */
    @SuppressWarnings("deprecation")
    public boolean removeMessageListener(GridTopic topic, @Nullable GridMessageListener lsnr) {
        return removeMessageListener((Object)topic, lsnr);
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to remove.
     * @return Whether or not the lsnr was removed.
     */
    @SuppressWarnings({"deprecation", "SynchronizationOnLocalVariableOrMethodParameter"})
    public boolean removeMessageListener(Object topic, @Nullable GridMessageListener lsnr) {
        assert topic != null;

        boolean rmv = true;

        Collection<GridCommunicationMessageSet> msgSets = null;

        // If listener is null, then remove all listeners.
        if (lsnr == null) {
            closedTopics.add(topic);

            lsnr = lsnrMap.remove(topic);

            rmv = lsnr != null;

            Map<UUID, GridCommunicationMessageSet> map = msgSetMap.remove(topic);

            if (map != null)
                msgSets = map.values();
        }
        else {
            for (;;) {
                GridMessageListener lsnrs = lsnrMap.get(topic);

                // If removing listener before subscription happened.
                if (lsnrs == null) {
                    closedTopics.add(topic);

                    Map<UUID, GridCommunicationMessageSet> map = msgSetMap.remove(topic);

                    if (map != null)
                        msgSets = map.values();

                    rmv = false;

                    break;
                }
                else {
                    boolean empty = false;

                    if (!(lsnrs instanceof ArrayListener)) {
                        if (lsnrs.equals(lsnr)) {
                            if (!lsnrMap.remove(topic, lsnrs))
                                continue; // Retry because it can be packed to array listener.

                            empty = true;
                        }
                        else
                            rmv = false;
                    }
                    else {
                        ArrayListener arrLsnr = (ArrayListener)lsnrs;

                        if (arrLsnr.remove(lsnr))
                            empty = arrLsnr.isEmpty();
                        else
                            // Listener was not found.
                            rmv = false;

                        if (empty)
                            lsnrMap.remove(topic, lsnrs);
                    }

                    // If removing last subscribed listener.
                    if (empty) {
                        closedTopics.add(topic);

                        Map<UUID, GridCommunicationMessageSet> map = msgSetMap.remove(topic);

                        if (map != null)
                            msgSets = map.values();
                    }

                    break;
                }
            }
        }

        if (msgSets != null)
            for (GridCommunicationMessageSet msgSet : msgSets)
                ctx.timeout().removeTimeoutObject(msgSet);

        if (rmv && log.isDebugEnabled())
            log.debug("Removed message listener [topic=" + topic + ", lsnr=" + lsnr + ']');

        if (lsnr instanceof ArrayListener)
        {
            for (GridMessageListener childLsnr : ((ArrayListener)lsnr).arr)
                closeListener(childLsnr);
        }
        else
            closeListener(lsnr);

        return rmv;
    }

    /**
     * Closes a listener, if applicable.
     * @param lsnr Listener.
     */
    private void closeListener(GridMessageListener lsnr) {
        if (lsnr instanceof GridUserMessageListener) {
            GridUserMessageListener userLsnr = (GridUserMessageListener)lsnr;

            if (userLsnr.predLsnr instanceof GridLifecycleAwareMessageFilter)
                ((GridLifecycleAwareMessageFilter)userLsnr.predLsnr).close();
        }
    }

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    public int getSentMessagesCount() {
        return getSpi().getSentMessagesCount();
    }

    /**
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    public long getSentBytesCount() {
        return getSpi().getSentBytesCount();
    }

    /**
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    public int getReceivedMessagesCount() {
        return getSpi().getReceivedMessagesCount();
    }

    /**
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    public long getReceivedBytesCount() {
        return getSpi().getReceivedBytesCount();
    }

    /**
     * Gets outbound messages queue size.
     *
     * @return Outbound messages queue size.
     */
    public int getOutboundMessagesQueueSize() {
        return getSpi().getOutboundMessagesQueueSize();
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> IO manager memory stats [grid=" + ctx.gridName() + ']');
        X.println(">>>  lsnrMapSize: " + lsnrMap.size());
        X.println(">>>  msgSetMapSize: " + msgSetMap.size());
        X.println(">>>  closedTopicsSize: " + closedTopics.sizex());
        X.println(">>>  discoWaitMapSize: " + waitMap.size());
    }

    /**
     * Linked chain of listeners.
     */
    private static class ArrayListener implements GridMessageListener {
        /** */
        private volatile GridMessageListener[] arr;

        /**
         * @param arr Array of listeners.
         */
        ArrayListener(GridMessageListener... arr) {
            this.arr = arr;
        }

        /**
         * Passes message to the whole chain.
         *
         * @param nodeId Node ID.
         * @param msg Message.
         */
        @Override public void onMessage(UUID nodeId, Object msg) {
            GridMessageListener[] arr0 = arr;

            if (arr0 == null)
                return;

            for (GridMessageListener l : arr0)
                l.onMessage(nodeId, msg);
        }

        /**
         * @return {@code true} If this instance is empty.
         */
        boolean isEmpty() {
            return arr == null;
        }

        /**
         * @param l Listener.
         * @return {@code true} If listener was removed.
         */
        synchronized boolean remove(GridMessageListener l) {
            GridMessageListener[] arr0 = arr;

            if (arr0 == null)
                return false;

            if (arr0.length == 1) {
                if (!arr0[0].equals(l))
                    return false;

                arr = null;

                return true;
            }

            for (int i = 0; i < arr0.length; i++) {
                if (arr0[i].equals(l)) {
                    int newLen = arr0.length - 1;

                    if (i == newLen) // Remove last.
                        arr = Arrays.copyOf(arr0, newLen);
                    else {
                        GridMessageListener[] arr1 = new GridMessageListener[newLen];

                        if (i != 0) // Not remove first.
                            System.arraycopy(arr0, 0, arr1, 0, i);

                        System.arraycopy(arr0, i + 1, arr1, i, newLen - i);

                        arr = arr1;
                    }

                    return true;
                }
            }

            return false;
        }

        /**
         * @param l Listener.
         * @return {@code true} if listener was added. Add can fail if this instance is empty and is about to be removed
         *         from map.
         */
        synchronized boolean add(GridMessageListener l) {
            GridMessageListener[] arr0 = arr;

            if (arr0 == null)
                return false;

            int oldLen = arr0.length;

            arr0 = Arrays.copyOf(arr0, oldLen + 1);

            arr0[oldLen] = l;

            arr = arr0;

            return true;
        }
    }

    /**
     * This class represents a message listener wrapper that knows about peer deployment.
     */
    private class GridUserMessageListener implements GridMessageListener {
        /** Predicate listeners. */
        private final IgniteBiPredicate<UUID, Object> predLsnr;

        /** User message topic. */
        private final Object topic;

        /**
         * @param topic User topic.
         * @param predLsnr Predicate listener.
         * @throws IgniteCheckedException If failed to inject resources to predicates.
         */
        GridUserMessageListener(@Nullable Object topic, @Nullable IgniteBiPredicate<UUID, Object> predLsnr)
            throws IgniteCheckedException {
            this.topic = topic;
            this.predLsnr = predLsnr;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter", "ConstantConditions",
            "OverlyStrongTypeCast"})
        @Override public void onMessage(UUID nodeId, Object msg) {
            if (!(msg instanceof GridIoUserMessage)) {
                U.error(log, "Received unknown message (potentially fatal problem): " + msg);

                return;
            }

            GridIoUserMessage ioMsg = (GridIoUserMessage)msg;

            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null) {
                U.warn(log, "Failed to resolve sender node (did the node left grid?): " + nodeId);

                return;
            }

            busyLock.readLock();

            try {
                if (stopping) {
                    if (log.isDebugEnabled())
                        log.debug("Received user message while stopping (will ignore) [nodeId=" +
                            nodeId + ", msg=" + msg + ']');

                    return;
                }

                Object msgBody = ioMsg.body();

                assert msgBody != null || ioMsg.bodyBytes() != null;

                try {
                    byte[] msgTopicBytes = ioMsg.topicBytes();

                    Object msgTopic = ioMsg.topic();

                    GridDeployment dep = ioMsg.deployment();

                    if (dep == null && ctx.config().isPeerClassLoadingEnabled() &&
                        ioMsg.deploymentClassName() != null) {
                        dep = ctx.deploy().getGlobalDeployment(
                            ioMsg.deploymentMode(),
                            ioMsg.deploymentClassName(),
                            ioMsg.deploymentClassName(),
                            ioMsg.userVersion(),
                            nodeId,
                            ioMsg.classLoaderId(),
                            ioMsg.loaderParticipants(),
                            null);

                        if (dep == null)
                            throw new IgniteDeploymentCheckedException(
                                "Failed to obtain deployment information for user message. " +
                                    "If you are using custom message or topic class, try implementing " +
                                    "GridPeerDeployAware interface. [msg=" + ioMsg + ']');

                        ioMsg.deployment(dep); // Cache deployment.
                    }

                    // Unmarshall message topic if needed.
                    if (msgTopic == null && msgTopicBytes != null) {
                        msgTopic = marsh.unmarshal(msgTopicBytes, dep != null ? dep.classLoader() : null);

                        ioMsg.topic(msgTopic); // Save topic to avoid future unmarshallings.
                    }

                    if (!F.eq(topic, msgTopic))
                        return;

                    if (msgBody == null) {
                        msgBody = marsh.unmarshal(ioMsg.bodyBytes(), dep != null ? dep.classLoader() : null);

                        ioMsg.body(msgBody); // Save body to avoid future unmarshallings.
                    }

                    // Resource injection.
                    if (dep != null)
                        ctx.resource().inject(dep, dep.deployedClass(ioMsg.deploymentClassName()), msgBody);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to unmarshal user message [node=" + nodeId + ", message=" +
                        msg + ']', e);
                }

                if (msgBody != null) {
                    if (predLsnr != null) {
                        if (!predLsnr.apply(nodeId, msgBody))
                            removeMessageListener(TOPIC_COMM_USER, this);
                    }
                }
            }
            finally {
                busyLock.readUnlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            GridUserMessageListener l = (GridUserMessageListener)o;

            return F.eq(predLsnr, l.predLsnr) && F.eq(topic, l.topic);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = predLsnr != null ? predLsnr.hashCode() : 0;

            res = 31 * res + (topic != null ? topic.hashCode() : 0);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GridUserMessageListener.class, this);
        }
    }

    /**
     * Ordered communication message set.
     */
    private class GridCommunicationMessageSet implements GridTimeoutObject {
        /** */
        private final UUID nodeId;

        /** */
        private long endTime;

        /** */
        private final IgniteUuid timeoutId;

        /** */
        @GridToStringInclude
        private final Object topic;

        /** */
        private final byte plc;

        /** */
        @GridToStringInclude
        private final Queue<GridTuple3<GridIoMessage, Long, IgniteRunnable>> msgs = new ConcurrentLinkedDeque<>();

        /** */
        private final AtomicBoolean reserved = new AtomicBoolean();

        /** */
        private final long timeout;

        /** */
        private final boolean skipOnTimeout;

        /** */
        private long lastTs;

        /**
         * @param plc Communication policy.
         * @param topic Communication topic.
         * @param nodeId Node ID.
         * @param timeout Timeout.
         * @param skipOnTimeout Whether message can be skipped on timeout.
         * @param msg Message to add immediately.
         * @param msgC Message closure (may be {@code null}).
         */
        GridCommunicationMessageSet(
            byte plc,
            Object topic,
            UUID nodeId,
            long timeout,
            boolean skipOnTimeout,
            GridIoMessage msg,
            @Nullable IgniteRunnable msgC
        ) {
            assert nodeId != null;
            assert topic != null;
            assert msg != null;

            this.plc = plc;
            this.nodeId = nodeId;
            this.topic = topic;
            this.timeout = timeout == 0 ? ctx.config().getNetworkTimeout() : timeout;
            this.skipOnTimeout = skipOnTimeout;

            endTime = endTime(timeout);

            timeoutId = IgniteUuid.randomUuid();

            lastTs = U.currentTimeMillis();

            msgs.add(F.t(msg, lastTs, msgC));
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return timeoutId;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
        @Override public void onTimeout() {
            GridMessageListener lsnr = lsnrMap.get(topic);

            if (lsnr != null) {
                long delta = 0;

                if (skipOnTimeout) {
                    while (true) {
                        delta = 0;

                        boolean unwind = false;

                        synchronized (this) {
                            if (!msgs.isEmpty()) {
                                delta = U.currentTimeMillis() - lastTs;

                                if (delta >= timeout)
                                    unwind = true;
                            }
                        }

                        if (unwind)
                            unwindMessageSet(this, lsnr);
                        else
                            break;
                    }
                }

                // Someone is still listening to messages, so delay set removal.
                endTime = endTime(timeout - delta);

                ctx.timeout().addTimeoutObject(this);

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Removing message set due to timeout: " + this);

            ConcurrentMap<UUID, GridCommunicationMessageSet> map = msgSetMap.get(topic);

            if (map != null) {
                boolean rmv;

                synchronized (map) {
                    rmv = map.remove(nodeId, this) && map.isEmpty();
                }

                if (rmv)
                    msgSetMap.remove(topic, map);
            }
        }

        /**
         * @return ID of node that sent the messages in the set.
         */
        UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Communication policy.
         */
        byte policy() {
            return plc;
        }

        /**
         * @return Message topic.
         */
        Object topic() {
            return topic;
        }

        /**
         * @return {@code True} if successful.
         */
        boolean reserve() {
            return reserved.compareAndSet(false, true);
        }

        /**
         * @return {@code True} if set is reserved.
         */
        boolean reserved() {
            return reserved.get();
        }

        /**
         * Releases reservation.
         */
        void release() {
            assert reserved.get() : "Message set was not reserved: " + this;

            reserved.set(false);
        }

        /**
         * @param lsnr Listener to notify.
         */
        void unwind(GridMessageListener lsnr) {
            assert reserved.get();

            for (GridTuple3<GridIoMessage, Long, IgniteRunnable> t = msgs.poll(); t != null; t = msgs.poll()) {
                try {
                    lsnr.onMessage(
                        nodeId,
                        t.get1().message());
                }
                finally {
                    if (t.get3() != null)
                        t.get3().run();
                }
            }
        }

        /**
         * @param msg Message to add.
         * @param msgC Message closure (may be {@code null}).
         */
        void add(
            GridIoMessage msg,
            @Nullable IgniteRunnable msgC
        ) {
            msgs.add(F.t(msg, U.currentTimeMillis(), msgC));
        }

        /**
         * @return {@code True} if set has messages to unwind.
         */
        boolean changed() {
            return !msgs.isEmpty();
        }

        /**
         * Calculates end time with overflow check.
         *
         * @param timeout Timeout in milliseconds.
         * @return End time in milliseconds.
         */
        private long endTime(long timeout) {
            long endTime = U.currentTimeMillis() + timeout;

            // Account for overflow.
            if (endTime < 0)
                endTime = Long.MAX_VALUE;

            return endTime;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GridCommunicationMessageSet.class, this);
        }
    }

    /**
     *
     */
    private static class ConcurrentHashMap0<K, V> extends ConcurrentHashMap8<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int hash;

        /**
         * @param o Object to be compared for equality with this map.
         * @return {@code True} only for {@code this}.
         */
        @Override public boolean equals(Object o) {
            return o == this;
        }

        /**
         * @return Identity hash code.
         */
        @Override public int hashCode() {
            if (hash == 0) {
                int hash0 = System.identityHashCode(this);

                hash = hash0 != 0 ? hash0 : -1;
            }

            return hash;
        }
    }

    /**
     *
     */
    private static class DelayedMessage {
        /** */
        private final UUID nodeId;

        /** */
        private final GridIoMessage msg;

        /** */
        private final IgniteRunnable msgC;

        /**
         * @param nodeId Node ID.
         * @param msg Message.
         * @param msgC Callback.
         */
        private DelayedMessage(UUID nodeId, GridIoMessage msg, IgniteRunnable msgC) {
            this.nodeId = nodeId;
            this.msg = msg;
            this.msgC = msgC;
        }

        /**
         * @return Message char.
         */
        public IgniteRunnable callback() {
            return msgC;
        }

        /**
         * @return Message.
         */
        public GridIoMessage message() {
            return msg;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DelayedMessage.class, this, super.toString());
        }
    }
}
