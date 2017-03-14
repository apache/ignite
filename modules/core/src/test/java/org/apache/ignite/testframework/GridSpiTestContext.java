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

package org.apache.ignite.testframework;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.TaskEvent;
import org.apache.ignite.internal.ClusterMetricsSnapshot;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.communication.GridIoManager;
import org.apache.ignite.internal.managers.communication.GridIoMessageFactory;
import org.apache.ignite.internal.managers.communication.GridIoUserMessage;
import org.apache.ignite.internal.managers.communication.GridMessageListener;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.timeout.GridSpiTimeoutObject;
import org.apache.ignite.internal.processors.timeout.GridTimeoutProcessor;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.plugin.security.SecuritySubject;
import org.apache.ignite.spi.IgniteNodeValidationResult;
import org.apache.ignite.spi.IgnitePortProtocol;
import org.apache.ignite.spi.IgniteSpiContext;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.IgniteSpiTimeoutObject;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.events.EventType.EVT_NODE_METRICS_UPDATED;
import static org.apache.ignite.internal.GridTopic.TOPIC_COMM_USER;

/**
 * Test SPI context.
 */
public class GridSpiTestContext implements IgniteSpiContext {
    /** */
    private final Collection<ClusterNode> rmtNodes = new ConcurrentLinkedQueue<>();

    /** */
    private ClusterNode locNode;

    /** */
    private final Map<GridLocalEventListener, Set<Integer>> evtLsnrs = new HashMap<>();

    /** */
    @SuppressWarnings("deprecation")
    private final Collection<GridMessageListener> msgLsnrs = new ArrayList<>();

    /** */
    private final Map<ClusterNode, Serializable> sentMsgs = new HashMap<>();

    /** */
    private final ConcurrentMap<String, Map> cache = new ConcurrentHashMap<>();

    /** */
    private MessageFormatter formatter;

    /** */
    private MessageFactory factory;

    /** */
    private GridTimeoutProcessor timeoutProcessor;

    /**
     * @param timeoutProcessor Timeout processor.
     */
    public void timeoutProcessor(GridTimeoutProcessor timeoutProcessor) {
        this.timeoutProcessor = timeoutProcessor;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> remoteNodes() {
        return rmtNodes;
    }

    /** {@inheritDoc} */
    @Override public ClusterNode localNode() {
        return locNode;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> remoteDaemonNodes() {
        Collection<ClusterNode> daemons = new ArrayList<>();

        for (ClusterNode node : rmtNodes) {
            if (node.isDaemon())
                daemons.add(node);
        }

        return daemons;
    }

    /** {@inheritDoc} */
    @Override public Collection<ClusterNode> nodes() {
        Collection<ClusterNode> all = new ArrayList<>(rmtNodes);

        if (locNode != null)
            all.add(locNode);

        return all;
    }

    /**
     * @param locNode Local node.
     */
    public void setLocalNode(@Nullable ClusterNode locNode) {
        this.locNode = locNode;
    }

    /** {@inheritDoc} */
    @Nullable @Override public ClusterNode node(UUID nodeId) {
        if (locNode != null && locNode.id().equals(nodeId))
            return locNode;

        for (ClusterNode node : rmtNodes) {
            if (node.id().equals(nodeId))
                return node;
        }

        return null;
    }

    /** */
    public void createLocalNode() {
        setLocalNode(new GridTestNode(UUID.randomUUID(), createMetrics(1, 1)));
    }

    /**
     * @param cnt Number of nodes.
     */
    public void createRemoteNodes(int cnt) {
        for (int i = 0; i < cnt; i++)
            addNode(new GridTestNode(UUID.randomUUID(), createMetrics(1, 1)));
    }

    /** */
    public void reset() {
        setLocalNode(null);

        rmtNodes.clear();
    }

    /**
     * @param waitingJobs Waiting jobs count.
     * @param activeJobs Active jobs count.
     * @return Metrics adapter.
     */
    private ClusterMetricsSnapshot createMetrics(int waitingJobs, int activeJobs) {
        ClusterMetricsSnapshot metrics = new ClusterMetricsSnapshot();

        metrics.setCurrentWaitingJobs(waitingJobs);
        metrics.setCurrentActiveJobs(activeJobs);

        return metrics;
    }

    /**
     * @param nodes Nodes to reset.
     * @param rmv Whether nodes that were not passed in should be removed or not.
     */
    public void resetNodes(Collection<ClusterNode> nodes, boolean rmv) {
        for (ClusterNode node : nodes) {
            assert !node.equals(locNode);

            if (!rmtNodes.contains(node))
                addNode(node);
        }

        if (rmv) {
            for (Iterator<ClusterNode> iter = rmtNodes.iterator(); iter.hasNext();) {
                ClusterNode node = iter.next();

                if (!nodes.contains(node)) {
                    iter.remove();

                    notifyListener(new DiscoveryEvent(locNode, "Node left", EVT_NODE_LEFT, node));
                }
            }
        }
    }

    /**
     * @param node Node to check.
     * @return {@code True} if the node is local.
     */
    public boolean isLocalNode(ClusterNode node) {
        return locNode.equals(node);
    }

    /**
     * @param node Node to add.
     */
    public void addNode(ClusterNode node) {
        rmtNodes.add(node);

        notifyListener(new DiscoveryEvent(locNode, "Node joined", EVT_NODE_JOINED, node));
    }

    /**
     * @param node Node to remove.
     */
    public void removeNode(ClusterNode node) {
        if (rmtNodes.remove(node))
            notifyListener(new DiscoveryEvent(locNode, "Node left", EVT_NODE_LEFT, node));
    }

    /**
     * @param nodeId Node ID.
     */
    public void removeNode(UUID nodeId) {
        for (Iterator<ClusterNode> iter = rmtNodes.iterator(); iter.hasNext(); ) {
            ClusterNode node = iter.next();

            if (node.id().equals(nodeId)) {
                iter.remove();

                notifyListener(new DiscoveryEvent(locNode, "Node left", EVT_NODE_LEFT, node));
            }
        }
    }

    /**
     * @param node Node to fail.
     */
    public void failNode(ClusterNode node) {
        if (rmtNodes.remove(node))
            notifyListener(new DiscoveryEvent(locNode, "Node failed", EVT_NODE_FAILED, node));
    }

    /**
     * @param node Node for metrics update.
     */
    public void updateMetrics(ClusterNode node) {
        if (locNode.equals(node) || rmtNodes.contains(node))
            notifyListener(new DiscoveryEvent(locNode, "Metrics updated.", EVT_NODE_METRICS_UPDATED, node));
    }

    /** */
    public void updateAllMetrics() {
        notifyListener(new DiscoveryEvent(locNode, "Metrics updated", EVT_NODE_METRICS_UPDATED, locNode));

        for (ClusterNode node : rmtNodes)
            notifyListener(new DiscoveryEvent(locNode, "Metrics updated", EVT_NODE_METRICS_UPDATED, node));
    }

    /**
     * @param evt Event node.
     */
    private void notifyListener(Event evt) {
        assert evt.type() > 0;

        for (Map.Entry<GridLocalEventListener, Set<Integer>> entry : evtLsnrs.entrySet()) {
            if (F.isEmpty(entry.getValue()) || entry.getValue().contains(evt.type()))
                entry.getKey().onEvent(evt);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean pingNode(UUID nodeId) {
        return node(nodeId) != null;
    }

    /** {@inheritDoc} */
    @Override public void send(ClusterNode node, Serializable msg, String topic)
        throws IgniteSpiException {
        sentMsgs.put(node, msg);
    }

    /**
     * @param node Node message was sent to.
     * @return Sent message.
     */
    public Serializable getSentMessage(ClusterNode node) {
        return sentMsgs.get(node);
    }

    /**
     * @param node Node message was sent to.
     * @return Sent message.
     */
    public Serializable removeSentMessage(ClusterNode node) {
        return sentMsgs.remove(node);
    }

    /**
     * @param node Destination node.
     * @param msg Message.
     */
    @SuppressWarnings("deprecation")
    public void triggerMessage(ClusterNode node, Object msg) {
        for (GridMessageListener lsnr : msgLsnrs)
            lsnr.onMessage(node.id(), msg);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void addLocalMessageListener(Object topic, IgniteBiPredicate<UUID, ?> p) {
        try {
            addMessageListener(TOPIC_COMM_USER,
                new GridLocalMessageListener(topic, (IgniteBiPredicate<UUID, Object>)p));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to add.
     */
    @SuppressWarnings({"TypeMayBeWeakened", "deprecation"})
    public void addMessageListener(GridTopic topic, GridMessageListener lsnr) {
        addMessageListener(lsnr, ((Object)topic).toString());
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void addMessageListener(GridMessageListener lsnr, String topic) {
        msgLsnrs.add(lsnr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public boolean removeMessageListener(GridMessageListener lsnr, String topic) {
        return msgLsnrs.remove(lsnr);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override public void removeLocalMessageListener(Object topic, IgniteBiPredicate<UUID, ?> p) {
        try {
            removeMessageListener(TOPIC_COMM_USER,
                new GridLocalMessageListener(topic, (IgniteBiPredicate<UUID, Object>)p));
        }
        catch (IgniteCheckedException e) {
            throw new IgniteException(e);
        }
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to remove.
     * @return Whether or not the lsnr was removed.
     */
    @SuppressWarnings("deprecation")
    public boolean removeMessageListener(GridTopic topic, @Nullable GridMessageListener lsnr) {
        return removeMessageListener(lsnr, ((Object)topic).toString());
    }

    /**
     * @param type Event type.
     * @param taskName Task name.
     * @param taskSesId Session ID.
     * @param msg Event message.
     */
    public void triggerTaskEvent(int type, String taskName, IgniteUuid taskSesId, String msg) {
        assert type > 0;

        triggerEvent(new TaskEvent(locNode, msg, type, taskSesId, taskName, null, false, null));
    }

    /**
     * @param evt Event to trigger.
     */
    public void triggerEvent(Event evt) {
        notifyListener(evt);
    }

    /** {@inheritDoc} */
    @Override public void addLocalEventListener(GridLocalEventListener lsnr, int... types) {
        Set<Integer> typeSet = F.addIfAbsent(evtLsnrs, lsnr, F.<Integer>newSet());

        assert typeSet != null;

        if (types != null) {
            for (int type : types)
                typeSet.add(type);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeLocalEventListener(GridLocalEventListener lsnr) {
        boolean res = evtLsnrs.containsKey(lsnr);

        evtLsnrs.remove(lsnr);

        return res;
    }

    /** {@inheritDoc} */
    @Override public boolean isEventRecordable(int... types) {
        return true;
    }

    /** {@inheritDoc} */
    @Override public void recordEvent(Event evt) {
        notifyListener(evt);
    }

    /** {@inheritDoc} */
    @Override public void registerPort(int port, IgnitePortProtocol proto) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public void deregisterPort(int port, IgnitePortProtocol proto) {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public void deregisterPorts() {
        /* No-op. */
    }

    /** {@inheritDoc} */
    @Override public <K, V> V get(String cacheName, K key) {
        assert cacheName != null;
        assert key != null;

        V res = null;

        Map<K, CachedObject<V>> cache = getOrCreateCache(cacheName);

        CachedObject<V> obj = cache.get(key);

        if (obj != null) {
            if (obj.expire == 0 || obj.expire > System.currentTimeMillis())
                res = obj.obj;
            else
                cache.remove(key);
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public <K, V> V put(String cacheName, K key, V val, long ttl) {
        assert cacheName != null;
        assert key != null;
        assert ttl >= 0;

        long expire = ttl > 0 ? System.currentTimeMillis() + ttl : 0;

        CachedObject<V> obj = new CachedObject<>(expire, val);

        Map<K, CachedObject<V>> cache = getOrCreateCache(cacheName);

        CachedObject<V> prev = cache.put(key, obj);

        return prev != null ? prev.obj : null;
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <K, V> V putIfAbsent(String cacheName, K key, V val, long ttl) {
        V v = get(cacheName, key);

        if (v != null)
            return put(cacheName, key, val, ttl);

        return v;
    }

    /** {@inheritDoc} */
    @Override public <K, V> V remove(String cacheName, K key) {
        assert cacheName != null;
        assert key != null;

        Map<K, CachedObject<V>> cache = getOrCreateCache(cacheName);

        CachedObject<V> prev = cache.remove(key);

        return prev != null ? prev.obj : null;
    }

    /** {@inheritDoc} */
    @Override public <K> boolean containsKey(String cacheName, K key) {
        assert cacheName != null;
        assert key != null;

        boolean res = false;

        try {
            res = get(cacheName, key) != null;
        }
        catch (IgniteException ignored) {

        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public int partition(String cacheName, Object key) {
        return -1;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteNodeValidationResult validateNode(ClusterNode node) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<SecuritySubject> authenticatedSubjects() {
        return Collections.emptyList();
    }

    /** {@inheritDoc} */
    @Override public SecuritySubject authenticatedSubject(UUID subjId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public MessageFormatter messageFormatter() {
        if (formatter == null) {
            formatter = new MessageFormatter() {
                @Override public MessageWriter writer(UUID rmtNodeId) {
                    return new DirectMessageWriter(GridIoManager.DIRECT_PROTO_VER);
                }

                @Override public MessageReader reader(UUID rmtNodeId, MessageFactory msgFactory) {
                    return new DirectMessageReader(msgFactory, GridIoManager.DIRECT_PROTO_VER);
                }
            };
        }

        return formatter;
    }

    /** {@inheritDoc} */
    @Override public MessageFactory messageFactory() {
        if (factory == null)
            factory = new GridIoMessageFactory(null);

        return factory;
    }

    /** {@inheritDoc} */
    @Override public boolean isStopping() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean tryFailNode(UUID nodeId, @Nullable String warning) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public void failNode(UUID nodeId, @Nullable String warning) {
        // No-op.
    }

    /** {@inheritDoc} */
    @Override public void addTimeoutObject(IgniteSpiTimeoutObject obj) {
        if (timeoutProcessor != null)
            timeoutProcessor.addTimeoutObject(new GridSpiTimeoutObject(obj));
    }

    /** {@inheritDoc} */
    @Override public void removeTimeoutObject(IgniteSpiTimeoutObject obj) {
        if (timeoutProcessor != null)
            timeoutProcessor.removeTimeoutObject(new GridSpiTimeoutObject(obj));
    }

    /**
     * @param cacheName Cache name.
     * @return Map representing cache.
     */
    @SuppressWarnings("unchecked")
    private <K, V> Map<K, V> getOrCreateCache(String cacheName) {
        synchronized (cache) {
            Map<K, V> map = cache.get(cacheName);

            if (map == null)
                cache.put(cacheName, map = new ConcurrentHashMap<>());

            return map;
        }
    }

    /**
     * Cached object.
     */
    private static class CachedObject<V> {
        /** */
        private long expire;

        /** */
        private V obj;

        /**
         * @param expire Expire time.
         * @param obj Object.
         */
        private CachedObject(long expire, V obj) {
            this.expire = expire;
            this.obj = obj;
        }
    }

    /**
     * This class represents a message listener wrapper that knows about peer deployment.
     */
    private class GridLocalMessageListener implements GridMessageListener {
        /** Predicate listeners. */
        private final IgniteBiPredicate<UUID, Object> predLsnr;

        /** User message topic. */
        private final Object topic;

        /**
         * @param topic User topic.
         * @param predLsnr Predicate listener.
         * @throws IgniteCheckedException If failed to inject resources to predicates.
         */
        GridLocalMessageListener(@Nullable Object topic, @Nullable IgniteBiPredicate<UUID, Object> predLsnr)
            throws IgniteCheckedException {
            this.topic = topic;
            this.predLsnr = predLsnr;
        }

        /** {@inheritDoc} */
        @SuppressWarnings({
            "SynchronizationOnLocalVariableOrMethodParameter", "ConstantConditions",
            "OverlyStrongTypeCast"})
        @Override public void onMessage(UUID nodeId, Object msg) {
            GridIoUserMessage ioMsg = (GridIoUserMessage)msg;

            Object msgBody = ioMsg.body();

            assert msgBody != null || ioMsg.bodyBytes() != null;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            GridLocalMessageListener l = (GridLocalMessageListener)o;

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
            return S.toString(GridLocalMessageListener.class, this);
        }
    }
}
