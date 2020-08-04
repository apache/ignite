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

package org.apache.ignite.internal.client.impl.connection;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;
import org.apache.ignite.internal.client.GridClientConfiguration;
import org.apache.ignite.internal.client.GridClientDisconnectedException;
import org.apache.ignite.internal.client.GridClientException;
import org.apache.ignite.internal.client.GridClientNode;
import org.apache.ignite.internal.client.GridClientProtocol;
import org.apache.ignite.internal.client.GridClientTopologyListener;
import org.apache.ignite.internal.client.impl.GridClientNodeImpl;
import org.apache.ignite.internal.client.impl.GridClientThreadFactory;
import org.apache.ignite.internal.client.util.GridClientUtils;
import org.apache.ignite.internal.util.typedef.C1;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.IgniteNodeAttributes.ATTR_MACS;

/**
 * Client topology cache.
 */
public class GridClientTopology {
    /** Logger. */
    private static final Logger log = Logger.getLogger(GridClientTopology.class.getName());

    /** Topology cache */
    private Map<UUID, GridClientNodeImpl> nodes = Collections.emptyMap();

    /** Cached last error prevented topology from update. */
    private GridClientException lastError;

    /** Router addresses from configuration.  */
    private final String routers;

    /**
     * Set of router addresses to infer direct connectivity
     * when client is working in router connection mode.
     * {@code null} when client is working in direct connection node.
     */
    private final Set<InetSocketAddress> routerAddrs;

    /** List of all known local MACs */
    private final Collection<String> macsCache;

    /** Protocol. */
    private final GridClientProtocol prot;

    /** Flag indicating whether metrics should be cached. */
    private final boolean metricsCache;

    /** Flag indicating whether metrics should be cached. */
    private final boolean attrCache;

    /** Lock for topology changing. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Topology listeners. */
    private final Collection<GridClientTopologyListener> topLsnrs = new ConcurrentLinkedQueue<>();

    /** Executor for listener notification. */
    private final ExecutorService exec =
        Executors.newSingleThreadExecutor(new GridClientThreadFactory("top-lsnr", true));

    /**
     * Creates topology instance.
     *
     * @param cfg Client configuration.
     */
    public GridClientTopology(GridClientConfiguration cfg) {
        metricsCache = cfg.isEnableMetricsCache();
        attrCache = cfg.isEnableAttributesCache();
        prot = cfg.getProtocol();

        if (!cfg.getRouters().isEmpty() && cfg.getServers().isEmpty()) {
            routers = cfg.getRouters().toString();

            routerAddrs = U.newHashSet(cfg.getRouters().size());

            for (String router : cfg.getRouters()) {
                int portIdx = router.lastIndexOf(":");

                if (portIdx > 0) {
                    String hostName = router.substring(0, portIdx);

                    try {
                        int port = Integer.parseInt(router.substring(portIdx + 1));

                        InetSocketAddress inetSockAddr = new InetSocketAddress(hostName, port);

                        routerAddrs.add(inetSockAddr);
                    }
                    catch (Exception ignore) {
                        // No-op.
                    }
                }
            }
        }
        else {
            routers = null;

            routerAddrs = Collections.emptySet();
        }

        macsCache = U.allLocalMACs();
    }

    /**
     * Adds topology listener.
     *
     * @param lsnr Topology listener.
     */
    public void addTopologyListener(GridClientTopologyListener lsnr) {
        topLsnrs.add(lsnr);
    }

    /**
     * Removes topology listener.
     *
     * @param lsnr Topology listener.
     */
    public void removeTopologyListener(GridClientTopologyListener lsnr) {
        topLsnrs.remove(lsnr);
    }

    /**
     * Returns all added topology listeners.
     *
     * @return Unmodifiable view of topology listeners.
     */
    public Collection<GridClientTopologyListener> topologyListeners() {
        return Collections.unmodifiableCollection(topLsnrs);
    }

    /**
     * Updates topology if cache enabled. If cache is disabled, returns original node.
     *
     * @param node Converted rest server response.
     * @return Node in topology.
     */
    public GridClientNode updateNode(GridClientNodeImpl node) {
        lock.writeLock().lock();

        try {
            boolean newNode = !nodes.containsKey(node.nodeId());

            GridClientNodeImpl preparedNode = prepareNode(node);

            // We update the whole topology if node was not in topology or we cache metrics.
            if (newNode || metricsCache || attrCache) {
                Map<UUID, GridClientNodeImpl> updatedTop = new HashMap<>(nodes);

                updatedTop.put(node.nodeId(), preparedNode);

                // Change the reference to new topology.
                // So everyone who captured old version will see a consistent snapshot.
                nodes = updatedTop;
                lastError = null;
            }

            if (newNode)
                notifyEvents(Collections.singletonList(new TopologyEvent(true, preparedNode)));

            return preparedNode;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates (if cache is enabled) the whole topology. If cache is disabled, original collection is returned.
     *
     * @param nodeList Converted rest server response.
     * @return Topology nodes.
     */
    public Collection<? extends GridClientNode> updateTopology(Collection<GridClientNodeImpl> nodeList) {
        Collection<TopologyEvent> evts = new LinkedList<>();

        lock.writeLock().lock();

        try {
            Map<UUID, GridClientNodeImpl> updated = new HashMap<>();

            Collection<GridClientNodeImpl> preparedNodes = F.transform(nodeList,
                new C1<GridClientNodeImpl, GridClientNodeImpl>() {
                    @Override public GridClientNodeImpl apply(GridClientNodeImpl e) {
                        return prepareNode(e);
                    }
                });

            for (GridClientNodeImpl node : preparedNodes) {
                updated.put(node.nodeId(), node);

                // Generate add events.
                if (!nodes.containsKey(node.nodeId()))
                    evts.add(new TopologyEvent(true, node));
            }

            for (Map.Entry<UUID, GridClientNodeImpl> e : nodes.entrySet()) {
                if (!updated.containsKey(e.getKey()))
                    evts.add(new TopologyEvent(false, e.getValue()));
            }

            nodes = updated;
            lastError = null;

            if (!evts.isEmpty())
                notifyEvents(evts);

            return preparedNodes;
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Marks topology as failed. After this method called all accessors will throw exception
     * until a next successful update.
     *
     * @param cause Exception caused the failure.
     */
    public void fail(GridClientException cause) {
        lock.writeLock().lock();

        try {
            lastError = cause;

            for (GridClientNode n : nodes.values())
                notifyEvents(Collections.singletonList(new TopologyEvent(false, n)));

            nodes = Collections.emptyMap();
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Updates topology when node that is expected to be in topology fails.
     *
     * @param nodeId Node id for which node failed to be obtained.
     */
    public void nodeFailed(UUID nodeId) {
        lock.writeLock().lock();

        try {
            boolean nodeDeleted = nodes.containsKey(nodeId);

            GridClientNode deleted = null;

            // We update the whole topology if node was not in topology or we cache metrics.
            if (nodeDeleted) {
                Map<UUID, GridClientNodeImpl> updatedTop = new HashMap<>(nodes);

                deleted = updatedTop.remove(nodeId);

                // Change the reference to new topology.
                // So everyone who captured old version will see a consistent snapshot.
                nodes = updatedTop;
            }

            if (nodeDeleted)
                notifyEvents(Collections.singletonList(new TopologyEvent(false, deleted)));
        }
        finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets node from last saved topology snapshot by it's id.
     *
     * @param id Node id.
     * @return Node or {@code null} if node was not found.
     * @throws GridClientException If topology is failed and no nodes available.
     */
    public GridClientNode node(UUID id) throws GridClientException {
        assert id != null;

        lock.readLock().lock();

        try {
            if (lastError != null)
                throw new GridClientDisconnectedException(
                    "Topology is failed [protocol=" + prot + ", routers=" + routers + ']', lastError);
            else
                return nodes.get(id);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets a collection of nodes from last saved topology snapshot by their ids.
     *
     * @param ids Collection of ids for which nodes should be retrieved..
     * @return Collection of nodes that are in topology.
     * @throws GridClientException If topology is failed and no nodes available.
     */
    public Collection<GridClientNode> nodes(Iterable<UUID> ids) throws GridClientException {
        assert ids != null;

        Collection<GridClientNode> res = new LinkedList<>();

        lock.readLock().lock();

        try {
            if (lastError != null)
                throw new GridClientDisconnectedException(
                    "Latest topology update failed.", lastError);

            for (UUID id : ids) {
                GridClientNodeImpl node = nodes.get(id);

                if (node != null)
                    res.add(node);
            }

            return res;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Gets full topology snapshot.
     *
     * @return Collection of nodes that were in last captured topology snapshot.
     * @throws GridClientException If topology is failed and no nodes available.
     */
    public Collection<GridClientNodeImpl> nodes() throws GridClientException {
        lock.readLock().lock();

        try {
            @Nullable GridClientException e = lastError();

            if (e != null)
                throw e;

            return Collections.unmodifiableCollection(nodes.values());
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public @Nullable GridClientException lastError() {
        lock.readLock().lock();

        try {
            if (lastError != null)
                return new GridClientDisconnectedException(
                    "Latest topology update failed.", lastError);
        }
        finally {
            lock.readLock().unlock();
        }

        return null;
    }

    /**
     * @return Whether topology is failed.
     */
    public boolean failed() {
        lock.readLock().lock();

        try {
            return lastError != null;
        }
        finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Shutdowns executor service that performs listener notification.
     */
    public void shutdown() {
        GridClientUtils.shutdownNow(GridClientTopology.class, exec, log);
    }

    /**
     * Updates node properties according to current topology settings.
     * Particularly attributes and metrics caching policies.
     *
     * @param node Node to be processed.
     * @return The same node if cache is enabled or node contains no attributes and metrics,
     *      otherwise will return new node without attributes and metrics.
     */
    private GridClientNodeImpl prepareNode(final GridClientNodeImpl node) {
        final boolean noAttrsAndMetrics =
            (metricsCache && attrCache) || (node.attributes().isEmpty() && node.metrics() == null);

        // Try to bypass object copying.
        if (noAttrsAndMetrics && routerAddrs.isEmpty() && node.connectable())
            return node;

        // Return a new node instance based on the original one.
        GridClientNodeImpl.Builder nodeBuilder = GridClientNodeImpl.builder(node, !attrCache, !metricsCache);

        for (InetSocketAddress addr : node.availableAddresses(prot, true)) {
            boolean router = routerAddrs.isEmpty() || routerAddrs.contains(addr);

            boolean reachable = noAttrsAndMetrics || !addr.getAddress().isLoopbackAddress() ||
                F.containsAny(macsCache, node.<String>attribute(ATTR_MACS).split(", "));

            if (router && reachable) {
                nodeBuilder.connectable(true);

                break;
            }
        }

        return nodeBuilder.build();
    }

    /**
     * Runs listener notification is separate thread.
     *
     * @param evts Event list.
     */
    private void notifyEvents(final Iterable<TopologyEvent> evts) {
        try {
            exec.execute(new Runnable() {
                @Override public void run() {
                    for (TopologyEvent evt : evts) {
                        if (evt.added()) {
                            for (GridClientTopologyListener lsnr : topLsnrs)
                                lsnr.onNodeAdded(evt.node());
                        }
                        else {
                            for (GridClientTopologyListener lsnr : topLsnrs)
                                lsnr.onNodeRemoved(evt.node());
                        }
                    }
                }
            });
        }
        catch (RejectedExecutionException e) {
            log.warning("Unable to notify event listeners on topology change since client is shutting down: " +
                e.getMessage());
        }
    }

    /**
     * Event for node adding and removal.
     */
    private static class TopologyEvent {
        /** Added or removed flag */
        private boolean added;

        /** Node that triggered event. */
        private GridClientNode node;

        /**
         * Creates a new event.
         *
         * @param added If {@code true}, indicates that node was added to topology.
         *      If {@code false}, indicates that node was removed.
         * @param node Added or removed node.
         */
        private TopologyEvent(boolean added, GridClientNode node) {
            this.added = added;
            this.node = node;
        }

        /**
         * @return Flag indicating whether node was added or removed.
         */
        private boolean added() {
            return added;
        }

        /**
         * @return Node that triggered event.
         */
        private GridClientNode node() {
            return node;
        }
    }
}
