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

package org.apache.ignite.internal.client.thin;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.client.ClientPartitionAwarenessMapper;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Client cache partition awareness context.
 */
public class ClientCacheAffinityContext {
    /** If a factory needs to be removed. */
    private static final long REMOVED_TS = 0;

    /** Binary data processor. */
    private final IgniteBinary binary;

    /** Factory for each cache id to produce key to partition mapping functions. */
    private final Map<Integer, ClientPartitionAwarenessMapperHolder> cacheKeyMapperFactoryMap = new HashMap<>();

    /** Contains last topology version and known nodes of this version. */
    private final AtomicReference<TopologyNodes> lastTop = new AtomicReference<>();

    /** Cache IDs, which should be included to the next affinity mapping request. */
    private final Set<Integer> pendingCacheIds = new GridConcurrentHashSet<>();

    /** Current affinity mapping. */
    private volatile ClientCacheAffinityMapping affinityMapping;

    /** Caches that have been requested partition mappings for. */
    private volatile CacheMappingRequest rq;

    /**
     * @param binary Binary data processor.
     */
    public ClientCacheAffinityContext(IgniteBinary binary) {
        this.binary = binary;
    }

    /**
     * Update topology version if it's greater than current version and store nodes for last topology.
     *
     * @param topVer Topology version.
     * @param nodeId Node id.
     * @return {@code True} if last topology was updated to the new version.
     */
    public boolean updateLastTopologyVersion(AffinityTopologyVersion topVer, UUID nodeId) {
        while (true) {
            TopologyNodes lastTop = this.lastTop.get();

            if (lastTop == null || topVer.compareTo(lastTop.topVer) > 0) {
                if (this.lastTop.compareAndSet(lastTop, new TopologyNodes(topVer, nodeId)))
                    return true;
            }
            else if (topVer.equals(lastTop.topVer)) {
                lastTop.nodes.add(nodeId);

                return false;
            }
            else
                return false;
        }
    }

    /**
     * Is affinity update required for given cache.
     *
     * @param cacheId Cache id.
     */
    public boolean affinityUpdateRequired(int cacheId) {
        ClientCacheAffinityMapping mapping = currentMapping();

        if (mapping == null || !mapping.cacheIds().contains(cacheId)) {
            pendingCacheIds.add(cacheId);

            return true;
        }

        return false;
    }

    /**
     * @param ch Payload output channel.
     */
    public void writePartitionsUpdateRequest(PayloadOutputChannel ch) {
        assert rq == null : "Previous mapping request was not properly handled: " + rq;

        final Set<Integer> cacheIds;
        long lastAccessed;

        synchronized (cacheKeyMapperFactoryMap) {
            cacheIds = new HashSet<>(pendingCacheIds);

            pendingCacheIds.removeAll(cacheIds);

            lastAccessed = cacheIds.stream()
                .map(cacheKeyMapperFactoryMap::get)
                .filter(Objects::nonNull)
                .mapToLong(h -> h.ts)
                .reduce(Math::max)
                .orElse(0);
        }

        rq = new CacheMappingRequest(cacheIds, lastAccessed);
        ClientCacheAffinityMapping.writeRequest(ch, rq.caches, rq.ts > 0);
    }

    /**
     * @param ch Payload input channel.
     */
    public synchronized boolean readPartitionsUpdateResponse(PayloadInputChannel ch) {
        if (lastTop.get() == null)
            return false;

        CacheMappingRequest rq0 = rq;

        ClientCacheAffinityMapping newMapping = ClientCacheAffinityMapping.readResponse(ch,
            new Function<Integer, Function<Integer, ClientPartitionAwarenessMapper>>() {
                @Override public Function<Integer, ClientPartitionAwarenessMapper> apply(Integer cacheId) {
                    synchronized (cacheKeyMapperFactoryMap) {
                        ClientPartitionAwarenessMapperHolder hld = cacheKeyMapperFactoryMap.get(cacheId);

                        // Factory concurrently removed on cache destroy.
                        if (hld == null || hld.ts == REMOVED_TS)
                            return null;

                        return hld.factory;
                    }
                }
            }
        );

        // Clean up outdated factories.
        rq0.caches.removeAll(newMapping.cacheIds());

        synchronized (cacheKeyMapperFactoryMap) {
            cacheKeyMapperFactoryMap.entrySet()
                .removeIf(e -> rq0.caches.contains(e.getKey())
                    && e.getValue().ts <= rq0.ts);
        }

        rq = null;

        ClientCacheAffinityMapping oldMapping = affinityMapping;

        if (oldMapping == null || newMapping.compareTo(oldMapping) > 0) {
            affinityMapping = newMapping;

            // Re-request mappings that are out of date.
            if (oldMapping != null)
                pendingCacheIds.addAll(oldMapping.cacheIds());

            return true;
        }

        if (newMapping.compareTo(oldMapping) == 0) {
            affinityMapping = ClientCacheAffinityMapping.merge(oldMapping, newMapping);

            return true;
        }

        // Obsolete mapping.
        return true;
    }

    /**
     * Gets last topology information.
     */
    public TopologyNodes lastTopology() {
        return lastTop.get();
    }

    /**
     * Resets affinity context.
     *
     * @param top Topology which triggers reset.
     */
    public synchronized void reset(TopologyNodes top) {
        if (lastTop.compareAndSet(top, null)) {
            affinityMapping = null;

            pendingCacheIds.clear();
        }
    }

    /**
     * Calculates affinity node for given cache and key.
     *
     * @param cacheId Cache ID.
     * @param key Key.
     * @return Affinity node id or {@code null} if affinity node can't be determined for given cache and key.
     */
    public UUID affinityNode(int cacheId, Object key) {
        ClientCacheAffinityMapping mapping = currentMapping();

        return mapping == null ? null : mapping.affinityNode(binary, cacheId, key);
    }

    /**
     * Calculates affinity node for given cache and partition.
     *
     * @param cacheId Cache ID.
     * @param part Partition.
     * @return Affinity node id or {@code null} if affinity node can't be determined for given cache and partition.
     */
    public UUID affinityNode(int cacheId, int part) {
        ClientCacheAffinityMapping mapping = currentMapping();

        return mapping == null ? null : mapping.affinityNode(cacheId, part);
    }

    /**
     * Current affinity mapping.
     */
    protected ClientCacheAffinityMapping currentMapping() {
        TopologyNodes top = lastTop.get();

        if (top == null)
            return null;

        ClientCacheAffinityMapping mapping = affinityMapping;

        if (mapping == null)
            return null;

        if (top.topVer.compareTo(mapping.topologyVersion()) > 0)
            return null;

        return mapping;
    }

    /**
     * @param cacheId Cache id.
     * @param factory Key mapper factory.
     */
    public void putKeyMapperFactory(int cacheId, Function<Integer, ClientPartitionAwarenessMapper> factory) {
        synchronized (cacheKeyMapperFactoryMap) {
            ClientPartitionAwarenessMapperHolder hld = cacheKeyMapperFactoryMap.computeIfAbsent(cacheId,
                id -> new ClientPartitionAwarenessMapperHolder(factory));

            hld.ts = U.currentTimeMillis();
        }
    }

    /**
     * @param cacheId Cache id.
     */
    public void removeKeyMapperFactory(int cacheId) {
        synchronized (cacheKeyMapperFactoryMap) {
            ClientPartitionAwarenessMapperHolder hld = cacheKeyMapperFactoryMap.get(cacheId);

            if (hld == null)
                return;

            // Schedule cache factory remove.
            hld.ts = REMOVED_TS;
        }
    }

    /**
     * Holder for list of nodes for topology version.
     */
    static class TopologyNodes {
        /** Topology version. */
        private final AffinityTopologyVersion topVer;

        /** Nodes. */
        private final Collection<UUID> nodes = new ConcurrentLinkedQueue<>();

        /**
         * @param topVer Topology version.
         * @param nodeId Node id.
         */
        private TopologyNodes(AffinityTopologyVersion topVer, UUID nodeId) {
            this.topVer = topVer;

            nodes.add(nodeId);
        }

        /**
         * Gets nodes of this topology.
         */
        public Iterable<UUID> nodes() {
            return Collections.unmodifiableCollection(nodes);
        }
    }

    /** Holder of a mapper factory. */
    private static class ClientPartitionAwarenessMapperHolder {
        /** Factory. */
        private final Function<Integer, ClientPartitionAwarenessMapper> factory;

        /** Last accessed timestamp. */
        private long ts;

        /**
         * @param factory Factory.
         */
        public ClientPartitionAwarenessMapperHolder(Function<Integer, ClientPartitionAwarenessMapper> factory) {
            this.factory = factory;
        }
    }

    /** Request of cache mappings. */
    private static class CacheMappingRequest {
        /** Cache ids which have been requested. */
        private final Set<Integer> caches;

        /** Request timestamp. */
        private final long ts;

        /**
         * @param caches Cache ids which have been requested.
         * @param ts Request timestamp.
         */
        public CacheMappingRequest(Set<Integer> caches, long ts) {
            this.caches = caches;
            this.ts = ts;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "CacheMappingRequest{" +
                "caches=" + caches +
                ", ts=" + ts +
                '}';
        }
    }
}
