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

package org.apache.ignite.internal.managers.discovery;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.GridConcurrentHashSet;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.P1;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class DiscoCache {
    /** Local node. */
    private final ClusterNode loc;

    /** Remote nodes. */
    private final List<ClusterNode> rmtNodes;

    /** All nodes. */
    private final List<ClusterNode> allNodes;

    /** All server nodes. */
    private final List<ClusterNode> srvNodes;

    /** Daemon nodes. */
    private final List<ClusterNode> daemonNodes;

    /** All server nodes. */
    private final List<ClusterNode> srvNodesWithCaches;

    /** All nodes with at least one cache configured. */
    @GridToStringInclude
    private final List<ClusterNode> allNodesWithCaches;

    /** All remote nodes with at least one cache configured. */
    @GridToStringInclude
    private final List<ClusterNode> rmtNodesWithCaches;

    /** Cache nodes by cache name. */
    @GridToStringInclude
    private final Map<Integer, List<ClusterNode>> allCacheNodes;

    /** Affinity cache nodes by cache name. */
    @GridToStringInclude
    private final Map<Integer, List<ClusterNode>> affCacheNodes;

    /** Node map. */
    private final Map<UUID, ClusterNode> nodeMap;

    /** Caches where at least one node has near cache enabled. */
    @GridToStringInclude
    private final Set<Integer> nearEnabledCaches;

    /** Alive nodes. */
    private final Set<UUID> alives = new GridConcurrentHashSet<>();

    /**
     * @param loc Local node.
     * @param rmtNodes Remote nodes.
     * @param allNodes All nodes.
     * @param srvNodes Server nodes.
     * @param daemonNodes Daemon nodes.
     * @param srvNodesWithCaches Server nodes with at least one cache configured.
     * @param allNodesWithCaches All nodes with at least one cache configured.
     * @param rmtNodesWithCaches Remote nodes with at least one cache configured.
     * @param allCacheNodes Cache nodes by cache name.
     * @param affCacheNodes Affinity cache nodes by cache name.
     * @param nodeMap Node map.
     * @param nearEnabledCaches Caches where at least one node has near cache enabled.
     * @param alives Alive nodes.
     */
    DiscoCache(ClusterNode loc,
        List<ClusterNode> rmtNodes,
        List<ClusterNode> allNodes,
        List<ClusterNode> srvNodes,
        List<ClusterNode> daemonNodes,
        List<ClusterNode> srvNodesWithCaches,
        List<ClusterNode> allNodesWithCaches,
        List<ClusterNode> rmtNodesWithCaches,
        Map<Integer, List<ClusterNode>> allCacheNodes,
        Map<Integer, List<ClusterNode>> affCacheNodes,
        Map<UUID, ClusterNode> nodeMap,
        Set<Integer> nearEnabledCaches,
        Set<UUID> alives) {
        this.loc = loc;
        this.rmtNodes = rmtNodes;
        this.allNodes = allNodes;
        this.srvNodes = srvNodes;
        this.daemonNodes = daemonNodes;
        this.srvNodesWithCaches = srvNodesWithCaches;
        this.allNodesWithCaches = allNodesWithCaches;
        this.rmtNodesWithCaches = rmtNodesWithCaches;
        this.allCacheNodes = allCacheNodes;
        this.affCacheNodes = affCacheNodes;
        this.nodeMap = nodeMap;
        this.nearEnabledCaches = nearEnabledCaches;
        this.alives.addAll(alives);
    }

    /** @return Local node. */
    public ClusterNode localNode() {
        return loc;
    }

    /** @return Remote nodes. */
    public List<ClusterNode> remoteNodes() {
        return rmtNodes;
    }

    /** @return All nodes. */
    public List<ClusterNode> allNodes() {
        return allNodes;
    }

    /** @return Server nodes. */
    public List<ClusterNode> serverNodes() {
        return srvNodes;
    }

    /** @return Daemon nodes. */
    public List<ClusterNode> daemonNodes() {
        return daemonNodes;
    }

    /** @return Server nodes with at least one cache configured. */
    public List<ClusterNode> serverNodesWithCaches() {
        return srvNodesWithCaches;
    }

    /**
     * Gets all remote nodes that have at least one cache configured.
     *
     * @return Collection of nodes.
     */
    public List<ClusterNode> remoteNodesWithCaches() {
        return rmtNodesWithCaches;
    }

    /**
     * Gets collection of nodes with at least one cache configured.
     *
     * @return Collection of nodes.
     */
    public List<ClusterNode> allNodesWithCaches() {
        return allNodesWithCaches;
    }

    /**
     * Gets collection of server nodes with at least one cache configured.
     *
     * @return Collection of nodes.
     */
    public Collection<ClusterNode> aliveServerNodes() {
        return F.view(serverNodes(), new P1<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return alives.contains(node.id());
            }
        });
    }

    /**
     * Gets collection of server nodes with at least one cache configured.
     *
     * @return Collection of nodes.
     */
    public Collection<ClusterNode> aliveServerNodesWithCaches() {
        return F.view(serverNodesWithCaches(), new P1<ClusterNode>() {
            @Override public boolean apply(ClusterNode node) {
                return alives.contains(node.id());
            }
        });
    }

    /**
     * @return Oldest alive server node.
     */
    public @Nullable ClusterNode oldestAliveServerNode(){
        Iterator<ClusterNode> it = aliveServerNodes().iterator();
        return it.hasNext() ? it.next() : null;
    }

    /**
     * @return Oldest alive server node with at least one cache configured.
     */
    public @Nullable ClusterNode oldestAliveServerNodeWithCache(){
        Iterator<ClusterNode> it = aliveServerNodesWithCaches().iterator();
        return it.hasNext() ? it.next() : null;
    }

    /**
     * Gets all nodes that have cache with given name.
     *
     * @param cacheName Cache name.
     * @return Collection of nodes.
     */
    public List<ClusterNode> cacheNodes(@Nullable String cacheName) {
        return cacheNodes(CU.cacheId(cacheName));
    }

    /**
     * Gets all nodes that have cache with given ID.
     *
     * @param cacheId Cache ID.
     * @return Collection of nodes.
     */
    public List<ClusterNode> cacheNodes(Integer cacheId) {
        return emptyIfNull(allCacheNodes.get(cacheId));
    }

    /**
     * Gets all nodes that have cache with given ID and should participate in affinity calculation. With
     * partitioned cache nodes with near-only cache do not participate in affinity node calculation.
     *
     * @param cacheName Cache name.
     * @return Collection of nodes.
     */
    public List<ClusterNode> cacheAffinityNodes(@Nullable String cacheName) {
        return cacheAffinityNodes(CU.cacheId(cacheName));
    }

    /**
     * Gets all nodes that have cache with given ID and should participate in affinity calculation. With
     * partitioned cache nodes with near-only cache do not participate in affinity node calculation.
     *
     * @param cacheId Cache ID.
     * @return Collection of nodes.
     */
    public List<ClusterNode> cacheAffinityNodes(int cacheId) {
        return emptyIfNull(affCacheNodes.get(cacheId));
    }

    /**
     * Checks if cache with given ID has at least one node with near cache enabled.
     *
     * @param cacheId Cache ID.
     * @return {@code True} if cache with given name has at least one node with near cache enabled.
     */
    public boolean hasNearCache(int cacheId) {
        return nearEnabledCaches.contains(cacheId);
    }

    /**
     * @param id Node ID.
     * @return Node.
     */
    public @Nullable ClusterNode node(UUID id) {
        return nodeMap.get(id);
    }

    /**
     * Removes left node from alives lists.
     *
     * @param rmvd Removed node.
     */
    public void updateAlives(ClusterNode rmvd) {
        alives.remove(rmvd.id());
    }

    /**
     * Removes left nodes from cached alives lists.
     *
     * @param discovery Discovery manager.
     */
    public void updateAlives(GridDiscoveryManager discovery) {
        for (UUID alive : alives) {
            if (!discovery.alive(alive))
                alives.remove(alive);
        }
    }

    /**
     * Returns {@code True} if all nodes has the given attribute and its value equals to {@code expVal}.
     *
     * @param <T> Attribute Type.
     * @param name Attribute name.
     * @param expVal Expected value.
     * @return {@code True} if all the given nodes has the given attribute and its value equals to {@code expVal}.
     */
    public <T> boolean checkAttribute(String name, T expVal) {
        for (ClusterNode node : allNodes) {
            T attr = node.attribute(name);

            if (attr == null || !expVal.equals(attr))
                return false;
        }

        return true;
    }

    /**
     * @param nodes Cluster nodes.
     * @return Empty collection if nodes list is {@code null}
     */
    private List<ClusterNode> emptyIfNull(List<ClusterNode> nodes) {
        return nodes == null ? Collections.<ClusterNode>emptyList() : nodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(DiscoCache.class, this);
    }
}
