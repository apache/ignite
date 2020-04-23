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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Zk Cluster Nodes.
 */
public class ZkClusterNodes {
    /** */
    final ConcurrentSkipListMap<Long, ZookeeperClusterNode> nodesByOrder = new ConcurrentSkipListMap<>();

    /** */
    final ConcurrentSkipListMap<Long, ZookeeperClusterNode> nodesByInternalId = new ConcurrentSkipListMap<>();

    /** */
    final ConcurrentHashMap<UUID, ZookeeperClusterNode> nodesById = new ConcurrentHashMap<>();

    /**
     * @return Remote nodes.
     */
    public Collection<ClusterNode> remoteNodes() {
        List<ClusterNode> nodes = new ArrayList<>();

        for (ClusterNode node : nodesById.values()) {
            if (!node.isLocal())
                nodes.add(node);
        }

        return nodes;
    }

    /**
     * Checks if all nodes pass the given predicate.
     *
     * @param p Predicate to check.
     * @return {@code} True if all nodes pass the given filter, {@code false} if there is at least one node failing
     *      the predicate.
     */
    public boolean isAllNodes(IgnitePredicate<ClusterNode> p) {
        for (ZookeeperClusterNode node : nodesById.values()) {
            if (!p.apply(node))
                return false;
        }

        return true;
    }

    /**
     * @return Current nodes in topology.
     */
    @SuppressWarnings("unchecked")
    List<ClusterNode> topologySnapshot() {
        return new ArrayList<>((Collection)nodesByOrder.values());
    }

    /**
     * @param node New node.
     */
    void addNode(ZookeeperClusterNode node) {
        assert node.id() != null : node;
        assert node.order() > 0 : node;

        ZookeeperClusterNode old = nodesById.put(node.id(), node);

        assert old == null : old;

        old = nodesByOrder.put(node.order(), node);

        assert old == null : old;

        old = nodesByInternalId.put(node.internalId(), node);

        assert old == null : old;
    }

    /**
     * @param internalId Node internal ID.
     * @return Removed node.
     */
    ZookeeperClusterNode removeNode(long internalId) {
        ZookeeperClusterNode node = nodesByInternalId.remove(internalId);

        assert node != null : internalId;
        assert node.order() > 0 : node;

        Object rvmd = nodesByOrder.remove(node.order());

        assert rvmd != null;

        rvmd = nodesById.remove(node.id());

        assert rvmd != null;

        return node;
    }
}
