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

package org.apache.ignite.internal.processors.cache.query;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.GridDhtPartitionTopology;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.lang.IgniteClosure;

/**
 * Partition set.
 */
public class GridCacheQueryPartSet {
    /** */
    private final ClusterNode node;

    /** Partitions. */
    private final Collection<Integer> parts;

    /**
     * @param node Node.
     */
    public GridCacheQueryPartSet(ClusterNode node) {
        this(node, new LinkedList<Integer>());
    }

    /**
     * @param node Node.
     */
    public GridCacheQueryPartSet(ClusterNode node, Collection<Integer> parts) {
        this.node = node;

        this.parts = parts;
    }

    /**
     * @return Node.
     */
    public ClusterNode node() {
        return node;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return node.id();
    }

    /**
     * @return Partition set size.
     */
    public int size() {
        return parts.size();
    }

    /**
     * Adds partition to partition set.
     *
     * @param part Partition to add.
     * @return {@code True} if partition was added, {@code false} if partition already exists.
     */
    public boolean add(int part) {
        if (!parts.contains(part)) {
            parts.add(part);

            return true;
        }

        return false;
    }

    /**
     * @param part Partition to remove.
     */
    public void remove(Integer part) {
        parts.remove(part); // Remove object, not index.
    }

    /**
     * @return Partitions.
     */
    @SuppressWarnings("TypeMayBeWeakened")
    public Collection<Integer> partitions() {
        return parts;
    }

    /**
     * Checks if partition set contains given partition.
     *
     * @param part Partition to check.
     * @return {@code True} if partition set contains given partition.
     */
    public boolean contains(int part) {
        return parts.contains(part);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "PartSet [nodeId=" + node.id() + ", size=" + parts.size() + ", parts=" + parts + ']';
    }

    /**
     * Transform shortcut.
     *
     * @param nodes Nodes.
     */
    public static Collection<GridCacheQueryPartSet> forNodes(Collection<ClusterNode> nodes) {
        return F.transform(nodes, new IgniteClosure<ClusterNode, GridCacheQueryPartSet>() {
            @Override public GridCacheQueryPartSet apply(ClusterNode node) {
                return new GridCacheQueryPartSet(node);
            }
        });
    }

    public static Set<GridCacheQueryPartSet> forPartitions(GridDhtPartitionTopology topology,
        AffinityTopologyVersion ver, int[] parts) {

        Set<GridCacheQueryPartSet> set = new HashSet<>();

        for (int part : parts) {
            List<ClusterNode> owners = topology.owners(part);
        }

        return null;
    }
}
