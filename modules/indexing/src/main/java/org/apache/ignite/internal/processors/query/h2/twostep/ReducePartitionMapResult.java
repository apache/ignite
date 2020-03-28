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

package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.h2.util.IntArray;
import org.jetbrains.annotations.Nullable;

/**
 * Result of nodes to partitions mapping for a query or update.
 */
public class ReducePartitionMapResult {
    /** */
    private final Collection<ClusterNode> nodes;

    /** */
    private final Map<ClusterNode, IntArray> partsMap;

    /** */
    private final Map<ClusterNode, IntArray> qryMap;

    /**
     * Constructor.
     *
     * @param nodes Nodes.
     * @param partsMap Partitions map.
     * @param qryMap Nodes map.
     */
    public ReducePartitionMapResult(Collection<ClusterNode> nodes,
        @Nullable Map<ClusterNode, IntArray> partsMap,
        @Nullable Map<ClusterNode, IntArray> qryMap) {
        this.nodes = nodes;
        this.partsMap = partsMap;
        this.qryMap = qryMap;
    }

    /**
     * @return Collection of nodes a message shall be sent to.
     */
    public Collection<ClusterNode> nodes() {
        return nodes;
    }

    /**
     * @return Maps a node to partition array.
     */
    public @Nullable Map<ClusterNode, IntArray> partitionsMap() {
        return partsMap;
    }

    /**
     * @return Maps a node to partition array.
     */
    public @Nullable Map<ClusterNode, IntArray> queryPartitionsMap() {
        return qryMap;
    }
}
