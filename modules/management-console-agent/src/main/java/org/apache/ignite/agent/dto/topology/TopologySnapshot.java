/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.dto.topology;

import java.util.Collection;
import java.util.Map;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.util.typedef.internal.S;

import static java.util.Collections.emptyList;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toMap;

/**
 * DTO for topology snapshot.
 */
public class TopologySnapshot {
    /** */
    private long topVer;

    /** */
    private String crdConsistentId;

    /** */
    private Collection<Node> nodes = emptyList();

    /**
     * @param topVer Topology version.
     * @param crdConsistentId Coordinator node consistent id.
     * @param clusterNodes Cluster nodes.
     * @return Topology snapshot.
     */
    public static TopologySnapshot topology(
        long topVer,
        Object crdConsistentId,
        Collection<ClusterNode> clusterNodes,
        Collection<BaselineNode> baselineNodes
    ) {
        Map<String, Node> nodes = clusterNodes.stream()
            .map(n -> new Node(n).setOnline(true))
            .collect(toMap(Node::getConsistentId, identity()));

        if (baselineNodes != null) {
            for (BaselineNode node : baselineNodes) {
                String id = String.valueOf(node.consistentId());

                if (nodes.containsKey(id))
                    nodes.compute(id, (key, val) -> val.setBaselineNode(true));
                else
                    nodes.put(id, new Node(node).setBaselineNode(true).setOnline(false));
            }
        }

        return new TopologySnapshot(topVer, crdConsistentId, nodes.values());
    }

    /**
     * Default constructor for serialization.
     */
    public TopologySnapshot() {
        // No-op.
    }

    /**
     * Create topology snapshot for cluster.
     *
     * @param topVer Topology version.
     * @param crdConsistentId Coordinator node consistent id.
     * @param nodes List of cluster nodes.
     */
    private TopologySnapshot(long topVer, Object crdConsistentId, Collection<Node> nodes) {
        this.topVer = topVer;
        this.crdConsistentId = String.valueOf(crdConsistentId);
        this.nodes = nodes;
    }

    /**
     * @return Cluster topology version.
     */
    public long getTopologyVersion() {
        return topVer;
    }

    /**
     * @param topVer Cluster topology version.
     */
    public void setTopologyVersion(long topVer) {
        this.topVer = topVer;
    }

    /**
     * @return Coordinator node consistent id.
     */
    public String getCoordinatorConsistentId() {
        return crdConsistentId;
    }

    /**
     * @param crdConsistentId Coordinator node consistent id.
     */
    public void setCoordinatorConsistentId(String crdConsistentId) {
        this.crdConsistentId = crdConsistentId;
    }

    /**
     * @return Cluster nodes.
     */
    public Collection<Node> getNodes() {
        return nodes;
    }

    /**
     * @param nodes Cluster nodes.
     */
    public void setNodes(Collection<Node> nodes) {
        this.nodes = nodes;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(TopologySnapshot.class, this);
    }
}
