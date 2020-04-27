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
 *
 */

package org.apache.ignite.internal.processors.cluster;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import org.apache.ignite.cluster.BaselineNode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.cluster.DetachedClusterNode;
import org.apache.ignite.internal.cluster.NodeOrderComparator;
import org.apache.ignite.internal.util.typedef.internal.CU;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgnitePredicate;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * BaselineTopology represents a set of "database nodes" - nodes responsible for holding persistent-enabled caches
 * and persisting their information to durable storage.
 *
 * Two major features BaselineTopology allows are:
 * <ol>
 *     <li>Protection from conflicting updates.</li>
 *     <li>Cluster auto activation.</li>
 * </ol>
 *
 * <h2>Protection from conflicting updates</h2>
 * <p>
 *     Consider en example: there is a cluster of three nodes, A, B and C, all nodes store persistent data.
 *
 *     Cluster history looks like this:
 *           [A,B,C]
 *           |     \
 *      (1)cluster segmentation, two parts get activated independently
 *           |     |
 *         [A,B]  [C]
 *           |     |
 *      (2)updates to both parts
 *
 *      After independent updates applied to both parts of cluster at point(2) node C should not be allowed to join
 *      [A,B] part.
 *
 *      The following algorithm makes sure node C will never join [A,B] part back:
 *      <ol>
 *          <li>
 *              When the cluster is initially activated BaselineTopology is created; its property <b>branchingPntHash</b>
 *              is calculated based on consistent IDs of all nodes.
 *          </li>
 *          <li>
 *              At point(1) two parts are activated separately; BaselineTopology in each is updated:
 *              old <b>branchingPntHash</b> is moved to <b>branchingHistory</b> list;
 *              new one is calculated based on new set of nodes.
 *          </li>
 *          <li>
 *              If node C tries to join other part of cluster (e.g. after network connectivity repair)
 *              [A,B] cluster checks its <b>branchingPntHash</b> and <b>branchingHistory</b>
 *              to see if branching history of C's BaselineTopology has diverged from [A,B].
 *              If divergence is detected [A,B] cluster refuses to let C into topology.
 *          </li>
 *      </ol>
 *
 *      All new nodes joining a cluster should pass validation process before join; detailed algorithm of the process
 *      is described below.
 * </p>
 *
 * <h3>Joining node validation algorithm</h3>
 * Node joining a cluster firstly reads its local BaselineTopology from metastore and sends it to the cluster.
 *
 * <ol>
 *     <li>
 *         When BaselineTopology is set (e.g. on first activation) or recreated (e.g. with set-baseline command)
 *         its ID on all nodes is incremented by one.
 *
 *         So when cluster receives a join request with BaselineTopology it firstly compares joining node BlT ID with
 *         local BlT ID.
 *
 *         If joining node has a BaselineTopology with ID greater than one in cluster it means that BlT was changed
 *         more times there; therefore new node is not allowed to join the cluster.
 *     </li>
 *     <li>
 *         If user manually activates cluster when some of Baseline nodes are offline no new BlT is created.
 *         Instead current set of online nodes from BaselineTopology is used to update {@link BaselineTopology#branchingPntHash}
 *         property of current BaselineTopology.
 *         Old value of the property is moved to {@link BaselineTopology#branchingHist} list.
 *
 *         If joining node and local BlT IDs are the same then cluster takes <b>branchingPntHash</b> of joining node
 *         and verifies that its local <b>branchingHist</b> contains that hash.
 *
 *         If joining node hash is not presented in cluster branching history list
 *         it means that joining node was activated independently of currently running cluster;
 *         therefore new node is not allowed to join the cluster.
 *
 *         If joining node hash is presented in the history, that it is safe to let the node join the cluster.
 *     </li>
 *     <li>
 *         When BaselineTopology is recreated (e.g. with set-baseline command) previous BaselineTopology is moved
 *         to BaselineHistory (consult source code of {@link GridClusterStateProcessor} for more details).
 *
 *         If cluster sees that joining node BlT ID is less than cluster BlT ID it looks up for BaselineHistory item
 *         for new node ID.
 *         Having this BaselineHistory item cluster verifies that branching history of the item contains
 *         branching point hash of joining node
 *         (similar check as in the case above with only difference that joining node BlT is compared against
 *         BaselineHistory item instead of BaselineTopology).
 *
 *         If new node branching point hash is found in the history than node is allowed to join;
 *         otherwise it is rejected.
 *     </li>
 * </ol>
 *
 * <h2>Cluster auto activation</h2>
 */
public class BaselineTopology implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Consistent ID comparator. */
    private static final Comparator<Object> CONSISTENT_ID_COMPARATOR = new Comparator<Object>() {
        @Override public int compare(Object o1, Object o2) {
            if (o1 instanceof Comparable && o2 instanceof Comparable && o1.getClass().equals(o2.getClass()))
                return ((Comparable)o1).compareTo(o2);

            return o1.toString().compareTo(o2.toString());
        }
    };

    /** */
    private final int id;

    /** Key - node consistent ID, value - node attribute map. */
    private final Map<Object, Map<String, Object>> nodeMap;

    /** Compact ID to consistent ID mapping. */
    private final Map<Short, Object> compactIdMapping;

    /** Consistent ID to compact ID mapping. */
    private final Map<Object, Short> consistentIdMapping;

    /** */
    private BranchingPointType lastBranchingPointType;

    /** Branching point hash. */
    private long branchingPntHash;

    /** History of branching events.
     * Each time when branching point hash changes previous value is added to this list. */
    private final List<Long> branchingHist;

    /**
     * @param nodeMap Map of node consistent ID to it's attributes.
     */
    private BaselineTopology(Map<Object, Map<String, Object>> nodeMap, int id) {
        this.id = id;

        compactIdMapping = U.newHashMap(nodeMap.size());
        consistentIdMapping = U.newHashMap(nodeMap.size());

        this.nodeMap = nodeMap;

        Set<Object> consistentIds = new TreeSet<>(CONSISTENT_ID_COMPARATOR);

        for (Object o : nodeMap.keySet()) {
            branchingPntHash += (long)o.hashCode();

            consistentIds.add(o);
        }

        short compactId = 0;

        for (Object consistentId : consistentIds) {
            compactIdMapping.put(compactId, consistentId);

            consistentIdMapping.put(consistentId, compactId++);
        }

        lastBranchingPointType = BranchingPointType.NEW_BASELINE_TOPOLOGY;

        branchingHist = new ArrayList<>();

        branchingHist.add(branchingPntHash);
    }

    /**
     * @return id of this BaselineTopology.
     */
    public int id() {
        return id;
    }

    /**
     * @return Set of consistent IDs.
     */
    public Set<Object> consistentIds() {
        return nodeMap.keySet();
    }

    /**
     * @return Activation history.
     */
    public List<Long> branchingHistory() {
        return branchingHist;
    }

    /**
     * @return Compact IDs mapping.
     */
    public Map<Short, Object> compactIdMapping() {
        return compactIdMapping;
    }

    /**
     * @return Consistent IDs mapping.
     */
    public Map<Object, Short> consistentIdMapping() {
        return consistentIdMapping;
    }

    /**
     * @return Short consistent Id.
     */
    public Short resolveShortConsistentId(Object constId) {
        return consistentIdMapping.get(constId);
    }

    /**
     * @return Object consistent Id.
     */
    public Object resolveConsistentId(Short constId) {
        return compactIdMapping.get(constId);
    }

    /**
     * @return Activation hash.
     */
    public long branchingPointHash() {
        return branchingPntHash;
    }

    /**
     * @param consId Node consistent ID.
     * @return Node attributes map.
     */
    public Map<String, Object> attributes(Object consId) {
        return nodeMap.get(consId);
    }

    /**
     *
     */
    public List<BaselineNode> currentBaseline() {
        List<BaselineNode> res = new ArrayList<>();

        for (Map.Entry<Object, Map<String, Object>> consIdAttrsEntry : nodeMap.entrySet())
            res.add(new DetachedClusterNode(consIdAttrsEntry.getKey(), consIdAttrsEntry.getValue()));

        return res;
    }

    /**
     * @param consId Consistent ID.
     * @return Baseline node, if present in the baseline, or {@code null} if absent.
     */
    public ClusterNode baselineNode(Object consId) {
        Map<String, Object> attrs = nodeMap.get(consId);

        return attrs != null ? new DetachedClusterNode(consId, attrs) : null;
    }

    /**
     * @param aliveNodes Sorted list of currently alive nodes.
     * @param nodeFilter Node filter.
     * @return Sorted list of baseline topology nodes.
     */
    public List<ClusterNode> createBaselineView(
        Collection<ClusterNode> aliveNodes,
        @Nullable IgnitePredicate<ClusterNode> nodeFilter
    ) {
        List<ClusterNode> res = new ArrayList<>(nodeMap.size());

        boolean nullNodeFilter = nodeFilter == null;

        for (ClusterNode node : aliveNodes) {
            if (nodeMap.containsKey(node.consistentId()) && (nullNodeFilter || CU.affinityNode(node, nodeFilter)))
                res.add(node);
        }

        assert res.size() <= nodeMap.size();

        if (res.size() == nodeMap.size())
            return res;

        Map<Object, ClusterNode> consIdMap = new HashMap<>();

        for (ClusterNode node : aliveNodes) {
            if (nodeMap.containsKey(node.consistentId()) && (nullNodeFilter || CU.affinityNode(node, nodeFilter)))
                consIdMap.put(node.consistentId(), node);
        }

        for (Map.Entry<Object, Map<String, Object>> e : nodeMap.entrySet()) {
            Object consId = e.getKey();

            if (!consIdMap.containsKey(consId)) {
                DetachedClusterNode node = new DetachedClusterNode(consId, e.getValue());

                if (nullNodeFilter || CU.affinityNode(node, nodeFilter))
                    consIdMap.put(consId, node);
            }
        }

        res = new ArrayList<>();

        res.addAll(consIdMap.values());

        Collections.sort(res, NodeOrderComparator.getInstance());

        return res;
    }

    /**
     * @param presentedNodes Nodes present in cluster.
     * @return {@code True} if current topology satisfies baseline.
     */
    public boolean isSatisfied(@NotNull Collection<ClusterNode> presentedNodes) {
        if (presentedNodes.size() < nodeMap.size())
            return false;

        Set<Object> presentedNodeIds = new HashSet<>();

        for (ClusterNode node : presentedNodes)
            presentedNodeIds.add(node.consistentId());

        return presentedNodeIds.containsAll(nodeMap.keySet());
    }

    /**
     * @return Size of the baseline topology.
     */
    public int size() {
        return nodeMap.size();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;

        BaselineTopology top = (BaselineTopology)o;

        return nodeMap != null ? nodeMap.keySet().equals(top.nodeMap.keySet()) : top.nodeMap == null;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return nodeMap != null ? nodeMap.hashCode() : 0;
    }

    /**
     * @param blt1 Baseline topology instance.
     * @param blt2 Baseline topology instance.
     * @return {@code True} if equals.
     */
    public static boolean equals(BaselineTopology blt1, BaselineTopology blt2) {
        if (blt1 == null && blt2 == null)
            return true;

        if (blt1 == null ^ blt2 == null)
            return false;

        return blt1.equals(blt2);
    }

    /**
     * @return {@code true} If it is new baseline topology.
     */
    public boolean isNewTopology() {
        return lastBranchingPointType == BranchingPointType.NEW_BASELINE_TOPOLOGY && id == 0;
    }

    /**
     * @param nodes Nodes.
     * @param id ID of BaselineTopology to build.
     * @return Baseline topology consisting of given nodes.
     */
    @Nullable public static BaselineTopology build(Collection<? extends BaselineNode> nodes, int id) {
        if (nodes == null)
            return null;

        Map<Object, Map<String, Object>> nodeMap = new HashMap<>();

        for (BaselineNode node : nodes)
            nodeMap.put(node.consistentId(), node.attributes());

        return new BaselineTopology(nodeMap, id);
    }

    /**
     * @param blt BaselineTopology to check.
     * @return {@code True} if current BaselineTopology is compatible (the same or a newer one) with passed in Blt.
     */
    boolean isCompatibleWith(BaselineTopology blt) {
        return blt == null || (branchingPntHash == blt.branchingPntHash) || branchingHist.contains(blt.branchingPntHash);
    }

    /**
     * @param nodes Nodes.
     */
    boolean updateHistory(Collection<? extends BaselineNode> nodes) {
        long newTopHash = calculateTopologyHash(nodes);

        lastBranchingPointType = BranchingPointType.CLUSTER_ACTIVATION;

        if (branchingPntHash != newTopHash) {
            branchingPntHash = newTopHash;

            branchingHist.add(newTopHash);

            return true;
        }

        return false;
    }

    /**
     * Resets branching history of the current BaselineTopology.
     * Current branching point hash and list of previous branching hashes are erased
     * and replaced with the hash passed via method parameter.
     *
     * All nodes that were offline when reset happened (thus didn't reset history of their local BaselineTopologies)
     * won't be allowed to join the cluster when started back.
     *
     * @param newBranchingPointHash New branching point hash.
     */
    void resetBranchingHistory(long newBranchingPointHash) {
        lastBranchingPointType = BranchingPointType.BRANCHING_HISTORY_RESET;

        branchingHist.clear();

        branchingPntHash = newBranchingPointHash;
        branchingHist.add(newBranchingPointHash);
    }

    /**
     * @param nodes Nodes.
     */
    private long calculateTopologyHash(Collection<? extends BaselineNode> nodes) {
        long res = 0;

        Set<Object> bltConsIds = nodeMap.keySet();

        for (BaselineNode node : nodes) {
            if (bltConsIds.contains(node.consistentId()))
                res += (long) node.consistentId().hashCode();
        }

        return res;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return "BaselineTopology [id=" + id
            + ", branchingHash=" + branchingPntHash
            + ", branchingType='" + lastBranchingPointType + '\''
            + ", baselineNodes=" + nodeMap.keySet() + ']';
    }
}
