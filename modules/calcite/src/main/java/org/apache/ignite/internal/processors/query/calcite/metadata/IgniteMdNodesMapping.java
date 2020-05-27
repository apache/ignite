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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata.NodesMappingMetadata;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;

/**
 * Implementation class for {@link RelMetadataQueryEx#nodesMapping(RelNode)} method call.
 */
public class IgniteMdNodesMapping implements MetadataHandler<NodesMappingMetadata> {
    /**
     * Metadata provider, responsible for nodes mapping request. It uses this implementation class under the hood.
     */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            IgniteMethod.NODES_MAPPING.method(), new IgniteMdNodesMapping());

    /** {@inheritDoc} */
    @Override public MetadataDef<NodesMappingMetadata> getDef() {
        return NodesMappingMetadata.DEF;
    }

    /**
     * Requests meta information about nodes capable to execute a query over particular partitions.
     *
     * @param rel Relational node.
     * @param mq Metadata query instance. Used to request appropriate metadata from node children.
     * @return Nodes mapping, representing a list of nodes capable to execute a query over particular partitions.
     */
    public NodesMapping nodesMapping(RelNode rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    /**
     * See {@link IgniteMdNodesMapping#nodesMapping(RelNode, RelMetadataQuery)}
     */
    public NodesMapping nodesMapping(RelSubset rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    /**
     * See {@link IgniteMdNodesMapping#nodesMapping(RelNode, RelMetadataQuery)}
     */
    public NodesMapping nodesMapping(SingleRel rel, RelMetadataQuery mq) {
        return _nodesMapping(rel.getInput(), mq);
    }

    /**
     * See {@link IgniteMdNodesMapping#nodesMapping(RelNode, RelMetadataQuery)}
     *
     * {@link LocationMappingException} may be thrown on two children nodes locations merge. This means
     * that the fragment (which part the parent node is) cannot be executed on any node and additional exchange
     * is needed. This case we throw {@link OptimisticPlanningException} with an edge, where we need the additional
     * exchange. After the exchange is put into the fragment and the fragment is split into two ones, fragment meta
     * information will be recalculated for all fragments.
     */
    public NodesMapping nodesMapping(BiRel rel, RelMetadataQuery mq) {
        NodesMapping left = _nodesMapping(rel.getLeft(), mq);
        NodesMapping right = _nodesMapping(rel.getRight(), mq);

        try {
            return merge(left, right);
        }
        catch (LocationMappingException e) {
            String msg = "Failed to calculate physical distribution";

            // a replicated cache is cheaper to redistribute
            if (!left.hasPartitionedCaches())
                throw new OptimisticPlanningException(msg, rel.getLeft(), e);
            else if (!right.hasPartitionedCaches())
                throw new OptimisticPlanningException(msg, rel.getRight(), e);
            else {
                // both sub-trees have partitioned sources, less cost is better
                RelOptCluster cluster = rel.getCluster();

                RelOptCost leftCost = rel.getLeft().computeSelfCost(cluster.getPlanner(), mq);
                RelOptCost rightCost = rel.getRight().computeSelfCost(cluster.getPlanner(), mq);

                throw new OptimisticPlanningException(msg, leftCost.isLe(rightCost) ? rel.getLeft() : rel.getRight(), e);
            }
        }
    }

    /**
     * See {@link IgniteMdNodesMapping#nodesMapping(RelNode, RelMetadataQuery)}
     *
     * {@link LocationMappingException} may be thrown on two children nodes locations merge. This means
     * that the fragment (which part the parent node is) cannot be executed on any node and additional exchange
     * is needed. This case we throw {@link OptimisticPlanningException} with an edge, where we need the additional
     * exchange. After the exchange is put into the fragment and the fragment is split into two ones, fragment meta
     * information will be recalculated for all fragments.
     */
    public NodesMapping nodesMapping(SetOp rel, RelMetadataQuery mq) {
        NodesMapping res = null;

        for (RelNode input : rel.getInputs()) {
            NodesMapping inputMapping = _nodesMapping(input, mq);

            try {
                res = merge(res, inputMapping);
            }
            catch (LocationMappingException e) {
                throw new OptimisticPlanningException("Failed to calculate physical distribution", input, e);
            }
        }

        return res;
    }

    /**
     * See {@link IgniteMdNodesMapping#nodesMapping(RelNode, RelMetadataQuery)}
     *
     * Prunes involved partitions (hence nodes, involved in query execution) if possible.
     */
    public NodesMapping nodesMapping(IgniteFilter rel, RelMetadataQuery mq) {
        NodesMapping mapping = _nodesMapping(rel.getInput(), mq);

        return mapping == null ? null : mapping.prune(rel);
    }

    /**
     * See {@link IgniteMdNodesMapping#nodesMapping(RelNode, RelMetadataQuery)}
     */
    public NodesMapping nodesMapping(IgniteReceiver rel, RelMetadataQuery mq) {
        return null;
    }

    /**
     * See {@link IgniteMdNodesMapping#nodesMapping(RelNode, RelMetadataQuery)}
     */
    public NodesMapping nodesMapping(IgniteTableScan rel, RelMetadataQuery mq) {
        return rel.getTable().unwrap(IgniteTable.class).mapping(Commons.context(rel));
    }

    /**
     * See {@link IgniteMdNodesMapping#nodesMapping(RelNode, RelMetadataQuery)}
     */
    public NodesMapping nodesMapping(IgniteValues rel, RelMetadataQuery mq) {
        return null;
    }

    /**
     * Fragment info calculation entry point.
     * @param rel Root node of a calculated fragment.
     * @param mq Metadata query instance.
     * @return Fragment meta information.
     */
    public static NodesMapping _nodesMapping(RelNode rel, RelMetadataQuery mq) {
        assert mq instanceof RelMetadataQueryEx;

        return ((RelMetadataQueryEx) mq).nodesMapping(rel);
    }

    /** */
    private static NodesMapping merge(NodesMapping mapping1, NodesMapping mapping2) throws LocationMappingException {
        if (mapping1 == null)
            return mapping2;

        if (mapping2 == null)
            return mapping1;

        return mapping1.mergeWith(mapping2);
    }
}
