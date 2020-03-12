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
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata.FragmentMetadata;
import org.apache.ignite.internal.processors.query.calcite.prepare.Edge;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteValues;
import org.apache.ignite.internal.processors.query.calcite.schema.DistributedTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;

/**
 * Implementation class for {@link RelMetadataQueryEx#getFragmentInfo(RelNode)} method call.
 */
public class IgniteMdFragmentInfo implements MetadataHandler<FragmentMetadata> {
    /**
     * Metadata provider, responsible for fragment meta information request. It uses this implementation class under the hood.
     */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            IgniteMethod.FRAGMENT_INFO.method(), new IgniteMdFragmentInfo());

    /** {@inheritDoc} */
    @Override public MetadataDef<FragmentMetadata> getDef() {
        return FragmentMetadata.DEF;
    }

    /**
     * Requests meta information about a fragment with the given relation node at the head of the fragment, mainly it is data
     * location and a list of nodes, capable to execute the fragment on.
     *
     * @param rel Relational node.
     * @param mq Metadata query instance. Used to request appropriate metadata from node children.
     * @return Fragment meta information.
     */
    public FragmentInfo fragmentInfo(RelNode rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    /**
     * See {@link IgniteMdFragmentInfo#fragmentInfo(RelNode, RelMetadataQuery)}
     */
    public FragmentInfo fragmentInfo(RelSubset rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    /**
     * See {@link IgniteMdFragmentInfo#fragmentInfo(RelNode, RelMetadataQuery)}
     *
     * Prunes involved partitions (hence nodes, involved in query execution) if possible.
     */
    public FragmentInfo fragmentInfo(IgniteFilter rel, RelMetadataQuery mq) {
        return _fragmentInfo(rel.getInput(), mq).prune(rel);
    }

    /**
     * See {@link IgniteMdFragmentInfo#fragmentInfo(RelNode, RelMetadataQuery)}
     */
    public FragmentInfo fragmentInfo(SingleRel rel, RelMetadataQuery mq) {
        return _fragmentInfo(rel.getInput(), mq);
    }

    /**
     * See {@link IgniteMdFragmentInfo#fragmentInfo(RelNode, RelMetadataQuery)}
     *
     * {@link LocationMappingException} may be thrown on two children nodes locations merge. This means
     * that the fragment (which part the parent node is) cannot be executed on any node and additional exchange
     * is needed. This case we throw {@link OptimisticPlanningException} with an edge, where we need the additional
     * exchange. After the exchange is put into the fragment and the fragment is split into two ones, fragment meta
     * information will be recalculated for all fragments.
     */
    public FragmentInfo fragmentInfo(Join rel, RelMetadataQuery mq) {
        mq = RelMetadataQueryEx.wrap(mq);

        FragmentInfo left = _fragmentInfo(rel.getLeft(), mq);
        FragmentInfo right = _fragmentInfo(rel.getRight(), mq);

        try {
            return left.merge(right);
        }
        catch (LocationMappingException e) {
            // a replicated cache is cheaper to redistribute
            if (!left.mapping().hasPartitionedCaches())
                throw planningException(rel, e, true);
            else if (!right.mapping().hasPartitionedCaches())
                throw planningException(rel, e, false);

            // both sub-trees have partitioned sources, less cost is better
            RelOptCluster cluster = rel.getCluster();

            RelOptCost leftCost = rel.getLeft().computeSelfCost(cluster.getPlanner(), mq);
            RelOptCost rightCost = rel.getRight().computeSelfCost(cluster.getPlanner(), mq);

            throw planningException(rel, e, leftCost.isLe(rightCost));
        }
    }

    /**
     * See {@link IgniteMdFragmentInfo#fragmentInfo(RelNode, RelMetadataQuery)}
     */
    public FragmentInfo fragmentInfo(IgniteReceiver rel, RelMetadataQuery mq) {
        return new FragmentInfo(rel.source());
    }

    /**
     * See {@link IgniteMdFragmentInfo#fragmentInfo(RelNode, RelMetadataQuery)}
     */
    public FragmentInfo fragmentInfo(IgniteTableScan rel, RelMetadataQuery mq) {
        return new FragmentInfo(rel.getTable().unwrap(DistributedTable.class).mapping(Commons.context(rel)));
    }

    /**
     * See {@link IgniteMdFragmentInfo#fragmentInfo(RelNode, RelMetadataQuery)}
     */
    public FragmentInfo fragmentInfo(IgniteValues rel, RelMetadataQuery mq) {
        return new FragmentInfo();
    }

    /**
     * Fragment info calculation entry point.
     * @param rel Root node of a calculated fragment.
     * @param mq Metadata query instance.
     * @return Fragment meta information.
     */
    public static FragmentInfo _fragmentInfo(RelNode rel, RelMetadataQuery mq) {
        return RelMetadataQueryEx.wrap(mq).getFragmentInfo(rel);
    }

    /** */
    private static OptimisticPlanningException planningException(BiRel rel, Exception cause, boolean splitLeft) {
        String msg = "Failed to calculate physical distribution";

        if (splitLeft)
            return new OptimisticPlanningException(msg, new Edge(rel, rel.getLeft(), 0), cause);

        return new OptimisticPlanningException(msg, new Edge(rel, rel.getRight(), 1), cause);
    }
}
