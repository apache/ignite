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
import org.apache.calcite.util.Pair;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata.FragmentMetadata;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteReceiver;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.splitter.Edge;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;

/**
 *
 */
public class IgniteMdFragmentInfo implements MetadataHandler<FragmentMetadata> {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            IgniteMethod.FRAGMENT_INFO.method(), new IgniteMdFragmentInfo());

    @Override public MetadataDef<FragmentMetadata> getDef() {
        return FragmentMetadata.DEF;
    }

    public FragmentInfo getFragmentInfo(RelNode rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    public FragmentInfo getFragmentInfo(RelSubset rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    public FragmentInfo getFragmentInfo(SingleRel rel, RelMetadataQuery mq) {
        return fragmentInfo(rel.getInput(), mq);
    }

    public FragmentInfo getFragmentInfo(Join rel, RelMetadataQuery mq) {
        mq = RelMetadataQueryEx.wrap(mq);

        FragmentInfo left = fragmentInfo(rel.getLeft(), mq);
        FragmentInfo right = fragmentInfo(rel.getRight(), mq);

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

    public FragmentInfo getFragmentInfo(IgniteReceiver rel, RelMetadataQuery mq) {
        return new FragmentInfo(Pair.of(rel, rel.source()));
    }

    public FragmentInfo getFragmentInfo(IgniteTableScan rel, RelMetadataQuery mq) {
        return rel.getTable().unwrap(IgniteTable.class).fragmentInfo(Commons.plannerContext(rel));
    }

    public static FragmentInfo fragmentInfo(RelNode rel, RelMetadataQuery mq) {
        return RelMetadataQueryEx.wrap(mq).getFragmentLocation(rel);
    }

    private OptimisticPlanningException planningException(BiRel rel, Exception cause, boolean splitLeft) {
        String msg = "Failed to calculate physical distribution";

        if (splitLeft)
            return new OptimisticPlanningException(msg, new Edge(rel, rel.getLeft(), 0), cause);

        return new OptimisticPlanningException(msg, new Edge(rel, rel.getRight(), 1), cause);
    }
}
