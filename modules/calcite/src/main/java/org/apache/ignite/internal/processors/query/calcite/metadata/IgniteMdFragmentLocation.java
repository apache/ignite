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

import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.BiRel;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMetadata.FragmentLocationMetadata;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.Receiver;
import org.apache.ignite.internal.processors.query.calcite.rel.Sender;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.Edge;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;
import org.apache.ignite.internal.util.GridIntList;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 *
 */
public class IgniteMdFragmentLocation implements MetadataHandler<FragmentLocationMetadata> {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            IgniteMethod.FRAGMENT_LOCATION.method(), new IgniteMdFragmentLocation());

    @Override public MetadataDef<FragmentLocationMetadata> getDef() {
        return FragmentLocationMetadata.DEF;
    }

    public FragmentLocation getLocation(RelNode rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    public FragmentLocation getLocation(RelSubset rel, RelMetadataQuery mq) {
        throw new AssertionError();
    }

    public FragmentLocation getLocation(SingleRel rel, RelMetadataQuery mq) {
        return location(rel.getInput(), mq);
    }

    public FragmentLocation getLocation(Sender rel, RelMetadataQuery mq) {
        return rel.location(mq);
    }

    public FragmentLocation getLocation(BiRel rel, RelMetadataQuery mq) {
        mq = RelMetadataQueryEx.wrap(mq);

        FragmentLocation leftLoc = location(rel.getLeft(), mq);
        FragmentLocation rightLoc = location(rel.getRight(), mq);

        try {
            return merge(leftLoc, rightLoc);
        }
        catch (LocationMappingException e) {
            // a replicated cache is cheaper to redistribute
            if (!leftLoc.location.hasPartitionedCaches())
                throw planningException(rel, e, true);
            else if (!rightLoc.location.hasPartitionedCaches())
                throw planningException(rel, e, false);

            // both sub-trees have partitioned sources, less cost is better
            RelOptCluster cluster = rel.getCluster();

            RelOptCost leftCost = rel.getLeft().computeSelfCost(cluster.getPlanner(), mq);
            RelOptCost rightCost = rel.getRight().computeSelfCost(cluster.getPlanner(), mq);

            throw planningException(rel, e, leftCost.isLe(rightCost));
        }
    }

    private OptimisticPlanningException planningException(BiRel rel, Exception cause, boolean splitLeft) {
        String msg = "Failed to calculate physical distribution";

        if (splitLeft)
            return new OptimisticPlanningException(msg, new Edge(rel, rel.getLeft(), 0), cause);

        return new OptimisticPlanningException(msg, new Edge(rel, rel.getRight(), 1), cause);
    }

    public FragmentLocation getLocation(Receiver rel, RelMetadataQuery mq) {
        FragmentLocation res = new FragmentLocation();

        res.remoteInputs = Collections.singletonList(rel);
        res.topVer = rel.getCluster().getPlanner().getContext().unwrap(AffinityTopologyVersion.class);

        return res;
    }

    public FragmentLocation getLocation(IgniteTableScan rel, RelMetadataQuery mq) {
        return rel.location();
    }

    public static FragmentLocation location(RelNode rel, RelMetadataQuery mq) {
        return RelMetadataQueryEx.wrap(mq).getFragmentLocation(rel);
    }

    private static FragmentLocation merge(FragmentLocation left, FragmentLocation right) throws LocationMappingException {
        FragmentLocation res = new FragmentLocation();

        res.location = merge(left.location, right.location);
        res.remoteInputs = merge(left.remoteInputs, right.remoteInputs);
        res.localInputs = merge(left.localInputs, right.localInputs);
        res.topVer = U.firstNotNull(left.topVer, right.topVer);

        return res;
    }

    private static Location merge(Location left, Location right) throws LocationMappingException {
        if (left == null)
            return right;
        if (right == null)
            return left;

        return left.mergeWith(right);
    }

    private static <T> List<T> merge(List<T> left, List<T> right) {
        if (left == null)
            return right;
        if (right == null)
            return left;

        return Commons.union(left, right);
    }

    private static GridIntList merge(GridIntList left, GridIntList right) {
        if (left == null)
            return right;

        if (right != null)
            left.addAll(right);

        return left;
    }
}
