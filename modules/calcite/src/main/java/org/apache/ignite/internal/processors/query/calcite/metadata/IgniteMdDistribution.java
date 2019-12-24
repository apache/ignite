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

import java.util.List;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Values;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.F;

import static org.apache.calcite.rel.RelDistribution.Type.ANY;

/**
 *
 */
public class IgniteMdDistribution implements MetadataHandler<BuiltInMetadata.Distribution> {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.DISTRIBUTION.method, new IgniteMdDistribution());

    @Override public MetadataDef<BuiltInMetadata.Distribution> getDef() {
        return BuiltInMetadata.Distribution.DEF;
    }

    public IgniteDistribution distribution(RelNode rel, RelMetadataQuery mq) {
        return DistributionTraitDef.INSTANCE.getDefault();
    }

    public IgniteDistribution distribution(Filter filter, RelMetadataQuery mq) {
        return filter(mq, filter.getInput(), filter.getCondition());
    }

    public IgniteDistribution distribution(Project project, RelMetadataQuery mq) {
        return project(mq, project.getInput(), project.getProjects());
    }

    public IgniteDistribution distribution(Join join, RelMetadataQuery mq) {
        return join(mq, join.getLeft(), join.getRight(), join.analyzeCondition(), join.getJoinType());
    }

    public IgniteDistribution distribution(RelSubset rel, RelMetadataQuery mq) {
        return rel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
    }

    public IgniteDistribution distribution(TableScan rel, RelMetadataQuery mq) {
        return rel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);
    }

    public IgniteDistribution distribution(Values values, RelMetadataQuery mq) {
        return IgniteDistributions.broadcast();
    }

    public IgniteDistribution distribution(Exchange exchange, RelMetadataQuery mq) {
        return (IgniteDistribution) exchange.distribution;
    }

    public IgniteDistribution distribution(HepRelVertex rel, RelMetadataQuery mq) {
        return _distribution(rel.getCurrentRel(), mq);
    }

    public static IgniteDistribution project(RelMetadataQuery mq, RelNode input, List<? extends RexNode> projects) {
        return project(input.getRowType(), _distribution(input, mq), projects);
    }

    public static IgniteDistribution project(RelDataType inType, IgniteDistribution inDistr, List<? extends RexNode> projects) {
        return inDistr.apply(Project.getPartialMapping(inType.getFieldCount(), projects));
    }

    public static IgniteDistribution filter(RelMetadataQuery mq, RelNode input, RexNode condition) {
        return _distribution(input, mq);
    }

    public static IgniteDistribution join(RelMetadataQuery mq, RelNode left, RelNode right, JoinInfo joinInfo, JoinRelType joinType) {
        return join(_distribution(left, mq), _distribution(right, mq), joinInfo, joinType);
    }

    public static IgniteDistribution join(IgniteDistribution left, IgniteDistribution right, JoinInfo joinInfo, JoinRelType joinType) {
        return F.first(IgniteDistributions.suggestJoin(left, right, joinInfo, joinType)).out();
    }

    public static IgniteDistribution _distribution(RelNode rel, RelMetadataQuery mq) {
        IgniteDistribution distr = rel.getTraitSet().getTrait(DistributionTraitDef.INSTANCE);

        if (distr.getType() != ANY)
            return distr;

        return (IgniteDistribution) mq.distribution(rel);
    }
}
