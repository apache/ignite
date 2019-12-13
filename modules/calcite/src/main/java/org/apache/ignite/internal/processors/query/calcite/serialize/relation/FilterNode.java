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

package org.apache.ignite.internal.processors.query.calcite.serialize.relation;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.Expression;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.RexToExpTranslator;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class FilterNode extends RelGraphNode {
    private final Expression condition;

    private FilterNode(Expression condition) {
        this.condition = condition;
    }

    public static FilterNode create(IgniteFilter rel, RexToExpTranslator expTranslator) {
        return new FilterNode(expTranslator.translate(rel.getCondition()));
    }

    @Override public RelNode toRel(ConversionContext ctx, List<RelNode> children) {
        RelNode input = F.first(children);
        RexNode condition = this.condition.implement(ctx.getExpressionTranslator());
        RelOptCluster cluster = input.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();

        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
            .replaceIf(DistributionTraitDef.INSTANCE, () -> IgniteMdDistribution.filter(mq, input, condition));

        return new IgniteFilter(cluster, traits, input, condition);
    }
}
