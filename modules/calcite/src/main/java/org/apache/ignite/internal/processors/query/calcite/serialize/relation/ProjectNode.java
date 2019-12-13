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

package org.apache.ignite.internal.processors.query.calcite.serialize.relation;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.Expression;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.RexToExpTranslator;
import org.apache.ignite.internal.processors.query.calcite.serialize.type.DataType;
import org.apache.ignite.internal.processors.query.calcite.trait.DistributionTraitDef;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class ProjectNode extends RelGraphNode {
    private final List<Expression> projects;
    private final DataType dataType;

    private ProjectNode(List<Expression> projects, DataType dataType) {
        this.projects = projects;
        this.dataType = dataType;
    }

    public static ProjectNode create(IgniteProject rel, RexToExpTranslator rexTranslator) {
        return new ProjectNode(rexTranslator.translate(rel.getProjects()),
            DataType.fromType(rel.getRowType()));
    }

    @Override public RelNode toRel(ConversionContext ctx, List<RelNode> children) {
        RelNode input = F.first(children);
        List<RexNode> projects = ctx.getExpressionTranslator().translate(this.projects);
        RelOptCluster cluster = input.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();

        RelTraitSet traits = cluster.traitSetOf(IgniteConvention.INSTANCE)
            .replaceIf(DistributionTraitDef.INSTANCE, () -> IgniteMdDistribution.project(mq, input, projects));

        return new IgniteProject(cluster, traits, input, projects, dataType.toRelDataType(ctx.getTypeFactory()));
    }
}
