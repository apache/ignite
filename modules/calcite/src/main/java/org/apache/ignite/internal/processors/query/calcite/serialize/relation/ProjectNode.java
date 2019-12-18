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
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.Expression;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.RexToExpTranslator;
import org.apache.ignite.internal.processors.query.calcite.serialize.type.DataType;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Describes {@link IgniteProject}.
 */
public class ProjectNode extends RelGraphNode {
    /** */
    private final List<Expression> projects;

    /** */
    private final DataType dataType;

    /**
     * @param traits   Traits of this relational expression.
     * @param projects Projects.
     * @param dataType Output row type
     */
    private ProjectNode(RelTraitSet traits, List<Expression> projects, DataType dataType) {
        super(traits);
        this.projects = projects;
        this.dataType = dataType;
    }

    /**
     * Factory method.
     *
     * @param rel Project rel.
     * @param rexTranslator Expression translator.
     * @return ProjectNode.
     */
    public static ProjectNode create(IgniteProject rel, RexToExpTranslator rexTranslator) {
        return new ProjectNode(rel.getTraitSet(), rexTranslator.translate(rel.getProjects()),
            DataType.fromType(rel.getRowType()));
    }

    /** {@inheritDoc} */
    @Override public RelNode toRel(ConversionContext ctx, List<RelNode> children) {
        RelNode input = F.first(children);
        RelOptCluster cluster = input.getCluster();
        List<RexNode> projects = ctx.getExpressionTranslator().translate(this.projects);

        return new IgniteProject(cluster, traits.toTraitSet(cluster), input, projects, dataType.toRelDataType(ctx.getTypeFactory()));
    }
}
