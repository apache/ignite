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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerContext;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.ExpToRexTranslator;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class GraphToRelConverter implements ConversionContext {
    private final RelOptTable.ViewExpander viewExpander;
    private final RelBuilder relBuilder;
    private final ExpToRexTranslator expTranslator;

    public GraphToRelConverter(RelOptTable.ViewExpander viewExpander, RelBuilder relBuilder, SqlOperatorTable operatorTable) {
        this.viewExpander = viewExpander;
        this.relBuilder = relBuilder;

        expTranslator = new ExpToRexTranslator(
            relBuilder.getRexBuilder(),
            getTypeFactory(),
            operatorTable);
    }

    @Override public RelDataTypeFactory getTypeFactory() {
        return getCluster().getTypeFactory();
    }

    @Override public RelOptSchema getSchema() {
        return relBuilder.getRelOptSchema();
    }

    @Override public PlannerContext getContext() {
        return Commons.plannerContext(getCluster().getPlanner().getContext());
    }

    @Override public ExpToRexTranslator getExpressionTranslator() {
        return expTranslator;
    }

    @Override public RelOptCluster getCluster() {
        return relBuilder.getCluster();
    }

    @Override public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        return viewExpander.expandView(rowType, queryString, schemaPath, viewPath);
    }

    public RelNode convert(RelGraph graph) {
        return F.first(convertRecursive(this, graph, graph.nodes().subList(0, 1)));
    }

    private List<RelNode> convertRecursive(ConversionContext ctx, RelGraph graph, List<Ord<RelGraphNode>> src) {
        ImmutableList.Builder<RelNode> b = ImmutableList.builder();

        for (Ord<RelGraphNode> node : src)
            b.add(node.e.toRel(ctx, convertRecursive(ctx, graph, graph.children(node.i))));

        return b.build();
    }
}
