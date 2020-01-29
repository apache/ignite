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

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.ExpToRexTranslator;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;

/**
 * Converts RelGraph to RelNode tree.
 */
public class GraphToRelConverter implements ConversionContext {
    /** */
    private final RelOptTable.ViewExpander viewExpander;

    /** */
    private final RelBuilder relBuilder;

    /** */
    private final ExpToRexTranslator expTranslator;

    /**
     * @param viewExpander View expander.
     * @param relBuilder Rel builder.
     * @param opTable Operations table.
     */
    public GraphToRelConverter(RelOptTable.ViewExpander viewExpander, RelBuilder relBuilder, SqlOperatorTable opTable) {
        this.viewExpander = viewExpander;
        this.relBuilder = relBuilder;

        expTranslator = new ExpToRexTranslator(relBuilder.getRexBuilder(), opTable);
    }

    /** {@inheritDoc} */
    @Override public RelDataTypeFactory getTypeFactory() {
        return getCluster().getTypeFactory();
    }

    /** {@inheritDoc} */
    @Override public RelOptSchema getSchema() {
        return relBuilder.getRelOptSchema();
    }

    /** {@inheritDoc} */
    @Override public PlanningContext getContext() {
        return Commons.context(getCluster().getPlanner().getContext());
    }

    /** {@inheritDoc} */
    @Override public ExpToRexTranslator getExpressionTranslator() {
        return expTranslator;
    }

    /** {@inheritDoc} */
    @Override public RelOptCluster getCluster() {
        return relBuilder.getCluster();
    }

    /** {@inheritDoc} */
    @Override public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        return viewExpander.expandView(rowType, queryString, schemaPath, viewPath);
    }

    /**
     * Converts RelGraph to RelNode tree.
     *
     * @param graph RelGraph.
     * @return RelNode tree.
     */
    public IgniteRel convert(RelGraph graph) {
        return F.first(convertRecursive(this, graph, graph.nodes().subList(0, 1)));
    }

    /** */
    private List<IgniteRel> convertRecursive(ConversionContext ctx, RelGraph graph, List<Ord<RelGraphNode>> src) {
        ImmutableList.Builder<IgniteRel> b = ImmutableList.builder();

        for (Ord<RelGraphNode> node : src)
            b.add(node.e.toRel(ctx, convertRecursive(ctx, graph, graph.children(node.i))));

        return b.build();
    }
}
