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
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rex.RexNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.Expression;
import org.apache.ignite.internal.processors.query.calcite.serialize.expression.RexToExpTranslator;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public class TableModifyNode extends RelGraphNode {
    /** */
    private final List<String> tableName;

    /** */
    private final TableModify.Operation operation;

    /** */
    private final List<String> updateColumnList;

    /** */
    private final List<Expression> sourceExpressionList;

    /** */
    private final boolean flattened;

    /**
     * @param traits Traits of this relational expression.
     * @param tableName Table name.
     * @param operation Operation.
     * @param updateColumnList Update column list.
     * @param sourceExpressionList Source expression list.
     * @param flattened Flattened flag.
     */
    private TableModifyNode(RelTraitSet traits, List<String> tableName, TableModify.Operation operation, List<String> updateColumnList, List<Expression> sourceExpressionList, boolean flattened) {
        super(traits);

        this.tableName = tableName;
        this.operation = operation;
        this.updateColumnList = updateColumnList;
        this.sourceExpressionList = sourceExpressionList;
        this.flattened = flattened;
    }

    /** {@inheritDoc} */
    @Override public IgniteRel toRel(ConversionContext ctx, List<IgniteRel> children) {
        RelOptCluster cluster = ctx.getCluster();
        RelNode input = F.first(children);
        RelOptTable table = ctx.getSchema().getTableForMember(tableName);
        RelTraitSet traits = traitSet(cluster);
        CalciteCatalogReader catalogReader = (CalciteCatalogReader) ctx.getSchema();
        List<RexNode> sourceExpressionList = ctx.getExpressionTranslator().translate(this.sourceExpressionList);

        return new IgniteTableModify(cluster, traits, table, catalogReader, input, operation, updateColumnList, sourceExpressionList, flattened);
    }

    /**
     * Factory method.
     *
     * @param rel Table modify rel.
     * @param rexTranslator Expression translator.
     * @return ProjectNode.
     */
    public static TableModifyNode create(IgniteTableModify rel, RexToExpTranslator rexTranslator) {
        return new TableModifyNode(rel.getTraitSet(), rel.getTable().getQualifiedName(), rel.getOperation(), rel.getUpdateColumnList(),
            rexTranslator.translate(rel.getSourceExpressionList()), rel.isFlattened());
    }
}
