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

package org.apache.ignite.internal.processors.query.calcite.serialize;

import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;

/**
 *
 */
public class ConversionContext implements RelOptTable.ToRelContext {
    private final IgnitePlanner planner;
    private final SqlOperatorTable operatorTable;
    private final RelBuilder relBuilder;
    private final ExpToRexTranslator expTranslator;

    public ConversionContext(IgnitePlanner planner, RelBuilder relBuilder, SqlOperatorTable operatorTable) {
        this.planner = planner;
        this.relBuilder = relBuilder;
        this.operatorTable = operatorTable;

        expTranslator = new ExpToRexTranslator(rexBuilder(), typeFactory(), operatorTable);
    }

    public IgnitePlanner planner() {
        return planner;
    }

    public RelDataTypeFactory typeFactory() {
        return cluster().getTypeFactory();
    }

    public SqlOperatorTable operatorTable() {
        return operatorTable;
    }

    public RelOptSchema schema() {
        return relBuilder().getRelOptSchema();
    }

    public RelOptCluster cluster() {
        return relBuilder().getCluster();
    }

    public Context context() {
        return cluster().getPlanner().getContext();
    }

    public RelBuilder relBuilder() {
        return relBuilder;
    }

    public RexBuilder rexBuilder() {
        return cluster().getRexBuilder();
    }

    public ExpToRexTranslator expressionTranslator() {
        return expTranslator;
    }

    @Override public RelOptCluster getCluster() {
        return cluster();
    }

    @Override public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath, List<String> viewPath) {
        return planner.expandView(rowType, queryString, schemaPath, viewPath);
    }
}
