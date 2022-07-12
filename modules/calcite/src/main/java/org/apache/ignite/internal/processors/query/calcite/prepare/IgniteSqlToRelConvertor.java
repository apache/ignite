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

package org.apache.ignite.internal.processors.query.calcite.prepare;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.sql2rel.SqlRexConvertletTable;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.RelBuilder;
import org.checkerframework.checker.nullness.qual.Nullable;

import static java.util.Objects.requireNonNull;

/**
 * Converts a SQL parse tree into a relational algebra operators.
 */
public class IgniteSqlToRelConvertor extends SqlToRelConverter {
    /** */
    public IgniteSqlToRelConvertor(
        RelOptTable.ViewExpander viewExpander,
        @Nullable SqlValidator validator,
        Prepare.CatalogReader catalogReader, RelOptCluster cluster,
        SqlRexConvertletTable convertletTable,
        Config cfg
    ) {
        super(viewExpander, validator, catalogReader, cluster, convertletTable, cfg);
    }

    /** {@inheritDoc} */
    @Override protected RelRoot convertQueryRecursive(SqlNode qry, boolean top, @Nullable RelDataType targetRowType) {
        if (qry.getKind() == SqlKind.MERGE)
            return RelRoot.of(convertMerge((SqlMerge)qry), qry.getKind());
        else
            return super.convertQueryRecursive(qry, top, targetRowType);
    }

    /**
     * This method was copy-pasted from super-method except this changes:
     * - For updateCall we require all columns in the project and should not skip anything.
     * - If there is no updateCall, LEFT JOIN converted to ANTI JOIN.
     */
    private RelNode convertMerge(SqlMerge call) {
        RelOptTable targetTable = getTargetTable(call);

        // convert update column list from SqlIdentifier to String
        final List<String> targetColumnNameList = new ArrayList<>();
        final RelDataType targetRowType = targetTable.getRowType();
        SqlUpdate updateCall = call.getUpdateCall();
        if (updateCall != null) {
            for (SqlNode targetColumn : updateCall.getTargetColumnList()) {
                SqlIdentifier id = (SqlIdentifier)targetColumn;
                RelDataTypeField field =
                    SqlValidatorUtil.getTargetField(
                        targetRowType, typeFactory, id, catalogReader, targetTable);
                assert field != null : "column " + id.toString() + " not found";
                targetColumnNameList.add(field.getName());
            }
        }

        // replace the projection of the source select with a
        // projection that contains the following:
        // 1) the expressions corresponding to the new insert row (if there is
        //    an insert)
        // 2) all columns from the target table (if there is an update)
        // 3) the set expressions in the update call (if there is an update)

        // first, convert the merge's source select to construct the columns
        // from the target table and the set expressions in the update call
        RelNode mergeSourceRel = convertSelect(
            requireNonNull(call.getSourceSelect(), () -> "sourceSelect for " + call), false);

        // then, convert the insert statement so we can get the insert
        // values expressions
        SqlInsert insertCall = call.getInsertCall();
        int nLevel1Exprs = 0;
        List<RexNode> level1InsertExprs = null;
        List<RexNode> level2InsertExprs = null;
        if (insertCall != null) {
            RelNode insertRel = convertInsert(insertCall);

            // if there are 2 level of projections in the insert source, combine
            // them into a single project; level1 refers to the topmost project;
            // the level1 projection contains references to the level2
            // expressions, except in the case where no target expression was
            // provided, in which case, the expression is the default value for
            // the column; or if the expressions directly map to the source
            // table
            level1InsertExprs =
                ((LogicalProject)insertRel.getInput(0)).getProjects();
            if (insertRel.getInput(0).getInput(0) instanceof LogicalProject) {
                level2InsertExprs =
                    ((LogicalProject)insertRel.getInput(0).getInput(0))
                        .getProjects();
            }
            nLevel1Exprs = level1InsertExprs.size();
        }

        LogicalJoin join = (LogicalJoin)mergeSourceRel.getInput(0);

        final List<RexNode> projects = new ArrayList<>();

        for (int level1Idx = 0; level1Idx < nLevel1Exprs; level1Idx++) {
            requireNonNull(level1InsertExprs, "level1InsertExprs");
            if ((level2InsertExprs != null)
                && (level1InsertExprs.get(level1Idx) instanceof RexInputRef)) {
                int level2Idx =
                    ((RexInputRef)level1InsertExprs.get(level1Idx)).getIndex();
                projects.add(level2InsertExprs.get(level2Idx));
            }
            else
                projects.add(level1InsertExprs.get(level1Idx));
        }
        if (updateCall != null) {
            final LogicalProject project = (LogicalProject)mergeSourceRel;
            projects.addAll(project.getProjects());
        }
        else {
            // Convert to ANTI join if there is no UPDATE clause.
            join = join.copy(join.getTraitSet(), join.getCondition(), join.getLeft(), join.getRight(), JoinRelType.ANTI,
                false);
        }

        RelBuilder relBuilder = config.getRelBuilderFactory().create(cluster, null)
            .transform(config.getRelBuilderConfigTransform());

        relBuilder.push(join)
            .project(projects);

        return LogicalTableModify.create(targetTable, catalogReader,
            relBuilder.build(), LogicalTableModify.Operation.MERGE,
            targetColumnNameList, null, false);
    }
}
