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

package org.apache.ignite.internal.processors.query.calcite.exec;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Spool;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.Pair;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.processors.query.QueryUtils;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePlanner;
import org.apache.ignite.internal.processors.query.calcite.prepare.IgniteRelShuttle;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlannerPhase;
import org.apache.ignite.internal.processors.query.calcite.prepare.PlanningContext;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.ColumnDefinition;
import org.apache.ignite.internal.processors.query.calcite.prepare.ddl.CreateTableCommand;
import org.apache.ignite.internal.processors.query.calcite.rel.AbstractIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteIndexScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteProject;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteRel;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableModify;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableScan;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTableSpool;
import org.apache.ignite.internal.processors.query.calcite.schema.ColumnDescriptor;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.schema.LazyRelOptTable;
import org.apache.ignite.internal.processors.query.calcite.schema.SchemaHolder;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.processors.query.calcite.util.HintUtils;
import org.apache.ignite.internal.util.typedef.F;

/** */
public class PlannerHelper {
    /**
     * Default constructor.
     */
    private PlannerHelper() {

    }

    /**
     * @param sqlNode Sql node.
     * @param planner Planner.
     * @param log Logger.
     */
    public static IgniteRel optimize(SqlNode sqlNode, IgnitePlanner planner, IgniteLogger log) {
        try {
            // Convert to Relational operators graph
            RelRoot root = planner.rel(sqlNode);

            RelNode rel = root.rel;

            if (HintUtils.containsDisabledRules(root.hints))
                planner.setDisabledRules(HintUtils.disabledRules(root.hints));

            // Transformation chain
            rel = planner.transform(PlannerPhase.HEP_DECORRELATE, rel.getTraitSet(), rel);

            rel = planner.trimUnusedFields(root.withRel(rel)).rel;

            rel = planner.transform(PlannerPhase.HEP_FILTER_PUSH_DOWN, rel.getTraitSet(), rel);

            rel = planner.transform(PlannerPhase.HEP_PROJECT_PUSH_DOWN, rel.getTraitSet(), rel);

            RelTraitSet desired = rel.getCluster().traitSet()
                .replace(IgniteConvention.INSTANCE)
                .replace(IgniteDistributions.single())
                .replace(root.collation == null ? RelCollations.EMPTY : root.collation)
                .simplify();

            IgniteRel igniteRel = planner.transform(PlannerPhase.OPTIMIZATION, desired, rel);

            if (!root.isRefTrivial()) {
                final List<RexNode> projects = new ArrayList<>();
                final RexBuilder rexBuilder = igniteRel.getCluster().getRexBuilder();

                for (int field : Pair.left(root.fields))
                    projects.add(rexBuilder.makeInputRef(igniteRel, field));

                igniteRel = new IgniteProject(igniteRel.getCluster(), desired, igniteRel, projects, root.validatedRowType);
            }

            if (sqlNode.isA(ImmutableSet.of(SqlKind.INSERT, SqlKind.UPDATE, SqlKind.MERGE)))
                igniteRel = new FixDependentModifyNodeShuttle().visit(igniteRel);

            return igniteRel;
        }
        catch (Throwable ex) {
            log.error("Unexpected error at query optimizer.", ex);
            log.error(planner.dump());

            throw ex;
        }
    }

    /**
     * Creates physical plan for "INSERT INTO table SELECT ..." based on "CREATE TABLE table AS SELECT ..." statement.
     */
    public static IgniteRel makeCreateTableAsSelectPlan(CreateTableCommand createTableCmd, PlanningContext ctx,
        IgniteLogger log, SchemaHolder schemaHolder) {
        assert createTableCmd.query() != null;

        // query() - already validated SqlNode.
        IgniteRel qryRel = optimize(createTableCmd.query(), ctx.planner(), log);

        String schemaName = createTableCmd.schemaName();
        String tableName = createTableCmd.tableName();

        Supplier<Table> tableSupplier = () -> schemaHolder.schema().getSubSchema(schemaName).getTable(tableName);

        RelDataTypeFactory.Builder rowTypeBuilder = new RelDataTypeFactory.Builder(ctx.typeFactory());
        List<RexNode> projects = new ArrayList<>();
        RexBuilder rexBuilder = qryRel.getCluster().getRexBuilder();

        RelDataType objType = ctx.typeFactory().createJavaType(Object.class);

        rowTypeBuilder.add(QueryUtils.KEY_FIELD_NAME, objType);
        rowTypeBuilder.add(QueryUtils.VAL_FIELD_NAME, objType);

        projects.add(rexBuilder.makeNullLiteral(objType));
        projects.add(rexBuilder.makeNullLiteral(objType));

        RelDataType inputRowDataType = qryRel.getRowType();
        List<ColumnDefinition> cols = createTableCmd.columns();

        assert inputRowDataType.getFieldCount() == cols.size();

        for (int i = 0; i < inputRowDataType.getFieldCount(); i++) {
            RelDataTypeField fld = inputRowDataType.getFieldList().get(i);
            ColumnDefinition col = cols.get(i);

            rowTypeBuilder.add(col.name(), col.type());

            if (F.eq(col.type(), fld.getType()))
                projects.add(rexBuilder.makeInputRef(fld.getType(), i));
            else
                projects.add(rexBuilder.makeCast(col.type(), rexBuilder.makeInputRef(fld.getType(), i)));
        }

        RelDataType outputRowDataType = rowTypeBuilder.build();

        IgniteRel projectRel = new IgniteProject(
            qryRel.getCluster(),
            qryRel.getTraitSet(),
            qryRel,
            projects,
            outputRowDataType
        );

        return new IgniteTableModify(
            qryRel.getCluster(),
            qryRel.getTraitSet(),
            new LazyRelOptTable(tableSupplier, outputRowDataType, tableName),
            projectRel,
            TableModify.Operation.INSERT,
            null,
            null,
            false
        );
    }

    /**
     * This shuttle analyzes a relational tree and inserts an eager spool node
     * just under the TableModify node in case latter depends upon a table used
     * to query the data for modify node to avoid the double processing
     * of the retrieved rows.
     * <p/>
     * It considers two cases: <ol>
     *     <li>
     *         Modify node produces rows to insert, then a spool is required.
     *     </li>
     *     <li>
     *         Modify node updates rows only, then a spool is required if 1) we
     *         are scaning an index and 2) any of the indexed column is updated
     *         by modify node.
     *     </li>
     * <ol/>
     *
     */
    private static class FixDependentModifyNodeShuttle extends IgniteRelShuttle {
        /**
         * Flag indicates whether we should insert a spool or not.
         */
        private boolean spoolNeeded;

        /** Current modify node. */
        private IgniteTableModify modifyNode;

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteTableModify rel) {
            assert modifyNode == null;

            modifyNode = rel;

            if (rel.isDelete())
                return rel;

            processNode(rel);

            if (spoolNeeded) {
                IgniteTableSpool spool = new IgniteTableSpool(
                    rel.getCluster(),
                    rel.getInput().getTraitSet(),
                    Spool.Type.EAGER,
                    rel.getInput()
                );

                rel.replaceInput(0, spool);
            }

            return rel;
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteTableScan rel) {
            return processScan(rel);
        }

        /** {@inheritDoc} */
        @Override public IgniteRel visit(IgniteIndexScan rel) {
            return processScan(rel);
        }

        /** {@inheritDoc} */
        @Override protected IgniteRel processNode(IgniteRel rel) {
            List<IgniteRel> inputs = Commons.cast(rel.getInputs());

            for (int i = 0; i < inputs.size(); i++) {
                if (spoolNeeded)
                    break;

                visitChild(rel, i, inputs.get(i));
            }

            return rel;
        }

        /**
         * Process a scan node and raise a {@link #spoolNeeded flag} if needed.
         *
         * @param scan TableScan to analize.
         * @return The input rel.
         */
        private IgniteRel processScan(TableScan scan) {
            IgniteTable tbl = modifyNode != null ? modifyNode.getTable().unwrap(IgniteTable.class) : null;

            if (tbl == null || scan.getTable().unwrap(IgniteTable.class) != tbl)
                return (IgniteRel)scan;

            if (modifyNodeInsertsData()) {
                spoolNeeded = true;

                return (IgniteRel)scan;
            }

            // for update-only node the spool needed if any of the updated
            // column is part of the index we are going to scan
            if (scan instanceof IgniteTableScan)
                return (IgniteRel)scan;

            ImmutableSet<Integer> indexedCols = ImmutableSet.copyOf(
                tbl.getIndex(((AbstractIndexScan)scan).indexName()).collation().getKeys());

            spoolNeeded = modifyNode.getUpdateColumnList().stream()
                .map(tbl.descriptor()::columnDescriptor)
                .map(ColumnDescriptor::fieldIndex)
                .anyMatch(indexedCols::contains);

            return (IgniteRel)scan;
        }

        /**
         * @return {@code true} in case {@link #modifyNode} produces any insert.
         */
        private boolean modifyNodeInsertsData() {
            return modifyNode.isInsert(); // MERGE should be analyzed too
            // but currently it is not implemented
        }
    }
}
