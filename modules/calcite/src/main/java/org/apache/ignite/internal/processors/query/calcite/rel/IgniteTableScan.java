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

package org.apache.ignite.internal.processors.query.calcite.rel;

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyList;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;

/**
 * Relational operator that returns the contents of a table.
 */
@SuppressWarnings("AssignmentOrReturnOfFieldWithMutableType")
public class IgniteTableScan extends TableScan implements IgniteRel {
    public static final Set<SqlKind> TREE_INDEX_COMPARISON =
        EnumSet.of(
            EQUALS,
            LESS_THAN, GREATER_THAN,
            GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL);

    private final List<RexNode> filters;
    private final int[] projects;
    private final List<RexCall> indexConditions;

    /**
     * Creates a TableScan.
     *
     * @param cluster Cluster that this relational expression belongs to
     * @param traits Traits of this relational expression
     * @param tbl Table definition.
     */
    public IgniteTableScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        @Nullable List<RexNode> filters,
        @Nullable int[] projects
    ) {
        super(cluster, traits, ImmutableList.of(), tbl);

        this.filters = filters;
        this.projects = projects;
        indexConditions = buildIndexConditions(filters, tbl);
    }

    public static List<RexCall> buildIndexConditions(
        Collection<RexNode> filters,
        RelOptTable relOptTbl
    ) {
        if (!nonTrivialBoundsPossible(filters, relOptTbl))
            return emptyList();

        // TODO Merge OR filters result using several index cursors
        // TODO simplify and merge overlapping conditions
        // TODO do we always scan over index? datapages scan?
        // TODO IN operator
        // TODO BETWEEN

        return findBestIndexPredicates(filters, relOptTbl);
    }

    public static List<RexCall>  findBestIndexPredicates(Collection<RexNode> filters, RelOptTable relOptTbl) {
        List<RelCollation> idxCollations = relOptTbl.getCollationList();
        RelCollation idxCollation = idxCollations.get(0);
        List<RexNode> predicatesConjunction = RexUtil.flattenAnd(filters);
        double bestSelectivity = 1.0;
        List<RexCall> bestPreds = new ArrayList<>();

        for (RelFieldCollation fldCollation : idxCollation.getFieldCollations()) {
            double curSelectivity = bestSelectivity;
            RexCall curPred = null;
            for (RexNode exp : predicatesConjunction) {
                if (!suitableForIndex(exp, bestPreds))
                    continue;

                RexCall call = (RexCall)exp;

                RexInputRef colIdx = extractInputRef(call); // TODO different types of predicates

                if (colIdx.getIndex() != fldCollation.getFieldIndex())
                    continue;

                double sel = guessSelectivity(call) * curSelectivity;

                if (curSelectivity > sel) {
                    curSelectivity = sel;
                    curPred = call;
                }
            }
            if (curPred == null)
                return bestPreds;

            bestPreds.add(curPred);
            bestSelectivity = curSelectivity;

            if (!curPred.isA(EQUALS))
                break;
        }

        return bestPreds;
    }

    private static boolean suitableForIndex(RexNode exp, List<RexCall> bestPreds) {
        // We can apply index conditions to simple binary comparisons only: =, >, <.
        if (!isBinaryComparison(exp))
            return false;

        // Using several predicates makes sens only in case of several "=" predicates: x=A AND y=B
        if (!bestPreds.isEmpty() && !exp.isA(EQUALS))
            return false;

        return true;
    }

    private static boolean nonTrivialBoundsPossible(Collection<RexNode> filters, RelOptTable relOptTbl) {
        if (F.isEmpty(filters))
            return false;

        if (RexUtil.flattenOr(filters).size() > 1)
            return false;

        if (relOptTbl.getCollationList().isEmpty())
            return false;

        if (relOptTbl.getCollationList().size() > 1) {
            throw new UnsupportedOperationException("At most one table collation is currently supported: " +
                "[collations=" + relOptTbl.getCollationList() + ", table=" + relOptTbl + ']');
        }
        return true;
    }

    private static RexInputRef extractInputRef(RexCall call) {
        RexNode leftOp = call.getOperands().get(0);
        RexNode rightOp = call.getOperands().get(1);

        if (leftOp.isA(SqlKind.CAST))
            leftOp = ((RexCall)leftOp).getOperands().get(0);

        if (rightOp.isA(SqlKind.CAST))
            rightOp = ((RexCall)rightOp).getOperands().get(0);

        // TODO handle correlVariable as constant?
        if (leftOp instanceof RexInputRef && (rightOp instanceof RexLiteral || rightOp instanceof RexDynamicParam))
            return (RexInputRef)leftOp;
        else if ((leftOp instanceof RexLiteral || leftOp instanceof RexDynamicParam) && rightOp instanceof RexInputRef)
            return (RexInputRef)rightOp;

        return null;
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("indexConditions", indexConditions)
            .item("filters", filters)
            .item("projects", projects);
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return this;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     *
     */
    public List<RexNode> filters() {
        return filters;
    }

    /**
     *
     */
    public int[] projects() {
        return projects;
    }

    /**
     *
     */
    public List<RexCall> indexConditions() {
        return indexConditions;
    }

    private static double guessSelectivity(RexNode predicate) {
        double sel = 1.0;
        if ((predicate == null) || predicate.isAlwaysTrue()) {
            return sel;
        }

        for (RexNode pred : RelOptUtil.conjunctions(predicate)) {
            if (pred.getKind() == SqlKind.IS_NOT_NULL) {
                sel *= 0.9;
            }
            else if (pred.isA(EQUALS)) {
                sel *= 0.15;
            }
            else if (pred.isA(SqlKind.IN)) {
                sel *= 0.2;
            }
            else if (pred.isA(SqlKind.BETWEEN)) {
                sel *= 0.3;
            }
            else if (pred.isA(SqlKind.COMPARISON)) {
                sel *= 0.4;
            }
            else {
                sel *= 0.25;
            }
        }

        return sel;
    }

    private static boolean isBinaryComparison(RexNode exp) {
        return TREE_INDEX_COMPARISON.contains(exp.getKind()) &&
            (exp instanceof RexCall) && // TODO is it possible to be the not RexCall here?
            ((RexCall)exp).getOperands().size() == 2;
    }

    @Override public RelDataType deriveRowType() {
        if (projects == null)
            return super.deriveRowType();

        final RelDataTypeFactory.Builder builder = getCluster().getTypeFactory().builder();

        final List<RelDataTypeField> fieldList = table.getRowType().getFieldList();

        for (int project : projects)
            builder.add(fieldList.get(project));

        return builder.build();
    }

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = table.getRowCount();
        double cpu = rows;

        if (indexConditions != null)
            rows *= 0.1;
//        else if (indexConditions.lower() != null || indexConditions.upper() != null)
//            rows *= 0.5;

        if (!F.isEmpty(filters)) {
            Double selectivity = mq.getSelectivity(this, filters.get(0)); // TODO handle multiple items in filter.
            rows *= selectivity;

            if (!indexConditions.isEmpty()) {
                Double idxPredSelectivity = mq.getSelectivity(this, indexConditions.get(0));

                cpu *=  idxPredSelectivity;
                rows *= idxPredSelectivity;
            }
        }

//        if (!F.isEmpty(projects))
//            rows *= 0.9;

        RelOptCost cost = planner.getCostFactory().makeCost(rows, cpu, 0);

        System.out.println("TableScanCost==" + cost + ", filters=" + filters + ", projects=" + Arrays.toString(projects) + ", tbl=" + table.getQualifiedName());
        // TODO count projects.
        return cost;
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
        double rows = table.getRowCount();
        double cpu = rows;

        if (indexConditions != null)
            rows *= 0.1;
//        else if (indexConditions.lower() != null || indexConditions.upper() != null)
//            rows *= 0.5;

        if (!F.isEmpty(filters)) {
            Double selectivity = mq.getSelectivity(this, filters.get(0)); // TODO handle multiple items in filter.
            rows *= selectivity;
        }

        return rows;
    }
}
