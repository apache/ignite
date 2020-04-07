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
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
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
import org.apache.calcite.sql.SqlOperator;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.rex.RexUtil.removeCast;
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
    private static final int EQUALS_MASK = 1;
    private static final int LESS_MASK = EQUALS_MASK << 1;
    private static final int GREATER_MASK = LESS_MASK << 1;

    private final List<RexNode> filters;
    private final int[] projects;
    private final RexNode[] lowerIdxCondition;
    private final RexNode[] upperIdxCondition;
    private final IgniteTable igniteTable;
    private final RelCollation collation;
    private final int[] predicateMasks;

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
        this.igniteTable = tbl.unwrap(IgniteTable.class);
        this.collation = igniteTable.collations().isEmpty() ? RelCollations.EMPTY : igniteTable.collations().get(0);
        this.predicateMasks = new int[collation.getFieldCollations().size()];
        this.lowerIdxCondition = new RexNode[igniteTable.columnDescriptors().length];
        this.upperIdxCondition = new RexNode[igniteTable.columnDescriptors().length];
        buildIndexConditions();

    }

    private void buildIndexConditions() {
        if (!boundsArePossible())
            return;

        assert igniteTable.collations().size() <= 1 : igniteTable.collations();
        assert !filters.isEmpty() : filters;

        RelCollation collation = igniteTable.collations().get(0);
        Map<Integer, RelFieldCollation> idxCols = new HashMap<>(collation.getFieldCollations().size());
        for (RelFieldCollation fc : collation.getFieldCollations())
            idxCols.put(fc.getFieldIndex(), fc);

        int cols = igniteTable.columnDescriptors().length;

        List<RexNode> predicatesConjunction = RexUtil.flattenAnd(filters);

//        if (predicatesConjunction.isAlwaysTrue() || predicatesConjunction.isAlwaysFalse())
//            return; // TODO handle alwaysFalse and alwaysTrue.

        Map<Integer, List<RexCall>> fieldsToPredicates = new HashMap<>(predicatesConjunction.size());

        for (RexNode rexNode : predicatesConjunction) {
            if (!isBinaryComparison(rexNode))
                continue;

            RexCall predCall = (RexCall)rexNode;
            RexInputRef inputRef = (RexInputRef)extractOperand(predCall, true);

            if (inputRef == null) // TODO handle this situation
                continue;

            int constraintFldIdx = inputRef.getIndex();

            List<RexCall> fldPreds = fieldsToPredicates
                .computeIfAbsent(constraintFldIdx, k -> new ArrayList<>(predicatesConjunction.size()));

            // Let RexInputRef be on the left side.
            if (!inputRefOnTheLeft(predCall))
                predCall = (RexCall)RexUtil.invert(getCluster().getRexBuilder(), predCall);

            fldPreds.add(predCall);
        }

        for (int i = 0; i < cols; i++) {
            // TODO aliases or multiple collations
            RelFieldCollation fldCollation = idxCols.get(i);

            if (fldCollation == null)
                continue; // It is not an index field.

            List<RexCall> fldPreds = fieldsToPredicates.get(i);

            if (F.isEmpty(fldPreds))
                continue;

            int idxInCollation = collation.getFieldCollations().indexOf(fldCollation);

            boolean lowerBoundBelow = !fldCollation.getDirection().isDescending();

            for (RexCall pred : fldPreds) {
//                assert pred.getOperands().get(0) instanceof RexInputRef  &&
//                    ((RexSlot)pred.getOperands().get(0)).getIndex() == i : pred;

                RexNode cond = removeCast(pred.operands.get(1));

                assert cond instanceof RexLiteral || cond instanceof RexDynamicParam : cond;

                SqlOperator op = pred.getOperator();
                switch (op.kind) {
                    case EQUALS:
                        predicateMasks[idxInCollation] |= EQUALS_MASK;
                        lowerIdxCondition[i] = cond; // TODO support and merge multiple conditions on the same column.
                        upperIdxCondition[i] = cond;
                        break;

                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        lowerBoundBelow = !lowerBoundBelow;
                        // Fall through.

                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        if (lowerBoundBelow) {
                            lowerIdxCondition[i] = cond;
                            predicateMasks[idxInCollation] |= GREATER_MASK;
                        }
                        else {
                            upperIdxCondition[i] = cond;
                            predicateMasks[idxInCollation] |= LESS_MASK;
                        }
                        break;

                    default:
                        throw new AssertionError("Unknown condition: " + cond);
                }
            }
        }
    }

    private boolean inputRefOnTheLeft(RexCall predCall) {
        RexNode leftOp = predCall.getOperands().get(0);

        leftOp = removeCast(leftOp);

        return leftOp.isA(SqlKind.INPUT_REF);
    }

    private boolean boundsArePossible() {
        if (F.isEmpty(filters))
            return false;

        if (RexUtil.flattenOr(filters).size() > 1)
            return false;

        if (igniteTable.collations().isEmpty())
            return false;

        if (igniteTable.collations().size() > 1) {
            throw new UnsupportedOperationException("At most one table collation is currently supported: " +
                "[collations=" + igniteTable.collations() + ", table=" + igniteTable + ']');
        }
        return true;
    }

    private static RexNode extractOperand(RexCall call, boolean inputRef) {
        assert isBinaryComparison(call);

        RexNode leftOp = call.getOperands().get(0);
        RexNode rightOp = call.getOperands().get(1);

        leftOp = removeCast(leftOp);
        rightOp = removeCast(rightOp);

        // TODO handle correlVariable as constant?
        if (leftOp instanceof RexInputRef && (rightOp instanceof RexLiteral || rightOp instanceof RexDynamicParam))
            return inputRef ? leftOp : rightOp;
        else if ((leftOp instanceof RexLiteral || leftOp instanceof RexDynamicParam) && rightOp instanceof RexInputRef)
            return inputRef ? rightOp : leftOp;

        return null;
    }

//    private static List<RexNode> makeListOfNulls(int cols) {
//        List<RexNode> list = new ArrayList<>(cols);
//        for (int i = 0; i < cols; i++)
//            list.add(null);
//        return list;
//    }
//
//    public static List<RexNode> buildIndexConditions0(
//        Collection<RexNode> filters,
//        RelOptTable relOptTbl
//    ) {
////        if (!nonTrivialBoundsPossible0(filters, relOptTbl))
////            return emptyList();
//
//        // TODO Merge OR filters result using several index cursors
//        // TODO simplify and merge overlapping conditions
//        // TODO do we always scan over index? datapages scan?
//        // TODO IN operator
//        // TODO BETWEEN
//
//        return findBestIndexPredicates0(filters, relOptTbl);
//    }
//
//    public static List<RexNode>  findBestIndexPredicates0(Collection<RexNode> filters, RelOptTable relOptTbl) {
//        List<RelCollation> idxCollations = relOptTbl.getCollationList();
//        RelCollation idxCollation = idxCollations.get(0);
//        List<RexNode> predicatesConjunction = RexUtil.flattenAnd(filters);
//        double bestSelectivity = 1.0;
//        List<RexCall> bestPreds = new ArrayList<>();
//
//        for (RelFieldCollation fldCollation : idxCollation.getFieldCollations()) {
//            double curSelectivity = bestSelectivity;
//            RexCall curPred = null;
//            for (RexNode exp : predicatesConjunction) {
//                if (!suitableForIndex(exp, bestPreds))
//                    continue;
//
//                RexCall pred = (RexCall)exp;
//
//                RexInputRef predInputRef = extractInputRef(pred); // TODO different types of predicates
//
//                if (predInputRef == null)
//                    continue;
//
//                int predColIdx = predInputRef.getIndex();
//
//                if (isKeyAlias(relOptTbl, predColIdx))
//                    predColIdx = QueryUtils.KEY_COL;  // TODO rebuild predicate for using KEY_COL
//
//                if (predColIdx != fldCollation.getFieldIndex())
//                    continue;
//
//                double sel = guessSelectivity(pred) * curSelectivity;
//
//                if (curSelectivity > sel) {
//                    curSelectivity = sel;
//                    curPred = pred;
//                }
//            }
//            if (curPred == null)
//                break;
//
//            bestPreds.add(curPred);
//            bestSelectivity = curSelectivity;
//
//            if (!curPred.isA(EQUALS))
//                break;
//        }
//
//        return extractLiteralsAndParameters(bestPreds,
//            idxCollation.getFieldCollations(),
//            relOptTbl.getRowType().getFieldCount());
//    }
//
//    public static List<RexNode> extractLiteralsAndParameters0(
//        Collection<RexCall> predicates,
//        List<RelFieldCollation> collations,
//        int fieldsCount) {
//        if (predicates.isEmpty())
//            return emptyList();
//
//        List<Integer> collationCols = new ArrayList<>(collations.size());
//        for (RelFieldCollation coll : collations)
//            collationCols.add(coll.getFieldIndex());
//        Mappings.target(collationCols, collationCols.size());
//
//        return emptyList();
//
////        for (RexCall call : )
//    }
//
//    public static boolean isKeyAlias0(RelOptTable relOptTbl, int predColIdx) {
//        IgniteTable igniteTbl = relOptTbl.unwrap(IgniteTable.class);
//        TableDescriptor desc = igniteTbl.descriptor();
//        return predColIdx == desc.keyField();
//    }
//
//    private static boolean suitableForIndex0(RexNode exp, List<RexCall> bestPreds) {
//        // We can apply index conditions to simple binary comparisons only: =, >, <.
//        if (!isBinaryComparison(exp))
//            return false;
//
//        // Using several predicates makes sens only in case of several "=" predicates: x=A AND y=B
//        if (!bestPreds.isEmpty() && !exp.isA(EQUALS))
//            return false;
//
//        return true;
//    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("lower", Arrays.toString(lowerIdxCondition))
            .item("upper", Arrays.toString(upperIdxCondition))
            .item("filters", filters)
            .item("projects", Arrays.toString(projects));
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
        double rows = estimateRowCount(mq);

        RelOptCost cost = planner.getCostFactory().makeCost(rows, 0, 0);

        System.out.println("TableScanCost==" + cost + ", filters=" + filters + ", projects=" + Arrays.toString(projects) + ", tbl=" + table.getQualifiedName());
        // TODO count projects.
        return cost;
    }

    @Override public double estimateRowCount(RelMetadataQuery mq) {
        double rows = table.getRowCount();

        for (int i = 0; i < predicateMasks.length; i++) {
            if ((predicateMasks[i] & EQUALS_MASK) != 0) { // Handling '=' case.
                rows *= 0.15;
            } else if (i == 0) { // Handling '<', '>', '>=', '<=' cases.
                if ((predicateMasks[i] & LESS_MASK) != 0)
                    rows *= 0.5;
                if ((predicateMasks[i] & GREATER_MASK) != 0)
                    rows *= 0.5;
                break;
            }
        }

        if (!F.isEmpty(filters)) {
            Double selectivity = mq.getSelectivity(this, filters.get(0)); // TODO handle multiple items in filter.
            rows *= selectivity;
        }

        if (!F.isEmpty(projects))
            rows *= 0.99; // Encourage projects merged with scans TODO do we really need this multiplication?


        if (!F.isEmpty(filters)) {
            Double selectivity = mq.getSelectivity(this, filters.get(0)); // TODO handle multiple items in filter.
            rows *= selectivity;
        }

        return rows;
    }
}
