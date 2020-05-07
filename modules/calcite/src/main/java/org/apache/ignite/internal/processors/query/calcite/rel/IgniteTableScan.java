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
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;
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

    private final String idxName;
    private final RexNode condition;
    private final List<RexNode> lowerIdxCondition;
    private final List<RexNode> upperIdxCondition;
    private final IgniteTable igniteTable;
    private final RelCollation collation;
    private final int[] predicateMasks;

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteTableScan(RelInput input) {
        super(Commons.changeTraits(input, IgniteConvention.INSTANCE));
        idxName = input.getString("index");
        condition = input.getExpression("filters");
        lowerIdxCondition = input.getExpressionList("lower");
        upperIdxCondition = input.getExpressionList("upper");
        igniteTable = getTable().unwrap(IgniteTable.class);
        collation = igniteTable.getIndex(idxName).collation();
        predicateMasks = new int[0];
    }

    /**
     * Creates a TableScan.
     * @param cluster Cluster that this relational expression belongs to
     * @param traits Traits of this relational expression
     * @param tbl Table definition.
     * @param idxName Index name.
     * @param condition Filters for scan.
     */
    public IgniteTableScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        String idxName,
        @Nullable RexNode condition
    ) {
        super(cluster, traits, ImmutableList.of(), tbl);

        this.idxName = idxName;
        this.condition = condition;
        this.igniteTable = tbl.unwrap(IgniteTable.class);
        RelCollation coll = traits.getTrait(RelCollationTraitDef.INSTANCE);
        this.collation = coll == null ? RelCollationTraitDef.INSTANCE.getDefault() : coll;
        this.predicateMasks = new int[collation.getFieldCollations().size()];
        this.lowerIdxCondition = makeListOfNullLiterals(igniteTable.columnDescriptors().length);
        this.upperIdxCondition = makeListOfNullLiterals(igniteTable.columnDescriptors().length);
        buildIndexConditions();
    }

    //        // TODO Merge OR filters result using several index cursors
//        // TODO simplify and merge overlapping conditions
//        // TODO do we always scan over index? datapages scan?
//        // TODO IN operator
//        // TODO BETWEEN
//

    private void buildIndexConditions() {
        if (!boundsArePossible())
            return;

        assert condition != null;

        Map<Integer, RelFieldCollation> idxCols = new HashMap<>(collation.getFieldCollations().size());
        for (RelFieldCollation fc : collation.getFieldCollations())
            idxCols.put(fc.getFieldIndex(), fc);

        int cols = igniteTable.columnDescriptors().length;

        Map<Integer, List<RexCall>> fieldsToPredicates = mapPredicatesToFields();

        for (int i = 0; i < cols; i++) {
            RelFieldCollation fldCollation = idxCols.get(i);

            if (fldCollation == null)
                continue; // It is not an index field.

            List<RexCall> fldPreds = fieldsToPredicates.get(i);

            if (F.isEmpty(fldPreds))
                continue;

            int idxInCollation = collation.getFieldCollations().indexOf(fldCollation);

            boolean lowerBoundBelow = !fldCollation.getDirection().isDescending();

            for (RexCall pred : fldPreds) {
                RexNode cond = removeCast(pred.operands.get(1));

                assert cond instanceof RexLiteral || cond instanceof RexDynamicParam : cond;

                SqlOperator op = pred.getOperator();
                switch (op.kind) {
                    case EQUALS:
                        predicateMasks[idxInCollation] |= EQUALS_MASK;
                        lowerIdxCondition.set(i, cond); // TODO support and merge multiple conditions on the same column.
                        upperIdxCondition.set(i, cond);
                        break;

                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        lowerBoundBelow = !lowerBoundBelow;
                        // Fall through.

                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        if (lowerBoundBelow) {
                            lowerIdxCondition.set(i, cond);
                            predicateMasks[idxInCollation] |= GREATER_MASK;
                        }
                        else {
                            upperIdxCondition.set(i, cond);
                            predicateMasks[idxInCollation] |= LESS_MASK;
                        }
                        break;

                    default:
                        throw new AssertionError("Unknown condition: " + cond);
                }
            }
        }
    }

    @NotNull private Map<Integer, List<RexCall>> mapPredicatesToFields() {
        List<RexNode> predicatesConjunction = RelOptUtil.conjunctions(condition);

        Map<Integer, List<RexCall>> fieldsToPredicates = new HashMap<>(predicatesConjunction.size());

        for (RexNode rexNode : predicatesConjunction) {
            if (!isBinaryComparison(rexNode))
                continue;

            RexCall predCall = (RexCall)rexNode;
            RexLocalRef inputRef = (RexLocalRef)extractOperand(predCall, true);

            if (inputRef == null)
                continue;

            int constraintFldIdx = inputRef.getIndex();

            List<RexCall> fldPreds = fieldsToPredicates
                .computeIfAbsent(constraintFldIdx, k -> new ArrayList<>(predicatesConjunction.size()));

            // Let RexLocalRef be on the left side.
            if (refOnTheRight(predCall))
                predCall = (RexCall)RexUtil.invert(getCluster().getRexBuilder(), predCall);

            fldPreds.add(predCall);
        }
        return fieldsToPredicates;
    }

    private boolean boundsArePossible() {
        if (condition == null)
            return false;


        if (RelOptUtil.disjunctions(condition).size() > 1)
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
        if (leftOp instanceof RexLocalRef && (rightOp instanceof RexLiteral || rightOp instanceof RexDynamicParam))
            return inputRef ? leftOp : rightOp;
        else if ((leftOp instanceof RexLiteral || leftOp instanceof RexDynamicParam) && rightOp instanceof RexLocalRef)
            return inputRef ? rightOp : leftOp;

        return null;
    }

    private static boolean refOnTheRight(RexCall predCall) {
        RexNode rightOp = predCall.getOperands().get(1);

        rightOp = removeCast(rightOp);

        return rightOp.isA(SqlKind.LOCAL_REF);
    }

    public String indexName() {
        return idxName;
    }

    public List<RexNode> lowerIndexCondition() {
        return lowerIdxCondition;
    }

    public List<RexNode>  upperIndexCondition() {
        return upperIdxCondition;
    }

    private static boolean allNulls(List arr) {
        for (int i = 0; i < arr.size(); i++) {
            if (arr.get(i) != null)
                return false;
        }
        return true;
    }


    private List<RexNode> makeListOfNullLiterals(int size) {
        List<RexNode> list = new ArrayList<>(size);
        RexNode nullLiteral = getCluster().getRexBuilder()
            .makeNullLiteral(getCluster().getTypeFactory().createJavaType(Object.class));
        for (int i = 0; i < size; i++) {
            list.add(nullLiteral);
        }
        return list;
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        return pw.item("index", idxName )
            .item("lower", lowerIdxCondition) // TODO all nulls handling?
            .item("upper", upperIdxCondition)
            .itemIf("filters", condition, condition != null)
            .item("collation", collation);
    }

    /** {@inheritDoc} */
    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert inputs.isEmpty();

        return this;
    }

    /** {@inheritDoc} */
    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     *
     */
    public RexNode condition() {
        return condition;
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

    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = estimateRowCount(mq);

        RelOptCost cost = planner.getCostFactory().makeCost(rows, 0, 0);

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

        if (condition != null) {
            Double selectivity = mq.getSelectivity(this, condition);
            rows *= selectivity;
        }

        return rows;
    }
}
