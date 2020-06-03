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
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexLocalRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.ignite.internal.processors.query.calcite.schema.IgniteTable;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.rex.RexUtil.removeCast;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN;
import static org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.LESS_THAN;
import static org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL;
import static org.apache.calcite.sql.SqlKind.OR;

/**
 * Relational operator that returns the contents of a table.
 */
public class IgniteTableScan extends TableScan implements IgniteRel {
    /** Supported index operations. */
    public static final Set<SqlKind> TREE_INDEX_COMPARISON =
        EnumSet.of(
            EQUALS,
            LESS_THAN, GREATER_THAN,
            GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL);

    /** */
    private final String idxName;

    /** */
    private final RexNode cond;

    /** */
    private final IgniteTable igniteTbl;

    /** */
    private final RelCollation collation;

    /** */
    private final List<RexNode> lowerIdxCond;

    /** */
    private final List<RexNode> upperIdxCond;

    /** */
    private double idxSelectivity = 1.0;

    /**
     * Constructor used for deserialization.
     *
     * @param input Serialized representation.
     */
    public IgniteTableScan(RelInput input) {
        super(Commons.changeTraits(input, IgniteConvention.INSTANCE));
        idxName = input.getString("index");
        cond = input.getExpression("filters");
        lowerIdxCond = input.getExpressionList("lower");
        upperIdxCond = input.getExpressionList("upper");
        igniteTbl = getTable().unwrap(IgniteTable.class);
        collation = igniteTbl.getIndex(idxName).collation();
    }

    /**
     * Creates a TableScan.
     * @param cluster Cluster that this relational expression belongs to
     * @param traits Traits of this relational expression
     * @param tbl Table definition.
     * @param idxName Index name.
     * @param cond Filters for scan.
     */
    public IgniteTableScan(
        RelOptCluster cluster,
        RelTraitSet traits,
        RelOptTable tbl,
        String idxName,
        @Nullable RexNode cond
    ) {
        super(cluster, traits, ImmutableList.of(), tbl);

        this.idxName = idxName;
        this.cond = cond;
        igniteTbl = tbl.unwrap(IgniteTable.class);
        RelCollation coll = Commons.collation(traits);
        collation = coll == null ? RelCollationTraitDef.INSTANCE.getDefault() : coll;
        lowerIdxCond = new ArrayList<>(getRowType().getFieldCount());
        upperIdxCond = new ArrayList<>(getRowType().getFieldCount());
        buildIndexConditions();
    }

    /**
     * Builds index conditions.
     */
    private void buildIndexConditions() {
        if (!boundsArePossible())
            return;

        assert cond != null;

        Map<Integer, List<RexCall>> fieldsToPredicates = mapPredicatesToFields();

        double selectivity = 1.0;

        for (int i = 0; i < collation.getFieldCollations().size(); i++) {
            RelFieldCollation fc = collation.getFieldCollations().get(i);

            int collFldIdx = fc.getFieldIndex();

            List<RexCall> collFldPreds = fieldsToPredicates.get(collFldIdx);

            if (F.isEmpty(collFldPreds))
                break;

            boolean lowerBoundBelow = !fc.getDirection().isDescending();

            RexNode bestUpper = null;
            RexNode bestLower = null;

            for (RexCall pred : collFldPreds) {
                RexNode cond = removeCast(pred.operands.get(1));

                assert cond instanceof RexLiteral || cond instanceof RexDynamicParam : cond;

                SqlOperator op = pred.getOperator();
                switch (op.kind) {
                    case EQUALS:
                        bestUpper = pred;
                        bestLower = pred;
                        break;

                    case LESS_THAN:
                    case LESS_THAN_OR_EQUAL:
                        lowerBoundBelow = !lowerBoundBelow;
                        // Fall through.

                    case GREATER_THAN:
                    case GREATER_THAN_OR_EQUAL:
                        if (lowerBoundBelow)
                            bestLower = pred;
                        else
                            bestUpper = pred;
                        break;

                    default:
                        throw new AssertionError("Unknown condition: " + cond);
                }

                if (bestUpper != null && bestLower != null)
                    break; // We've found either "=" condition or both lower and upper.
            }

            if (bestLower == null && bestUpper == null)
                break; // No bounds, so break the loop.

            if (i > 0 && bestLower != bestUpper)
                break; // Go behind the first index field only in the case of multiple "=" conditions on index fields.

            if (bestLower == bestUpper) { // "x=10"
                upperIdxCond.add(bestUpper);
                lowerIdxCond.add(bestLower);
                selectivity *= 0.1;
            }
            else if (bestLower != null && bestUpper != null) { // "x>5 AND x<10"
                upperIdxCond.add(bestUpper);
                lowerIdxCond.add(bestLower);
                selectivity *= 0.25;
                break;
            }
            else if (bestLower != null) { // "x>5"
                lowerIdxCond.add(bestLower);
                selectivity *= 0.35;
                break;
            }
            else { // "x<10"
                upperIdxCond.add(bestUpper);
                selectivity *= 0.35;
                break;
            }
        }
        idxSelectivity = selectivity;
    }

    /** */
    private Map<Integer, List<RexCall>> mapPredicatesToFields() {
        List<RexNode> predicatesConjunction = RelOptUtil.conjunctions(cond);

        Map<Integer, List<RexCall>> fieldsToPredicates = new HashMap<>(predicatesConjunction.size());

        for (RexNode rexNode : predicatesConjunction) {
            if (!isBinaryComparison(rexNode))
                continue;

            RexCall predCall = (RexCall)rexNode;
            RexLocalRef ref = (RexLocalRef)extractRef(predCall);

            if (ref == null)
                continue;

            int constraintFldIdx = ref.getIndex();

            List<RexCall> fldPreds = fieldsToPredicates
                .computeIfAbsent(constraintFldIdx, k -> new ArrayList<>(predicatesConjunction.size()));

            // Let RexLocalRef be on the left side.
            if (refOnTheRight(predCall))
                predCall = (RexCall)RexUtil.invert(getCluster().getRexBuilder(), predCall);

            fldPreds.add(predCall);
        }
        return fieldsToPredicates;
    }

    /** */
    private boolean boundsArePossible() {
        if (cond == null)
            return false;

        RexCall dnf = ((RexCall)RexUtil.toDnf(getCluster().getRexBuilder(), cond));

        if (dnf.isA(OR) && dnf.getOperands().size() > 1) // OR conditions are not supported yet.
            return false;

        if (igniteTbl.collations().isEmpty())
            return false;

        if (igniteTbl.collations().size() > 1) {
            throw new UnsupportedOperationException("At most one table collation is currently supported: " +
                "[collations=" + igniteTbl.collations() + ", table=" + igniteTbl + ']');
        }
        return true;
    }

    /** */
    private static RexNode extractRef(RexCall call) {
        assert isBinaryComparison(call);

        RexNode leftOp = call.getOperands().get(0);
        RexNode rightOp = call.getOperands().get(1);

        leftOp = removeCast(leftOp);
        rightOp = removeCast(rightOp);

        if (leftOp instanceof RexLocalRef && (rightOp instanceof RexLiteral || rightOp instanceof RexDynamicParam))
            return leftOp;
        else if ((leftOp instanceof RexLiteral || leftOp instanceof RexDynamicParam) && rightOp instanceof RexLocalRef)
            return rightOp;

        return null;
    }

    /** */
    private static boolean refOnTheRight(RexCall predCall) {
        RexNode rightOp = predCall.getOperands().get(1);

        rightOp = removeCast(rightOp);

        return rightOp.isA(SqlKind.LOCAL_REF);
    }

    /** */
    public String indexName() {
        return idxName;
    }

    /** */
    public List<RexNode> lowerIndexCondition() {
        if (F.isEmpty(lowerIdxCond))
            return null;

        return buildIndexCondition(lowerIdxCond);
    }

    /** */
    public List<RexNode> upperIndexCondition() {
        if (F.isEmpty(upperIdxCond))
            return null;

        return buildIndexCondition(upperIdxCond);
    }

    /** */
    public List<RexNode> buildIndexCondition(Iterable<RexNode> idxCond) {
        List<RexNode> lowerIdxCond = makeListOfNullLiterals(rowType.getFieldCount());

        for (RexNode pred : idxCond) {
            assert pred instanceof RexCall;

            RexCall call = (RexCall)pred;
            RexLocalRef ref = (RexLocalRef)removeCast(call.operands.get(0));
            RexNode cond = removeCast(call.operands.get(1));

            assert cond instanceof RexLiteral || cond instanceof RexDynamicParam : cond;

            lowerIdxCond.set(ref.getIndex(), cond);
        }

        return lowerIdxCond;
    }

    /** */
    private List<RexNode> makeListOfNullLiterals(int size) {
        List<RexNode> list = new ArrayList<>(size);
        RexNode nullLiteral = getCluster().getRexBuilder()
            .makeNullLiteral(getCluster().getTypeFactory().createJavaType(Object.class));
        for (int i = 0; i < size; i++) {
            list.add(nullLiteral);
        }
        return list;
    }

    /** {@inheritDoc} */
    @Override public RelWriter explainTerms(RelWriter pw) {
        super.explainTerms(pw);
        return pw.item("index", idxName )
            .itemIf("lower", lowerIdxCond, lowerIdxCond != null)
            .itemIf("upper", upperIdxCond, upperIdxCond != null)
            .itemIf("filters", cond, cond != null)
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

    /** */
    public RexNode condition() {
        return cond;
    }

    /** */
    public IgniteTable igniteTable() {
        return igniteTbl;
    }

    /** */
    private static boolean isBinaryComparison(RexNode exp) {
        return TREE_INDEX_COMPARISON.contains(exp.getKind()) &&
            (exp instanceof RexCall) &&
            ((RexCall)exp).getOperands().size() == 2;
    }

    /** {@inheritDoc} */
    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = estimateRowCount(mq);

        return planner.getCostFactory().makeCost(rows, 0, 0);
    }

    /** {@inheritDoc} */
    @Override public double estimateRowCount(RelMetadataQuery mq) {
        double rows = table.getRowCount();

        double rowsIn = rows * idxSelectivity;
        double rowsOut = rowsIn;

        if (cond != null) {
            Double sel = mq.getSelectivity(this, cond);
            rowsOut *= sel;
        }

        return rowsIn + rowsOut;
    }
}
