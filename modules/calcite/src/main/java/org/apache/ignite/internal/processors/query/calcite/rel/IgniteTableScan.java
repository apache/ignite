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

package org.apache.ignite.internal.processors.query.calcite.rel;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
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
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.processors.query.calcite.util.Commons.isBinaryComparison;

/**
 *
 */
public class IgniteTableScan extends TableScan implements IgniteRel {
    private final List<RexNode> filters;
    private final ImmutableIntList projects;
    private final RexNode indexPredicate;
    private final List<RexNode> complementaryPredicates;


    public IgniteTableScan(RelOptCluster cluster,
        RelTraitSet traitSet,
        RelOptTable table,
        @Nullable List<RexNode> filters,
        @Nullable ImmutableIntList projects) {
        super(cluster, traitSet, table);

        // TODO check if mapping is trivial


        this.filters = filters;
        this.projects = projects;

        if (!F.isEmpty(filters)) {
            // TODO multicolumn indexes support
            // TODO Merge OR filters result using several index cursors
            // TODO simplify and merge overlapping conditions

            // TODO do we always scan over index?
            int firstIdxCol = getCollationList().get(0).getFieldCollations().get(0).getFieldIndex();
            double bestSelectivity = Double.MAX_VALUE;
            RexCall bestPredicate = null;

            List<RexNode> predicates = RexUtil.flattenAnd(filters);
            complementaryPredicates = new ArrayList<>(predicates.size());

            for (RexNode exp : predicates) {
                if (!isBinaryComparison(exp)) {
                    complementaryPredicates.add(exp);

                    continue;
                }

                RexCall call = (RexCall)exp;

                RexNode leftOp = call.getOperands().get(0);
                RexNode rightOp = call.getOperands().get(1);

                if (leftOp.isA(SqlKind.CAST))
                    leftOp = ((RexCall)leftOp).getOperands().get(0);

                if (rightOp.isA(SqlKind.CAST))
                    rightOp = ((RexCall)rightOp).getOperands().get(0);

                int colIdx;

                // TODO handle correlVariable as constant?
                if (leftOp instanceof RexInputRef  && (rightOp instanceof RexLiteral || rightOp instanceof RexDynamicParam))
                    colIdx = ((RexInputRef)leftOp).getIndex();
                else if ((leftOp instanceof RexLiteral || leftOp instanceof RexDynamicParam)  && rightOp instanceof RexInputRef)
                    colIdx = ((RexInputRef)rightOp).getIndex();
                else {
                    complementaryPredicates.add(exp);

                    continue;
                }

                if (firstIdxCol == colIdx) { // Congrats! We have an indexed column in the predicate.
                    double curSelectivity = guessSelectivity(call);
                    if (bestPredicate == null || curSelectivity < bestSelectivity) {
                        if (bestPredicate != null)
                            complementaryPredicates.add(bestPredicate); // Move previous best to the complementary.

                        bestPredicate = call;
                        bestSelectivity = curSelectivity;
                    }
                    else {
                        complementaryPredicates.add(bestPredicate);
                    }
                }
                else {
                    complementaryPredicates.add(exp);
                }
            }

            indexPredicate = bestPredicate;
        }
        else {
            indexPredicate = null;
            complementaryPredicates = null;
        }
    }


    @Override public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double rows = table.getRowCount();
        double cpu = rows;

        if (!F.isEmpty(filters)) {
            Double selectivity = mq.getSelectivity(this, filters.get(0)); // TODO handle multiple items in filter.
            rows *= selectivity;

            if (indexPredicate != null) {
                Double idxPredSelectivity = mq.getSelectivity(this, indexPredicate);

                cpu *=  idxPredSelectivity;
                rows *= idxPredSelectivity;
            }
        }

        RelOptCost cost = planner.getCostFactory().makeCost(rows, cpu, 0);

        // TODO count projects.
        return cost;
    }


    @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return this;
    }

    @Override public <T> T accept(IgniteRelVisitor<T> visitor) {
        return visitor.visit(this);
    }

    public List<RexNode> filters() {
        return filters;
    }

    public ImmutableIntList projects() {
        return projects;
    }

    @Override public RelWriter explainTerms(RelWriter pw) {
        return super.explainTerms(pw)
            .item("indexPredicate", indexPredicate)
            .item("additionalPredicate", complementaryPredicates)
            .item("projects", projects);
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


    private static double guessSelectivity(RexCall call) {
        switch (call.getKind()) {
            case EQUALS:
                return 0.1;

            case LESS_THAN:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
            case LESS_THAN_OR_EQUAL:
                return 0.3;

            default:
                throw new AssertionError("Wrong argument type: " + call);
        }
    }
}
