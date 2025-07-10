/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rule.logical;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Litmus;
import org.apache.ignite.lang.IgniteBiTuple;
import org.immutables.value.Value;

import static java.util.Objects.requireNonNull;

/**
 * Transform that converts IN, EXISTS and scalar sub-queries into joins.
 *
 * <p>Sub-queries are represented by {@link RexSubQuery} expressions.
 *
 * <p>A sub-query may or may not be correlated. If a sub-query is correlated,
 * the wrapped {@link RelNode} will contain a {@link RexCorrelVariable} before
 * the rewrite, and the product of the rewrite will be a {@link Correlate}.
 * The Correlate can be removed using {@link RelDecorrelator}.
 *
 * @see CoreRules#FILTER_SUB_QUERY_TO_CORRELATE
 * @see CoreRules#PROJECT_SUB_QUERY_TO_CORRELATE
 * @see CoreRules#JOIN_SUB_QUERY_TO_CORRELATE
 *
 * TODO Revise fixing or removing of this rule after https://issues.apache.org/jira/browse/CALCITE-7034
 */
@Value.Enclosing
public class IgniteSubQueryRemoveRule extends SubQueryRemoveRule {
    /** */
    public static final RelOptRule FILTER = Config.FILTER.toRule();

    /** */
    private final Config cfg;

    /** Creates a SubQueryRemoveRule. */
    protected IgniteSubQueryRemoveRule(Config cfg) {
        super(SubQueryRemoveRule.Config.FILTER);

        this.cfg = cfg;

        requireNonNull(cfg.matchHandler());
    }

    /** {@inheritDoc} */
    @Override public void onMatch(RelOptRuleCall call) {
        cfg.matchHandler().accept(this, call);
    }

    /**
     * Returns a list of expressions that project the first {@code fieldCount} fields of the top input
     * on a {@link RelBuilder}'s stack.
     */
    private static List<RexNode> fields(RelBuilder builder, int fieldCnt) {
        List<RexNode> projects = new ArrayList<>();

        for (int i = 0; i < fieldCnt; i++)
            projects.add(builder.field(i));

        return projects;
    }

    /**
     * Also may temporarily replace index of a correlated column.
     *
     * @see Correlate#isValid(Litmus, RelNode.Context)
     */
    private static void matchFilter(IgniteSubQueryRemoveRule rule, RelOptRuleCall call) {
        Filter filter = call.rel(0);
        RelBuilder builder = call.builder();

        builder.push(filter.getInput());

        int cnt = 0;

        RexNode condition = filter.getCondition();

        Collection<IgniteBiTuple<RexNode, RexFieldAccess>> replacedInputRefs = new ArrayList<>();

        while (true) {
            final RexSubQuery subQry = RexUtil.SubQueryFinder.find(condition);

            if (subQry == null) {
                assert cnt > 0;
                break;
            }

            ++cnt;

            RelOptUtil.Logic logic = LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(condition), subQry);
            Set<CorrelationId> variablesSet = RelOptUtil.getVariablesUsed(subQry.rel);

            RexSubQuery subQryReplaced = subQry;

            if (!variablesSet.isEmpty()
                && (subQry.getKind() == SqlKind.IN || subQry.getKind() == SqlKind.SOME || subQry.getKind() == SqlKind.EXISTS)) {
                CorrelationId id = Iterables.getOnlyElement(variablesSet);
                RexBuilder rexBuilder = filter.getCluster().getRexBuilder();

                // Tries to replace correlated colums index taking in account current row type to pass
                // the required field check when replacing to a join.
                subQryReplaced = subQry.clone(subQryReplaced.rel.accept(new RelHomogeneousShuttle() {
                    private final RexShuttle rexShuttle = new RexShuttle() {
                        @Override public RexNode visitFieldAccess(RexFieldAccess field) {
                            if (!(field.getReferenceExpr() instanceof RexCorrelVariable)
                                || !((RexCorrelVariable)field.getReferenceExpr()).id.equals(id))
                                return super.visitFieldAccess(field);

                            int oldIdx = field.getField().getIndex();

                            // This is not correct sometimes. A correlated column might refer to other preceding inputs.
                            // And may have a bigger index than current left shoulder row size of the join being created.
                            // We fix this index temporarily to pass the conversion.
                            if (oldIdx < filter.getRowType().getFieldCount())
                                return super.visitFieldAccess(field);

                            RexNode newCorr = rexBuilder.makeCorrel(filter.getRowType(), id);

                            RexNode fix = rexBuilder.makeFieldAccess(newCorr, 0);

                            assert replacedInputRefs.stream().noneMatch(tpl -> tpl.get2() == fix);

                            replacedInputRefs.add(new IgniteBiTuple<>(fix, field));

                            return fix;
                        }
                    };

                    @Override public RelNode visit(RelNode other) {
                        return super.visit(other).accept(rexShuttle);
                    }
                }));
            }

            // Filter without variables could be handled before this change, we do not want
            // to break it yet for compatibility reason.
            if (!filter.getVariablesSet().isEmpty()) {
                // Only consider the correlated variables which originated from this sub-query level.
                variablesSet.retainAll(filter.getVariablesSet());
            }

            RexNode target = rule.apply(subQryReplaced, variablesSet, logic, builder, 1, builder.peek().getRowType().getFieldCount(), cnt);
            RexShuttle shuttle = new ReplaceSubQueryShuttle(subQry, target);

            condition = condition.accept(shuttle);
        }

        builder.filter(condition);
        builder.project(fields(builder, filter.getRowType().getFieldCount()));

        RelNode result = builder.build();

        // Restore original field refs.
        if (!replacedInputRefs.isEmpty()) {
            result = result.accept(new RelHomogeneousShuttle() {
                private final RexShuttle rexShuttle = new RexShuttle() {
                    @Override public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
                        Iterator<IgniteBiTuple<RexNode, RexFieldAccess>> it = replacedInputRefs.iterator();

                        while (it.hasNext()) {
                            IgniteBiTuple<RexNode, RexFieldAccess> rw = it.next();

                            if (rw.get1() == fieldAccess) {
                                it.remove();

                                return super.visitFieldAccess(rw.get2());
                            }
                        }

                        return super.visitFieldAccess(fieldAccess);
                    }
                };

                @Override public RelNode visit(LogicalFilter filter) {
                    RexNode newC = rexShuttle.apply(filter.getCondition());

                    if (newC != filter.getCondition())
                        return call.builder().push(visit(filter.getInput())).filter(newC).build();

                    return super.visit(filter);
                }
            });
        }

        assert replacedInputRefs.isEmpty();

        call.transformTo(result);
    }

    /** Shuttle that replaces occurrences of a given {@link org.apache.calcite.rex.RexSubQuery} with a replacement expression. */
    private static class ReplaceSubQueryShuttle extends RexShuttle {
        /** */
        private final RexSubQuery subQry;

        /** */
        private final RexNode replacement;

        /** */
        private ReplaceSubQueryShuttle(RexSubQuery subQry, RexNode replacement) {
            this.subQry = subQry;
            this.replacement = replacement;
        }

        /** {@inheritDoc} */
        @Override public RexNode visitSubQuery(RexSubQuery subQry) {
            return subQry.equals(this.subQry) ? replacement : subQry;
        }
    }

    /** Rule configuration. */
    @Value.Immutable(singleton = false)
    public interface Config extends RelRule.Config {
        /** */
        Config FILTER = ImmutableIgniteSubQueryRemoveRule.Config.builder()
            .withMatchHandler(IgniteSubQueryRemoveRule::matchFilter)
            .build()
            .withOperandSupplier(b ->
                b.operand(Filter.class)
                    .predicate(RexUtil.SubQueryFinder::containsSubQuery).anyInputs())
            .withDescription("SubQueryRemoveRule:Filter");

        /** {@inheritDoc} */
        @Override default IgniteSubQueryRemoveRule toRule() {
            return new IgniteSubQueryRemoveRule(this);
        }

        /** Forwards a call to {@link #onMatch(RelOptRuleCall)}. */
        MatchHandler<IgniteSubQueryRemoveRule> matchHandler();

        /** Sets {@link #matchHandler()}. */
        Config withMatchHandler(MatchHandler<IgniteSubQueryRemoveRule> matchHandler);
    }
}

