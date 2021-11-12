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

import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.rules.AggregateMergeRule;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.rules.FilterJoinRule.FilterIntoJoinRule;
import org.apache.calcite.rel.rules.FilterJoinRule.JoinConditionPushRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.ignite.internal.processors.query.calcite.rule.CorrelateToNestedLoopRule;
import org.apache.ignite.internal.processors.query.calcite.rule.CorrelatedNestedLoopJoinRule;
import org.apache.ignite.internal.processors.query.calcite.rule.FilterConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.FilterSpoolMergeToHashIndexSpoolRule;
import org.apache.ignite.internal.processors.query.calcite.rule.FilterSpoolMergeToSortedIndexSpoolRule;
import org.apache.ignite.internal.processors.query.calcite.rule.HashAggregateConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.LogicalScanConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.MergeJoinConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.NestedLoopJoinConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.ProjectConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.SetOpConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.SortAggregateConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.SortConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.TableFunctionScanConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.TableModifyConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.UnionConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.ValuesConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.ExposeIndexRule;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.FilterScanMergeRule;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.LogicalOrToUnionRule;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.ProjectScanMergeRule;
import org.apache.ignite.internal.processors.query.calcite.rule.patch.AggregateExpandDistinctAggregatesRule;

import static org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePrograms.cbo;
import static org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePrograms.hep;

/**
 * Represents a planner phase with its description and a used rule set.
 */
public enum PlannerPhase {
    /** */
    HEP_DECORRELATE("Heuristic phase to decorrelate subqueries") {
        /** {@inheritDoc} */
        @Override public RuleSet getRules(PlanningContext ctx) {
            return RuleSets.ofList(
                CoreRules.FILTER_SUB_QUERY_TO_CORRELATE,
                CoreRules.PROJECT_SUB_QUERY_TO_CORRELATE,
                CoreRules.JOIN_SUB_QUERY_TO_CORRELATE
            );
        }

        /** {@inheritDoc} */
        @Override public Program getProgram(PlanningContext ctx) {
            return hep(getRules(ctx));
        }
    },

    /** */
    HEP_FILTER_PUSH_DOWN("Heuristic phase to push down filters") {
        /** {@inheritDoc} */
        @Override public RuleSet getRules(PlanningContext ctx) {
            return RuleSets.ofList(
                CoreRules.FILTER_MERGE,
                CoreRules.FILTER_AGGREGATE_TRANSPOSE,
                CoreRules.FILTER_SET_OP_TRANSPOSE,
                CoreRules.JOIN_CONDITION_PUSH,
                CoreRules.FILTER_INTO_JOIN,
                CoreRules.FILTER_PROJECT_TRANSPOSE
            );
        }

        /** {@inheritDoc} */
        @Override public Program getProgram(PlanningContext ctx) {
            return hep(getRules(ctx));
        }
    },

    /** */
    HEP_PROJECT_PUSH_DOWN("Heuristic phase to push down and merge projects") {
        /** {@inheritDoc} */
        @Override public RuleSet getRules(PlanningContext ctx) {
            return RuleSets.ofList(
                ProjectScanMergeRule.TABLE_SCAN_SKIP_CORRELATED,
                ProjectScanMergeRule.INDEX_SCAN_SKIP_CORRELATED,

                CoreRules.JOIN_PUSH_EXPRESSIONS,
                CoreRules.PROJECT_MERGE,
                CoreRules.PROJECT_REMOVE,
                CoreRules.PROJECT_FILTER_TRANSPOSE
            );
        }

        /** {@inheritDoc} */
        @Override public Program getProgram(PlanningContext ctx) {
            return hep(getRules(ctx));
        }
    },

    /** */
    OPTIMIZATION("Main optimization phase") {
        /** {@inheritDoc} */
        @Override public RuleSet getRules(PlanningContext ctx) {
            return ctx.rules(
                RuleSets.ofList(
                    FilterMergeRule.Config.DEFAULT
                        .withOperandFor(LogicalFilter.class).toRule(),

                    JoinPushThroughJoinRule.Config.LEFT
                        .withOperandFor(LogicalJoin.class).toRule(),

                    JoinPushThroughJoinRule.Config.RIGHT
                        .withOperandFor(LogicalJoin.class).toRule(),

                    JoinPushExpressionsRule.Config.DEFAULT
                        .withOperandFor(LogicalJoin.class).toRule(),

                    JoinConditionPushRule.Config.DEFAULT
                        .withOperandSupplier(b -> b.operand(LogicalJoin.class)
                            .anyInputs()).toRule(),

                    FilterIntoJoinRule.Config.DEFAULT
                        .withOperandSupplier(b0 ->
                            b0.operand(LogicalFilter.class).oneInput(b1 ->
                                b1.operand(LogicalJoin.class).anyInputs())).toRule(),

                    FilterProjectTransposeRule.Config.DEFAULT
                        .withOperandFor(LogicalFilter.class, f -> true, LogicalProject.class, p -> true).toRule(),

                    ProjectFilterTransposeRule.Config.DEFAULT
                        .withOperandFor(LogicalProject.class, LogicalFilter.class).toRule(),

                    ProjectMergeRule.Config.DEFAULT
                        .withOperandFor(LogicalProject.class).toRule(),

                    ProjectRemoveRule.Config.DEFAULT
                        .withOperandSupplier(b ->
                            b.operand(LogicalProject.class)
                                .predicate(ProjectRemoveRule::isTrivial)
                                .anyInputs()).toRule(),

                    AggregateMergeRule.Config.DEFAULT
                        .withOperandSupplier(b0 ->
                            b0.operand(LogicalAggregate.class)
                                .oneInput(b1 ->
                                    b1.operand(LogicalAggregate.class)
                                        .predicate(Aggregate::isSimple)
                                        .anyInputs())).toRule(),

                    AggregateExpandDistinctAggregatesRule.Config.JOIN.toRule(),

                    SortRemoveRule.Config.DEFAULT
                        .withOperandSupplier(b ->
                            b.operand(LogicalSort.class)
                                .anyInputs()).toRule(),

                    CoreRules.UNION_MERGE,
                    CoreRules.MINUS_MERGE,
                    CoreRules.INTERSECT_MERGE,
                    CoreRules.UNION_REMOVE,
                    CoreRules.JOIN_COMMUTE,
                    CoreRules.AGGREGATE_REMOVE,
                    CoreRules.JOIN_COMMUTE_OUTER,

                    // Useful of this rule is not clear now.
                    // CoreRules.AGGREGATE_REDUCE_FUNCTIONS,

                    PruneEmptyRules.SortFetchZeroRuleConfig.EMPTY
                        .withOperandSupplier(b ->
                            b.operand(LogicalSort.class).anyInputs())
                        .withDescription("PruneSortLimit0")
                        .as(PruneEmptyRules.SortFetchZeroRuleConfig.class)
                        .toRule(),

                    ExposeIndexRule.INSTANCE,
                    ProjectScanMergeRule.TABLE_SCAN,
                    ProjectScanMergeRule.INDEX_SCAN,
                    FilterScanMergeRule.TABLE_SCAN,
                    FilterScanMergeRule.INDEX_SCAN,
                    FilterSpoolMergeToSortedIndexSpoolRule.INSTANCE,
                    FilterSpoolMergeToHashIndexSpoolRule.INSTANCE,

                    LogicalOrToUnionRule.INSTANCE,

                    CorrelatedNestedLoopJoinRule.INSTANCE,
                    CorrelateToNestedLoopRule.INSTANCE,
                    NestedLoopJoinConverterRule.INSTANCE,
                    MergeJoinConverterRule.INSTANCE,

                    ValuesConverterRule.INSTANCE,
                    LogicalScanConverterRule.INDEX_SCAN,
                    LogicalScanConverterRule.TABLE_SCAN,
                    HashAggregateConverterRule.SINGLE,
                    HashAggregateConverterRule.MAP_REDUCE,
                    SortAggregateConverterRule.SINGLE,
                    SortAggregateConverterRule.MAP_REDUCE,
                    SetOpConverterRule.SINGLE_MINUS,
                    SetOpConverterRule.MAP_REDUCE_MINUS,
                    SetOpConverterRule.SINGLE_INTERSECT,
                    SetOpConverterRule.MAP_REDUCE_INTERSECT,
                    ProjectConverterRule.INSTANCE,
                    FilterConverterRule.INSTANCE,
                    TableModifyConverterRule.INSTANCE,
                    UnionConverterRule.INSTANCE,
                    SortConverterRule.INSTANCE,
                    TableFunctionScanConverterRule.INSTANCE
                )
            );
        }

        /** {@inheritDoc} */
        @Override public Program getProgram(PlanningContext ctx) {
            return cbo(getRules(ctx));
        }
    };

    /** */
    public final String description;

    /**
     * @param description Phase description.
     */
    PlannerPhase(String description) {
        this.description = description;
    }

    /**
     * Returns rule set, calculated on the basis of query, planner context and planner phase.
     *
     * @param ctx Planner context.
     * @return Rule set.
     */
    public abstract RuleSet getRules(PlanningContext ctx);

    /**
     * Returns a program, calculated on the basis of query, planner context planner phase and rules set.
     *
     * @param ctx Planner context.
     * @return Rule set.
     */
    public abstract Program getProgram(PlanningContext ctx);
}
