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

package org.apache.ignite.internal.processors.query.calcite.rule;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.volcano.AbstractConverter;
import org.apache.calcite.rel.rules.AbstractMaterializedViewRule;
import org.apache.calcite.rel.rules.AggregateExpandDistinctAggregatesRule;
import org.apache.calcite.rel.rules.AggregateJoinTransposeRule;
import org.apache.calcite.rel.rules.AggregateMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectMergeRule;
import org.apache.calcite.rel.rules.AggregateProjectPullUpConstantsRule;
import org.apache.calcite.rel.rules.AggregateReduceFunctionsRule;
import org.apache.calcite.rel.rules.AggregateRemoveRule;
import org.apache.calcite.rel.rules.AggregateStarTableRule;
import org.apache.calcite.rel.rules.AggregateValuesRule;
import org.apache.calcite.rel.rules.CalcRemoveRule;
import org.apache.calcite.rel.rules.DateRangeRules;
import org.apache.calcite.rel.rules.ExchangeRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.FilterAggregateTransposeRule;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterTableScanRule;
import org.apache.calcite.rel.rules.IntersectToDistinctRule;
import org.apache.calcite.rel.rules.JoinCommuteRule;
import org.apache.calcite.rel.rules.JoinPushExpressionsRule;
import org.apache.calcite.rel.rules.JoinPushThroughJoinRule;
import org.apache.calcite.rel.rules.MaterializedViewFilterScanRule;
import org.apache.calcite.rel.rules.ProjectFilterTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.rules.ProjectToWindowRule;
import org.apache.calcite.rel.rules.ProjectWindowTransposeRule;
import org.apache.calcite.rel.rules.PruneEmptyRules;
import org.apache.calcite.rel.rules.ReduceExpressionsRule;
import org.apache.calcite.rel.rules.SemiJoinRule;
import org.apache.calcite.rel.rules.SortJoinTransposeRule;
import org.apache.calcite.rel.rules.SortProjectTransposeRule;
import org.apache.calcite.rel.rules.SortRemoveConstantKeysRule;
import org.apache.calcite.rel.rules.SortRemoveRule;
import org.apache.calcite.rel.rules.SortUnionTransposeRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.rules.TableScanRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.rel.rules.UnionPullUpConstantsRule;
import org.apache.calcite.rel.rules.UnionToDistinctRule;
import org.apache.calcite.rel.rules.ValuesReduceRule;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.IgniteFilterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.IgniteJoinRule;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.IgniteProjectRule;

/**
 *
 */
public class IgniteRules {
    public static final List<RelOptRule> BASE_RULES = ImmutableList.of(
        AggregateStarTableRule.INSTANCE,
        AggregateStarTableRule.INSTANCE2,
        TableScanRule.INSTANCE,
        ProjectMergeRule.INSTANCE,
        FilterTableScanRule.INSTANCE,
        ProjectFilterTransposeRule.INSTANCE,
        FilterProjectTransposeRule.INSTANCE,
        FilterJoinRule.FILTER_ON_JOIN,
        JoinPushExpressionsRule.INSTANCE,
        AggregateExpandDistinctAggregatesRule.INSTANCE,
        AggregateReduceFunctionsRule.INSTANCE,
        FilterAggregateTransposeRule.INSTANCE,
        ProjectWindowTransposeRule.INSTANCE,
        JoinCommuteRule.INSTANCE,
        JoinPushThroughJoinRule.RIGHT,
        JoinPushThroughJoinRule.LEFT,
        SortProjectTransposeRule.INSTANCE,
        SortJoinTransposeRule.INSTANCE,
        SortRemoveConstantKeysRule.INSTANCE,
        SortUnionTransposeRule.INSTANCE,
        ExchangeRemoveConstantKeysRule.EXCHANGE_INSTANCE,
        ExchangeRemoveConstantKeysRule.SORT_EXCHANGE_INSTANCE);

    public static final List<RelOptRule> ABSTRACT_RULES = ImmutableList.of(
        AggregateProjectPullUpConstantsRule.INSTANCE2,
        UnionPullUpConstantsRule.INSTANCE,
        PruneEmptyRules.UNION_INSTANCE,
        PruneEmptyRules.INTERSECT_INSTANCE,
        PruneEmptyRules.MINUS_INSTANCE,
        PruneEmptyRules.PROJECT_INSTANCE,
        PruneEmptyRules.FILTER_INSTANCE,
        PruneEmptyRules.SORT_INSTANCE,
        PruneEmptyRules.AGGREGATE_INSTANCE,
        PruneEmptyRules.JOIN_LEFT_INSTANCE,
        PruneEmptyRules.JOIN_RIGHT_INSTANCE,
        PruneEmptyRules.SORT_FETCH_ZERO_INSTANCE,
        UnionMergeRule.INSTANCE,
        UnionMergeRule.INTERSECT_INSTANCE,
        UnionMergeRule.MINUS_INSTANCE,
        ProjectToWindowRule.PROJECT,
        FilterMergeRule.INSTANCE,
        DateRangeRules.FILTER_INSTANCE,
        IntersectToDistinctRule.INSTANCE);

    public static final List<RelOptRule> ABSTRACT_RELATIONAL_RULES = ImmutableList.of(
        FilterJoinRule.FILTER_ON_JOIN,
        FilterJoinRule.JOIN,
        AbstractConverter.ExpandConversionRule.INSTANCE,
        JoinCommuteRule.INSTANCE,
        SemiJoinRule.PROJECT,
        SemiJoinRule.JOIN,
        AggregateRemoveRule.INSTANCE,
        UnionToDistinctRule.INSTANCE,
        ProjectRemoveRule.INSTANCE,
        AggregateJoinTransposeRule.INSTANCE,
        AggregateMergeRule.INSTANCE,
        AggregateProjectMergeRule.INSTANCE,
        CalcRemoveRule.INSTANCE,
        SortRemoveRule.INSTANCE);

    public static final List<RelOptRule> CONSTANT_REDUCTION_RULES = ImmutableList.of(
        ReduceExpressionsRule.PROJECT_INSTANCE,
        ReduceExpressionsRule.FILTER_INSTANCE,
        ReduceExpressionsRule.CALC_INSTANCE,
        ReduceExpressionsRule.WINDOW_INSTANCE,
        ReduceExpressionsRule.JOIN_INSTANCE,
        ValuesReduceRule.FILTER_INSTANCE,
        ValuesReduceRule.PROJECT_FILTER_INSTANCE,
        ValuesReduceRule.PROJECT_INSTANCE,
        AggregateValuesRule.INSTANCE);

    public static final List<RelOptRule> MATERIALIZATION_RULES = ImmutableList.of(
        MaterializedViewFilterScanRule.INSTANCE,
        AbstractMaterializedViewRule.INSTANCE_PROJECT_FILTER,
        AbstractMaterializedViewRule.INSTANCE_FILTER,
        AbstractMaterializedViewRule.INSTANCE_PROJECT_JOIN,
        AbstractMaterializedViewRule.INSTANCE_JOIN,
        AbstractMaterializedViewRule.INSTANCE_PROJECT_AGGREGATE,
        AbstractMaterializedViewRule.INSTANCE_AGGREGATE);

    public static final List<RelOptRule> SUBQUERY_REWRITE_RULES = ImmutableList.of(
        SubQueryRemoveRule.FILTER,
        SubQueryRemoveRule.PROJECT,
        SubQueryRemoveRule.JOIN);

    public static final List<RelOptRule> IGNITE_BASE_RULES = ImmutableList.of(
        IgniteFilterRule.INSTANCE,
        IgniteProjectRule.INSTANCE,
        IgniteJoinRule.INSTANCE
    );

    public static List<RelOptRule> logicalRules(Context ctx) {
        return ImmutableList.<RelOptRule>builder()
            .addAll(BASE_RULES)
            .addAll(ABSTRACT_RULES)
            .addAll(ABSTRACT_RELATIONAL_RULES)
            .addAll(IGNITE_BASE_RULES)
            .build();
    }
}
