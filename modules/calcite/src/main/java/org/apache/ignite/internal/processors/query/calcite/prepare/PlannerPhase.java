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

import org.apache.calcite.rel.rules.FilterMergeRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.rel.rules.UnionMergeRule;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.ignite.internal.processors.query.calcite.rule.AggregateConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.AggregateTraitsPropagationRule;
import org.apache.ignite.internal.processors.query.calcite.rule.FilterConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.FilterTraitsPropagationRule;
import org.apache.ignite.internal.processors.query.calcite.rule.JoinConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.JoinTraitsPropagationRule;
import org.apache.ignite.internal.processors.query.calcite.rule.ProjectConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.ProjectTraitsPropagationRule;
import org.apache.ignite.internal.processors.query.calcite.rule.TableModifyConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.UnionConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rule.UnionTraitsTraitsPropagationRule;
import org.apache.ignite.internal.processors.query.calcite.rule.ValuesConverterRule;

import static org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePrograms.cbo;
import static org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePrograms.decorrelate;
import static org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePrograms.hep;
import static org.apache.ignite.internal.processors.query.calcite.prepare.IgnitePrograms.sequence;

/**
 * Represents a planner phase with its description and a used rule set.
 */
public enum PlannerPhase {
    /** */
    HEURISTIC_OPTIMIZATION("Heuristic optimization phase") {
        /** {@inheritDoc} */
        @Override public RuleSet getRules(PlanningContext ctx) {
            return RuleSets.ofList(
                ValuesConverterRule.INSTANCE,
                SubQueryRemoveRule.FILTER,
                SubQueryRemoveRule.PROJECT,
                SubQueryRemoveRule.JOIN);
        }

        /** {@inheritDoc} */
        @Override public Program getProgram(PlanningContext ctx) {
            return sequence(hep(getRules(ctx)), decorrelate());
        }
    },

    /** */
    OPTIMIZATION("Main optimization phase") {
        /** {@inheritDoc} */
        @Override public RuleSet getRules(PlanningContext ctx) {
            return RuleSets.ofList(
                AggregateConverterRule.INSTANCE,
                AggregateTraitsPropagationRule.INSTANCE,
                JoinConverterRule.INSTANCE,
                JoinTraitsPropagationRule.INSTANCE,
                ProjectConverterRule.INSTANCE,
                ProjectTraitsPropagationRule.INSTANCE,
                FilterConverterRule.INSTANCE,
                FilterMergeRule.INSTANCE,
                FilterProjectTransposeRule.INSTANCE,
                FilterTraitsPropagationRule.INSTANCE,
                ProjectMergeRule.INSTANCE,
                TableModifyConverterRule.INSTANCE,
                UnionMergeRule.INSTANCE,
                UnionConverterRule.INSTANCE,
                UnionTraitsTraitsPropagationRule.INSTANCE);
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
     * @param ctx Planner context.
     * @return Rule set.
     */
    public abstract RuleSet getRules(PlanningContext ctx);

    /**
     * Returns a program, calculated on the basis of query, planner context planner phase and rules set.
     * @param ctx Planner context.
     * @return Rule set.
     */
    public abstract Program getProgram(PlanningContext ctx);
}
