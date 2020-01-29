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

import org.apache.calcite.rel.rules.SubQueryRemoveRule;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.ignite.internal.processors.query.calcite.rule.FilterConverter;
import org.apache.ignite.internal.processors.query.calcite.rule.JoinConverter;
import org.apache.ignite.internal.processors.query.calcite.rule.ProjectConverter;

/**
 * Represents a planner phase with its description and a used rule set.
 */
public enum PlannerPhase {
    /** */
    SUBQUERY_REWRITE("Sub-queries rewrites") {
        /** {@inheritDoc} */
        @Override public RuleSet getRules(PlanningContext ctx) {
            return RuleSets.ofList(
                SubQueryRemoveRule.FILTER,
                SubQueryRemoveRule.PROJECT,
                SubQueryRemoveRule.JOIN);
        }
    },

    /** */
    OPTIMIZATION("Main optimization phase") {
        /** {@inheritDoc} */
        @Override public RuleSet getRules(PlanningContext ctx) {
            return RuleSets.ofList(
                JoinConverter.INSTANCE,
                ProjectConverter.INSTANCE,
                FilterConverter.INSTANCE);
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
}
