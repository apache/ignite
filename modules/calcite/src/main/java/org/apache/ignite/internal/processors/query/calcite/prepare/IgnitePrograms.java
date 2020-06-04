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

import org.apache.calcite.plan.RelOptLattice;
import org.apache.calcite.plan.RelOptMaterialization;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.apache.calcite.tools.RuleSet;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class IgnitePrograms {
    /**
     * Returns heuristic planer based program with given rules.
     *
     * @param rules Rules.
     * @return New program.
     */
    public static Program hep(RuleSet rules) {
        return (planner, rel, traits, materializations, lattices) -> {
                final HepProgramBuilder builder = new HepProgramBuilder();

                for (RelOptRule rule : rules)
                    builder.addRuleInstance(rule);

                final HepPlanner hepPlanner = new HepPlanner(builder.build(), Commons.context(rel), true,
                    null, Commons.context(rel).config().getCostFactory());

                for (RelOptMaterialization materialization : materializations)
                    hepPlanner.addMaterialization(materialization);

                for (RelOptLattice lattice : lattices)
                    hepPlanner.addLattice(lattice);

                hepPlanner.setRoot(rel);

                return hepPlanner.findBestExp();
        };
    }

    /**
     * Returns cost based planer based program with given rules.
     *
     * @param rules Rules.
     * @return New program.
     */
    public static Program cbo(RuleSet rules) {
        return Programs.of(rules);
    }
}
