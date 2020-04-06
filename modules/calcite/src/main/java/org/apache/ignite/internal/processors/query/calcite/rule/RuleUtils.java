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
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.apache.ignite.internal.util.typedef.F;
import org.jetbrains.annotations.NotNull;

import static org.apache.calcite.plan.RelOptRule.any;
import static org.apache.calcite.plan.RelOptRule.operand;
import static org.apache.calcite.plan.RelOptRule.some;

/**
 *
 */
public class RuleUtils {
    /** */
    public static RelOptRuleOperand traitPropagationOperand(Class<? extends RelNode> clazz) {
        return operand(clazz, IgniteDistributions.any(), some(operand(RelSubset.class, any())));
    }

    /** */
    public static RelNode convert(RelNode rel, @NotNull RelTrait toTrait) {
        RelTraitSet toTraits = rel.getTraitSet().replace(toTrait);

        if (rel.getTraitSet().matches(toTraits))
            return rel;

        RelOptPlanner planner = rel.getCluster().getPlanner();

        return planner.changeTraits(rel, toTraits.simplify());
    }

    /** */
    public static RelNode convert(RelNode rel, @NotNull RelTraitSet toTraits) {
        RelTraitSet outTraits = rel.getTraitSet();
        for (int i = 0; i < toTraits.size(); i++) {
            RelTrait toTrait = toTraits.getTrait(i);

            if (toTrait != null)
                outTraits = outTraits.replace(i, toTrait);
        }

        if (rel.getTraitSet().matches(outTraits))
            return rel;

        RelOptPlanner planner = rel.getCluster().getPlanner();

        return planner.changeTraits(rel, outTraits);
    }

    /** */
    public static void transformTo(RelOptRuleCall call, RelNode newRel) {
        transformTo(call, ImmutableList.of(newRel));
    }

    /** */
    public static void transformTo(RelOptRuleCall call, List<RelNode> newRels) {
        transformTo(call, newRels, ImmutableMap.of());
    }

    /** */
    public static void transformTo(RelOptRuleCall call, List<RelNode> newRels, Map<RelNode, RelNode> additional) {
        RelNode orig = call.rel(0);

        if (F.isEmpty(newRels)) {
            if (!F.isEmpty(additional))
                // small trick to register the additional equivalence map entries only, we pass
                // the original rel as transformed one, which will be skipped by the planner.
                call.transformTo(orig, additional);

            return;
        }

        if (isRoot(orig))
            newRels = Commons.transform(newRels, RuleUtils::changeToRootTraits);

        RelNode first = F.first(newRels);
        List<RelNode> remaining = newRels.subList(1, newRels.size());
        Map<RelNode, RelNode> equivMap = equivMap(orig, remaining, additional);

        call.transformTo(first, equivMap);
    }

    /** */
    public static RelNode changeTraits(RelNode rel, RelTrait... diff) {
        RelTraitSet traits = rel.getTraitSet();

        for (RelTrait trait : diff)
            traits = traits.replace(trait);

        return changeTraits(rel, traits);
    }

    /** */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public static RelNode changeTraits(RelNode rel, RelTraitSet toTraits) {
        RelTraitSet fromTraits = rel.getTraitSet();

        if (fromTraits.satisfies(toTraits))
            return rel;

        assert fromTraits.size() >= toTraits.size();

        RelOptPlanner planner = rel.getCluster().getPlanner();

        RelNode converted = rel;

        for (int i = 0; (converted != null) && (i < toTraits.size()); i++) {
            RelTrait fromTrait = converted.getTraitSet().getTrait(i);
            RelTrait toTrait = toTraits.getTrait(i);

            RelTraitDef traitDef = fromTrait.getTraitDef();

            if (toTrait == null)
                continue;

            assert traitDef == toTrait.getTraitDef();

            if (fromTrait.equals(toTrait))
                continue;

            rel = traitDef.convert(planner, converted, toTrait, true);

            assert rel == null || rel.getTraitSet().getTrait(traitDef).satisfies(toTrait);

            if (rel != null)
                planner.register(rel, converted);

            converted = rel;
        }

        assert converted == null || converted.getTraitSet().satisfies(toTraits);

        return converted;
    }

    /** */
    private static boolean isRoot(RelNode rel) {
        RelOptPlanner planner = rel.getCluster().getPlanner();
        RelNode root = planner.getRoot();

        if (root instanceof RelSubset)
            return ((VolcanoPlanner) planner).getSubset(rel, root.getTraitSet()) == root;

        if (root instanceof HepRelVertex)
            return root == rel || ((HepRelVertex) root).getCurrentRel() == rel;

        return root == rel;
    }

    /** */
    private static RelNode changeToRootTraits(RelNode rel) {
        RelTraitSet rootTraits = rel.getCluster().getPlanner().getRoot().getTraitSet();

        return changeTraits(rel, rootTraits);
    }

    /** */
    private static @NotNull Map<RelNode, RelNode> equivMap(RelNode orig, List<RelNode> equivList, Map<RelNode, RelNode> additional) {
        assert orig != null;
        assert equivList != null;
        assert additional != null;

        if(F.isEmpty(equivList))
            return additional;

        ImmutableMap.Builder<RelNode, RelNode> b = ImmutableMap.builder();

        for (RelNode equiv : equivList)
            b.put(equiv, orig);

        b.putAll(additional);

        return b.build();
    }
}
