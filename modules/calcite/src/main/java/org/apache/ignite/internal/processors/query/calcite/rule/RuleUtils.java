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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.U;
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
    public static void transformTo(RelOptRuleCall call, RelNode newRel) {
        transformTo(call, ImmutableList.of(newRel));
    }

    /** */
    public static void transformTo(RelOptRuleCall call, List<RelNode> newRels) {
        RelNode orig = call.rel(0);

        if (F.isEmpty(newRels))
            return;

        newRels = changeRootTraits(orig, newRels);

        RelNode first = F.first(newRels);
        Map<RelNode, RelNode> equivMap = equivMap(orig, newRels.subList(1, newRels.size()));
        call.transformTo(first, equivMap);
    }

    /** */
    public static RelNode changeTraits(RelNode rel, RelTrait trait) {
        RelTraitSet traits = rel.getTraitSet().replace(trait);

        return changeTraits(rel, traits);
    }

    /** */
    public static RelNode changeTraits(RelNode rel, RelTrait trait, RelTrait... diff) {
        RelTraitSet traits = rel.getTraitSet().replace(trait);

        for (RelTrait trait0 : diff)
            traits = traits.replace(trait0);

        return changeTraits(rel, traits);
    }

    /** */
    @SuppressWarnings({"unchecked"})
    public static RelNode changeTraits(RelNode rel, RelTraitSet toTraits) {
        RelOptPlanner planner = rel.getCluster().getPlanner();
        rel = planner.ensureRegistered(rel, null);
        RelTraitSet fromTraits = rel.getTraitSet();

        RelNode res = rel;

        if (!fromTraits.satisfies(toTraits)) {
            for (int i = 0; (rel != null) && (i < toTraits.size()); i++) {
                RelTrait fromTrait = rel.getTraitSet().getTrait(i);
                RelTrait toTrait = toTraits.getTrait(i);

                if (fromTrait.satisfies(toTrait))
                    continue;

                RelNode old = rel;
                rel = fromTrait.getTraitDef().convert(planner, old, toTrait, true);

                if (rel != null)
                    rel = planner.ensureRegistered(rel, old);

                assert rel == null || rel.getTraitSet().getTrait(i).satisfies(toTrait);
            }

            assert rel == null || rel.getTraitSet().satisfies(toTraits);

            res = rel == null ? planner.changeTraits(res, toTraits) : rel;
        }

        return res;
    }

    /** */
    public static boolean isRoot(RelNode rel) {
        RelOptPlanner planner = rel.getCluster().getPlanner();
        RelNode root = planner.getRoot();

        if (root instanceof RelSubset)
            return ((VolcanoPlanner) planner).getSubset(rel, root.getTraitSet()) == root;

        if (root instanceof HepRelVertex)
            return root == rel || ((HepRelVertex) root).getCurrentRel() == rel;

        return root == rel;
    }

    /** */
    public static List<RelNode> changeRootTraits(RelNode orig, List<RelNode> rels) {
        if (!isRoot(orig))
            return rels;

        RelTraitSet rootTraits = orig.getCluster().getPlanner().getRoot().getTraitSet();
        Set<RelNode> newNodes = U.newHashSet(rels.size());

        for (RelNode rel : rels) {
            RelNode newNode = rel.getConvention() == Convention.NONE
                ? rel // logical rels will be converted after to-ignite transformation
                : changeTraits(rel, rootTraits);

            newNodes.add(newNode);
        }

        return new ArrayList<>(newNodes);
    }

    /** */
    private static @NotNull Map<RelNode, RelNode> equivMap(RelNode orig, List<RelNode> equivList) {
        assert orig != null;
        assert equivList != null;

        if (F.isEmpty(equivList))
            return ImmutableMap.of();

        Map<RelNode, RelNode> res = U.newHashMap(equivList.size());

        for (RelNode equiv : equivList)
            res.put(equiv, orig);

        return res;
    }
}
