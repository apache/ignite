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

package org.apache.ignite.internal.processors.query.calcite.trait;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelNode;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteExchange;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteTrimExchange;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;
import org.jetbrains.annotations.Nullable;

import static org.apache.calcite.rel.RelDistribution.Type.BROADCAST_DISTRIBUTED;
import static org.apache.calcite.rel.RelDistribution.Type.HASH_DISTRIBUTED;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.any;
import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions.single;

/**
 *
 */
public class TraitUtils {
    /** */
    public static RelNode enforce(RelNode rel, RelTraitSet toTraits) {
        RelOptPlanner planner = rel.getCluster().getPlanner();
        RelTraitSet fromTraits = rel.getTraitSet();
        if (!fromTraits.satisfies(toTraits)) {
            for (int i = 0; (rel != null) && (i < toTraits.size()); i++) {
                RelTrait fromTrait = rel.getTraitSet().getTrait(i);
                RelTrait toTrait = toTraits.getTrait(i);

                if (fromTrait.satisfies(toTrait))
                    continue;

                RelNode old = rel;
                rel = convertTrait(planner, fromTrait, toTrait, old);

                if (rel != null)
                    rel = planner.register(rel, old);

                assert rel == null || rel.getTraitSet().getTrait(i).satisfies(toTrait);
            }

            assert rel == null || rel.getTraitSet().satisfies(toTraits);
        }

        return rel;
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static RelNode convertTrait(RelOptPlanner planner, RelTrait fromTrait, RelTrait toTrait, RelNode rel) {
        assert fromTrait.getTraitDef() == toTrait.getTraitDef();

        RelTraitDef converter = fromTrait.getTraitDef();

        if (converter == RelCollationTraitDef.INSTANCE)
            return convertCollation(planner, (RelCollation)toTrait, rel);
        else if (converter == DistributionTraitDef.INSTANCE)
            return convertDistribution(planner, (IgniteDistribution)toTrait, rel);
        else
            return converter.convert(planner, rel, toTrait, true);
    }

    /** */
    @Nullable public static RelNode convertCollation(RelOptPlanner planner,
        RelCollation toTrait, RelNode rel) {
        if (toTrait.getFieldCollations().isEmpty())
            return null;

        RelNode result = new IgniteSort(rel.getCluster(), rel.getTraitSet().replace(toTrait),
            rel, toTrait, null, null);

        return planner.register(result, rel);
    }

    /** */
    @Nullable public static RelNode convertDistribution(RelOptPlanner planner,
        IgniteDistribution toTrait, RelNode rel) {

        if (toTrait.getType() == RelDistribution.Type.ANY)
            return null;

        RelNode result;
        IgniteDistribution fromTrait = Commons.distribution(rel.getTraitSet());
        if (fromTrait.getType() == BROADCAST_DISTRIBUTED && toTrait.getType() == HASH_DISTRIBUTED)
            result = new IgniteTrimExchange(rel.getCluster(), rel.getTraitSet().replace(toTrait), rel, toTrait);
        else {
            result = new IgniteExchange(rel.getCluster(), rel.getTraitSet().replace(toTrait),
                RelOptRule.convert(rel, any()), toTrait);
        }

        return planner.register(result, rel);
    }

    /** */
    public static RelTraitSet fixTraits(RelTraitSet traits) {
        if (Commons.distribution(traits) == any())
            traits = traits.replace(single());

        return traits.replace(IgniteConvention.INSTANCE);
    }
}
