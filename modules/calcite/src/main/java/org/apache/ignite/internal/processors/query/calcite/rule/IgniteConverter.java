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

package org.apache.ignite.internal.processors.query.calcite.rule;

import com.google.common.collect.ImmutableMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.util.typedef.F;

/**
 *
 */
public abstract class IgniteConverter extends ConverterRule {
    protected IgniteConverter(Class<? extends RelNode> clazz, String descriptionPrefix) {
        super(clazz, Convention.NONE, IgniteConvention.INSTANCE, descriptionPrefix);
    }

    @Override public void onMatch(RelOptRuleCall call) {
        RelNode rel = call.rel(0);
        if (rel.getTraitSet().contains(Convention.NONE)) {
            List<RelNode> rels = convert0(rel);
            if (F.isEmpty(rels))
                return;

            Map<RelNode, RelNode> equiv = ImmutableMap.of();

            if (rels.size() > 1) {
                equiv = new HashMap<>();

                for (int i = 1; i < rels.size(); i++) {
                    equiv.put(rels.get(i), rel);
                }
            }

            call.transformTo(F.first(rels), equiv);
        }
    }

    @Override public RelNode convert(RelNode rel) {
        List<RelNode> converted = convert0(rel);

        if (converted.size() > 1) {
            RelOptPlanner planner = rel.getCluster().getPlanner();

            for (int i = 1; i < converted.size(); i++)
                planner.ensureRegistered(converted.get(i), rel);
        }

        return F.first(converted);
    }

    protected abstract List<RelNode> convert0(RelNode rel);
}
