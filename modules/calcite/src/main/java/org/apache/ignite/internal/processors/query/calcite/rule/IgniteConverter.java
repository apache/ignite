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

import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.volcano.VolcanoUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;

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
            List<RelNode> convertedRels = convert0(rel);

            for (RelNode convertedRel : convertedRels) {
                call.transformTo(convertedRel);
            }
        }

        VolcanoUtils.ensureRootConverters(rel.getCluster().getPlanner());
    }

    protected abstract List<RelNode> convert0(RelNode rel);

    @Override public RelNode convert(RelNode rel) {
        throw new AssertionError("Should not be called");
    }


}
