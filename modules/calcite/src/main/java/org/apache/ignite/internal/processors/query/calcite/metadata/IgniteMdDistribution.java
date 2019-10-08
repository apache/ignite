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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSlot;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributionTraitDef;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistributions;
import org.apache.ignite.internal.processors.query.calcite.util.IgniteMethod;

import static org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution.DistributionType.HASH;

/**
 *
 */
public class IgniteMdDistribution implements MetadataHandler<IgniteMetadata.Distribution> {
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(IgniteMethod.DISTRIBUTION.method(), new IgniteMdDistribution());

    @Override public MetadataDef<IgniteMetadata.Distribution> getDef() {
        return IgniteMetadata.Distribution.DEF;
    }

    public IgniteDistribution distribution(RelNode rel, RelMetadataQuery mq) {
        return IgniteDistributionTraitDef.INSTANCE.getDefault();
    }

    public IgniteDistribution distribution(Filter filter, RelMetadataQuery mq) {
        return filter(mq, filter.getInput(), filter.getCondition());
    }

    public IgniteDistribution distribution(Project project, RelMetadataQuery mq) {
        return project(mq, project.getInput(), project.getProjects());
    }

    public IgniteDistribution distribution(Join join, RelMetadataQuery mq) {
        return join(mq, join.getLeft(), join.getRight(), join.getCondition());
    }

    public IgniteDistribution distribution(RelSubset rel, RelMetadataQuery mq) {
        return rel.getTraitSet().getTrait(IgniteDistributionTraitDef.INSTANCE);
    }

    public static IgniteDistribution project(RelMetadataQuery mq, RelNode input, List<RexNode> projects) {
        IgniteDistribution trait = distribution_(input, mq);

        if (trait.type() == HASH) {
            ImmutableIntList keys = trait.keys();

            if (keys.size() > projects.size())
                return IgniteDistributions.random(trait.sources());

            Map<Integer, Integer> m = new HashMap<>(projects.size());

            for (Ord<RexNode> node : Ord.zip(projects)) {
                if (node.e instanceof RexInputRef)
                    m.put( ((RexSlot) node.e).getIndex(), node.i);
                else if (node.e.isA(SqlKind.CAST)) {
                    RexNode operand = ((RexCall) node.e).getOperands().get(0);

                    if (operand instanceof RexInputRef)
                        m.put(((RexSlot) operand).getIndex(), node.i);
                }
            }

            List<Integer> newKeys = new ArrayList<>(keys.size());

            for (Integer key : keys) {
                Integer mapped = m.get(key);

                if (mapped == null)
                    return IgniteDistributions.random(trait.sources());

                newKeys.add(mapped);
            }

            return IgniteDistributions.hash(newKeys, trait.sources());
        }

        return trait;
    }

    public static IgniteDistribution filter(RelMetadataQuery mq, RelNode input, RexNode condition) {
        return distribution_(input, mq);
    }

    public static IgniteDistribution join(RelMetadataQuery mq, RelNode left, RelNode right, RexNode condition) {
        return distribution_(left, mq);
    }

    public static IgniteDistribution distribution_(RelNode rel, RelMetadataQuery mq) {
        return rel.metadata(IgniteMetadata.Distribution.class, mq).distribution();
    }
}
