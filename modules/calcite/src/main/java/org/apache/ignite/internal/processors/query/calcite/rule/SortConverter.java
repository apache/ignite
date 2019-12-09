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
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDerivedDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteSort;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 * TODO: Add class description.
 */
public class SortConverter extends IgniteConverter {

    public static final RelOptRule INSTANCE = new SortConverter();

    private SortConverter() {
        super(LogicalSort.class, "SortConverter");
    }

    @Override protected List<RelNode> convert0(RelNode rel) {
        LogicalSort sort = (LogicalSort) rel;

        RelNode input = convert(sort.getInput(), IgniteConvention.INSTANCE);

        RelOptCluster cluster = rel.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();

        List<IgniteDistribution> distrs = IgniteMdDerivedDistribution.deriveDistributions(input, IgniteConvention.INSTANCE, mq);

        return Commons.transform(distrs, d -> create(sort, input, d));
    }

    private static IgniteSort create(LogicalSort sort, RelNode input, IgniteDistribution distr) {
        RelTraitSet traits = sort.getTraitSet()
            .replace(distr)
            .replace(IgniteConvention.INSTANCE);

        return new IgniteSort(sort.getCluster(), traits, convert(input, distr), sort.getCollation());
    }
}
