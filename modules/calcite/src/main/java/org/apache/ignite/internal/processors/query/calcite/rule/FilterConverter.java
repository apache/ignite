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
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.ignite.internal.processors.query.calcite.metadata.IgniteMdDerivedDistribution;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteConvention;
import org.apache.ignite.internal.processors.query.calcite.rel.IgniteFilter;
import org.apache.ignite.internal.processors.query.calcite.trait.IgniteDistribution;
import org.apache.ignite.internal.processors.query.calcite.util.Commons;

/**
 *
 */
public class FilterConverter extends IgniteConverter {
    public static final ConverterRule INSTANCE = new FilterConverter();

    public FilterConverter() {
        super(LogicalFilter.class, "FilterConverter");
    }

    @Override protected List<RelNode> convert0(RelNode rel) {
        LogicalFilter filter = (LogicalFilter) rel;

        RelNode input = convert(filter.getInput(), IgniteConvention.INSTANCE);

        RelOptCluster cluster = rel.getCluster();
        RelMetadataQuery mq = cluster.getMetadataQuery();

        List<IgniteDistribution> distrs = IgniteMdDerivedDistribution.deriveDistributions(input, IgniteConvention.INSTANCE, mq);

        return Commons.transform(distrs, d -> create(filter, input, d));
    }

    private static IgniteFilter create(LogicalFilter filter, RelNode input, IgniteDistribution distr) {
        RelTraitSet traits = filter.getTraitSet()
            .replace(distr)
            .replace(IgniteConvention.INSTANCE);

        return new IgniteFilter(filter.getCluster(), traits, convert(input, distr), filter.getCondition());
    }
}
